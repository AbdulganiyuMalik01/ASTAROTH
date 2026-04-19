"""
runner_detector_v3_bridge.py

Connects runner_detector.py to token_tracker_webhook_v3.py's BuyEvent data.

Instead of dummy buy data, this uses REAL webhook-captured buy events to
compute runner signals. Drops directly into existing v3 architecture.
"""

import asyncio
import logging
import time
from typing import Dict, Optional, List
from dataclasses import dataclass
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class BuySnapshot:
    """Snapshot of buy activity in a time window."""
    timestamp: float
    total_sol_in_window: float
    buy_count: int
    unique_wallets: int
    cluster_peak: int  # Max buys in any 10s sub-window


class RunnerMetricsFromV3:
    """
    Translates v3's TokenInfo.buy_events into runner detection metrics.

    TokenInfo structure (from token_tracker_webhook_v3.py):
        TokenInfo.mint
        TokenInfo.buy_events: List[BuyEvent]
            BuyEvent.timestamp
            BuyEvent.sol_amount
            BuyEvent.buyer
            BuyEvent.txn_sig
    """

    def __init__(self, token_address: str, symbol: str = "???"):
        self.token_address = token_address
        self.symbol = symbol

        # Metrics computed from BuyEvent history
        self.recent_buys_1h: deque = deque()  # (timestamp, sol, wallet) tuples
        self.velocity_samples: deque = deque(maxlen=12)  # 12 snapshots = 60 min @ 5s interval

        self.last_velocity_acceleration = 0.0
        self.last_cluster_peak = 0
        self.avg_buy_size_sol = 0.0
        self.new_wallet_ratio = 0.0
        self.total_1h_sol = 0.0
        self.unique_wallet_count = 0

        self.is_runner = False
        self.runner_score = 0.0

    def update_from_buy_events(self, buy_events: List) -> None:
        """
        Called with token's full buy_events list from v3.
        Computes all runner metrics from scratch.
        """
        if not buy_events:
            self._reset()
            return

        now = time.time()
        cutoff_1h = now - 3600

        # Filter to last 1 hour only
        recent = [(e.timestamp, e.sol_amount, e.buyer) for e in buy_events if e.timestamp >= cutoff_1h]

        if not recent:
            self._reset()
            return

        self.recent_buys_1h = deque(recent, maxlen=1000)

        # Metric 1: Total 1h SOL and unique wallets
        self.total_1h_sol = sum(b[1] for b in recent)
        self.unique_wallet_count = len(set(b[2] for b in recent))
        self.avg_buy_size_sol = self.total_1h_sol / len(recent) if recent else 0.0

        # Metric 2: New wallet ratio (wallets not seen in full history)
        all_wallets_ever = set(e.buyer for e in buy_events)
        recent_wallets = set(b[2] for b in recent)
        new_wallets = len(recent_wallets - (all_wallets_ever - recent_wallets))
        self.new_wallet_ratio = new_wallets / len(recent) if recent else 0.0

        # Metric 3: Cluster density (max buys in any 10s window)
        self.last_cluster_peak = self._compute_cluster_peak(recent)

        # Metric 4: Velocity acceleration
        self._update_velocity_history(recent, now)
        self.last_velocity_acceleration = self._compute_acceleration()

    def _reset(self) -> None:
        self.recent_buys_1h = deque()
        self.velocity_samples = deque()
        self.total_1h_sol = 0.0
        self.unique_wallet_count = 0
        self.avg_buy_size_sol = 0.0
        self.new_wallet_ratio = 0.0
        self.last_cluster_peak = 0
        self.last_velocity_acceleration = 0.0

    def _compute_cluster_peak(self, recent: List[tuple]) -> int:
        """Find max buys in any 10-second window."""
        if len(recent) < 3:
            return len(recent)

        sorted_by_time = sorted(recent, key=lambda x: x[0])
        max_in_window = 0

        for i, (ts, _, _) in enumerate(sorted_by_time):
            window_end = ts + 10.0
            count = sum(1 for t, _, _ in sorted_by_time if ts <= t <= window_end)
            max_in_window = max(max_in_window, count)

        return max_in_window

    def _update_velocity_history(self, recent: List[tuple], now: float) -> None:
        """
        Add current window velocity to rolling history.
        Velocity = SOL gained in last 5 minutes.
        """
        cutoff_5min = now - 300
        sol_5min = sum(b[1] for b in recent if b[0] >= cutoff_5min)
        # Scale to hourly equivalent (5 min * 12 = 60 min)
        hourly_velocity = sol_5min * 12
        self.velocity_samples.append(hourly_velocity)

    def _compute_acceleration(self) -> float:
        """% change in velocity over last 2 samples."""
        if len(self.velocity_samples) < 2:
            return 0.0

        prev = self.velocity_samples[-2]
        curr = self.velocity_samples[-1]

        if prev == 0:
            return 1.0 if curr > 0 else 0.0

        return (curr - prev) / prev


def compute_runner_score(metrics: RunnerMetricsFromV3) -> float:
    """
    Score a token as a runner (1-10) based on buy algorithm metrics.

    Weights:
    - Velocity acceleration (40%)
    - Cluster density (30%)
    - New wallet ratio (20%)
    - Buy size momentum (10%)
    + Liquidity bonus (1)
    """
    score = 0.0

    # Velocity acceleration: how fast is volume ramping?
    accel_clamped = min(max(metrics.last_velocity_acceleration, 0.0) * 100, 10.0) / 10.0
    score += accel_clamped * 4.0  # Max 4 points

    # Cluster density: are buys coming in coordinated bursts?
    cluster_normalized = min(metrics.last_cluster_peak / 8.0, 1.0)
    score += cluster_normalized * 3.0  # Max 3 points

    # New wallet ratio: is fresh capital entering?
    new_wallet_normalized = min(metrics.new_wallet_ratio / 0.5, 1.0)
    score += new_wallet_normalized * 2.0  # Max 2 points

    # Buy size momentum: are buys getting bigger?
    buy_size_normalized = min(metrics.avg_buy_size_sol / 1.0, 1.0)
    score += buy_size_normalized * 1.0  # Max 1 point

    return min(score, 10.0)


async def check_tokens_for_runners(
    tokens: Dict,
    tokens_lock: asyncio.Lock,
    send_telegram_message,
    liquidity_cache: Dict = None,  # Optional: token_address -> dexscreener metrics
) -> List[Dict]:
    """
    Scan all tokens in v3's tracking dict for runner signals.

    Returns list of tokens that newly crossed runner threshold.

    Usage in v3 loop:
        runners = await check_tokens_for_runners(tokens, tokens_lock, ...)
        for runner in runners:
            # Send alert, inject into other systems, etc.
    """
    MIN_RUNNER_SCORE = 7.0
    runners = []

    async with tokens_lock:
        token_list = list(tokens.items())

    for address, token_info in token_list:
        try:
            # Create metrics object
            metrics = RunnerMetricsFromV3(address, token_info.symbol or "???")

            # Feed v3's buy events into metrics
            metrics.update_from_buy_events(token_info.buy_events or [])

            # Skip if insufficient data
            if len(metrics.recent_buys_1h) < 3:
                continue

            # Compute score
            score = compute_runner_score(metrics)

            # State transition: non-runner -> runner
            if score >= MIN_RUNNER_SCORE and not getattr(token_info, '_is_runner', False):
                token_info._is_runner = True
                token_info._runner_score = score

                # Fetch fresh on-chain metrics for alert
                liq_usd = 0.0
                if liquidity_cache and address in liquidity_cache:
                    liq_usd = liquidity_cache[address].get("liquidity_usd", 0.0)

                runners.append({
                    "address": address,
                    "symbol": token_info.symbol,
                    "name": token_info.name or "Unknown",
                    "score": score,
                    "velocity_acceleration": metrics.last_velocity_acceleration,
                    "cluster_peak": metrics.last_cluster_peak,
                    "new_wallet_ratio": metrics.new_wallet_ratio,
                    "avg_buy_size": metrics.avg_buy_size_sol,
                    "total_1h_sol": metrics.total_1h_sol,
                    "unique_wallets": metrics.unique_wallet_count,
                    "liquidity_usd": liq_usd,
                })

                logger.info(f"🏃 Runner: ${token_info.symbol} score={score:.1f}")

            # State transition: runner -> non-runner (dropped below threshold)
            elif score < MIN_RUNNER_SCORE * 0.8 and getattr(token_info, '_is_runner', False):
                token_info._is_runner = False

        except Exception as e:
            logger.debug(f"Runner check error for {address}: {e}")

    return runners


def format_runner_alert_v3(runner: Dict) -> str:
    """Format runner detection for Telegram."""
    return (
        f"🏃 <b>RUNNER DETECTED</b>\n\n"
        f"<b>${runner['symbol']}</b> — {runner['name']}\n"
        f"<code>{runner['address']}</code>\n\n"
        f"📊 <b>Buy Algorithm Score:</b> {runner['score']:.1f}/10\n\n"
        f"⚡ <b>Velocity Acceleration:</b> +{runner['velocity_acceleration']*100:.1f}% SOL/hour\n"
        f"📈 <b>Cluster Density:</b> {runner['cluster_peak']} buys in 10s peak\n"
        f"👛 <b>New Wallets:</b> {runner['new_wallet_ratio']*100:.0f}%\n"
        f"💰 <b>Avg Buy Size:</b> {runner['avg_buy_size']:.2f} SOL\n"
        f"💵 <b>1-Hour Volume:</b> {runner['total_1h_sol']*180:.0f} USD\n"
        f"🎯 <b>Unique Wallets:</b> {runner['unique_wallets']}\n\n"
        f"📍 Liquidity: ${runner['liquidity_usd']:,.0f}\n\n"
        f"🔗 <a href='https://dexscreener.com/solana/{runner['address']}'>DexScreener</a> | "
        f"<a href='https://pump.fun/{runner['address']}'>Pump.fun</a>"
    )
