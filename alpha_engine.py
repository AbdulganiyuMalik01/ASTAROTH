"""
Alpha Engine v5 - Multi-Source Token Detection
===============================================
Replaces the Pump.fun-only webhook approach with a hybrid system:

1. DexScreener Poller  - free API, catches migrated/Raydium/PumpSwap tokens proactively
2. Helius Webhook      - narrowed to PumpSwap + Raydium + Moonshot (NOT raw Pump.fun bonding curve)
3. Scoring v5         - multi-signal composite score with source confidence weighting

Improvements applied (over the original):
  #1  — Hard gates replaced with soft penalty factors. Tokens outside the MC/vol
         bands no longer get zero-scored — they receive a configurable penalty
         multiplier instead, so near-miss tokens still surface at lower priority.
  #2  — DexScreener fallback: if DexScreener fails or returns no pairs,
         fetch_dex_pair_data() automatically retries against the Birdeye API
         and then Jupiter price API. This prevents total blindness during
         DexScreener outages.
  #3  — Buy-volume directionality: the new enrich_token_from_helius_transfers()
         helper uses actual Helius tokenTransfer direction data when available,
         replacing the txn-ratio approximation.

Supported Launchpads (with Helius program IDs):
  PumpSwap     pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA
  Raydium AMM  675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8
  Raydium CLMM CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK
  Meteora DLMM LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo
  Meteora Pools Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB
  Moonshot     MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG
  Pump.fun BC  6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P (DexScreener poll only)

Score Components (each 0-10, then multiplied + bonus):
  velocity_score    = estimated SOL/hour trade volume
  liquidity_score   = pool depth health (vol/liq ratio)
  wallet_score      = unique buyer diversity
  momentum_score    = buy/sell ratio + price acceleration
  age_score         = sweet spot 30m-6h window
  * source_mult     = 1.0-1.2x (DexScreener confirmed = 1.2x)
  + launchpad_bonus = 0-2.5 (PumpSwap/Raydium = 2.5, raw Pump.fun = 0)

Soft-gate penalties (#1):
  MC out-of-band     → × MC_PENALTY_FACTOR   (default 0.60)
  Vol out-of-band    → × VOL_PENALTY_FACTOR  (default 0.55)
  Liq below floor    → × LIQ_PENALTY_FACTOR  (default 0.50)
  Buy ratio weak     → × BUY_RATIO_PENALTY   (default 0.70)
  No acceleration    → × ACCEL_PENALTY       (default 0.65)
  Low buy tx count   → × TX_COUNT_PENALTY    (default 0.75)

Alert fires when final_score >= ALPHA_SCORE_THRESHOLD (default 72)
"""

import asyncio
import logging
import time
import os
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Tuneable constants (all env-overridable)
# ─────────────────────────────────────────────────────────────────────────────

# ── Alert thresholds ──────────────────────────────────────────────────────────
ALPHA_SCORE_WATCH         = float(os.getenv("ALPHA_SCORE_WATCH",         "45.0"))
ALPHA_SCORE_THRESHOLD     = float(os.getenv("ALPHA_SCORE_THRESHOLD",     "55.0"))
ALPHA_CONFIRMATIONS       = int(os.getenv("ALPHA_CONFIRMATIONS",         "1"))
ALPHA_CONFIRM_GAP_S       = int(os.getenv("ALPHA_CONFIRM_GAP_S",         "120"))
ALPHA_COOLDOWN_SECONDS    = int(os.getenv("ALPHA_COOLDOWN_SECONDS",      "900"))
ALPHA_GLOBAL_CAP_PER_MIN  = int(os.getenv("ALPHA_GLOBAL_CAP_PER_MIN",   "20"))
ALPHA_POLL_INTERVAL       = int(os.getenv("ALPHA_POLL_INTERVAL",         "30"))
ALPHA_MAX_TOKEN_AGE_H     = float(os.getenv("ALPHA_MAX_TOKEN_AGE_H",     "48.0"))
ALPHA_MIN_TOKEN_AGE_S     = int(os.getenv("ALPHA_MIN_TOKEN_AGE_S",       "60"))
SOL_PRICE_USD             = float(os.getenv("SOL_PRICE_USD",             "175.0"))

# ── Hard gate constants (now used as band centres; violations = penalties) ────
SMART_BUY_VOL_1H_MIN_USD  = float(os.getenv("SMART_BUY_VOL_1H_MIN_USD",  "3000"))
SMART_BUY_VOL_1H_MAX_USD  = float(os.getenv("SMART_BUY_VOL_1H_MAX_USD",  "500000"))
SMART_MC_MIN_USD          = float(os.getenv("SMART_MC_MIN_USD",           "10000"))
SMART_MC_MAX_USD          = float(os.getenv("SMART_MC_MAX_USD",           "2000000"))
SMART_MIN_BUY_TX_1H       = int(os.getenv("SMART_MIN_BUY_TX_1H",         "10"))
SMART_MIN_BUY_RATIO       = float(os.getenv("SMART_MIN_BUY_RATIO",        "0.50"))
SMART_MIN_LIQUIDITY_USD   = float(os.getenv("SMART_MIN_LIQUIDITY_USD",    "5000"))
SMART_MIN_LIQ_MC_RATIO    = float(os.getenv("SMART_MIN_LIQ_MC_RATIO",    "0.03"))
SMART_MIN_ACCEL           = float(os.getenv("SMART_MIN_ACCEL",            "0.05"))
ALPHA_MIN_LIQUIDITY_USD   = SMART_MIN_LIQUIDITY_USD  # alias

# ── Soft-gate penalty multipliers (#1) ───────────────────────────────────────
# Tokens that violate a gate receive a score multiplier instead of being zero'd.
# Set to 0.0 to restore hard-gate (reject) behaviour for any individual gate.
MC_PENALTY_FACTOR         = float(os.getenv("MC_PENALTY_FACTOR",   "0.60"))
VOL_PENALTY_FACTOR        = float(os.getenv("VOL_PENALTY_FACTOR",  "0.55"))
LIQ_PENALTY_FACTOR        = float(os.getenv("LIQ_PENALTY_FACTOR",  "0.50"))
BUY_RATIO_PENALTY         = float(os.getenv("BUY_RATIO_PENALTY",   "0.70"))
ACCEL_PENALTY             = float(os.getenv("ACCEL_PENALTY",        "0.65"))
TX_COUNT_PENALTY          = float(os.getenv("TX_COUNT_PENALTY",     "0.75"))

# ── Fallback API config (#2) ──────────────────────────────────────────────────
BIRDEYE_API_KEY           = os.getenv("BIRDEYE_API_KEY", "")
BIRDEYE_PRICE_URL         = "https://public-api.birdeye.so/defi/price?address={}"
JUPITER_PRICE_URL         = "https://price.jup.ag/v4/price?ids={}"
FALLBACK_ENABLED          = os.getenv("ALPHA_FALLBACK_ENABLED", "1") == "1"

# ─────────────────────────────────────────────────────────────────────────────
# Launchpad registry
# ─────────────────────────────────────────────────────────────────────────────

LAUNCHPAD_PROGRAMS: Dict[str, str] = {
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "PumpSwap",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "Meteora Pools",
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG": "Moonshot",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "Pump.fun",
}

HELIUS_WATCH_PROGRAMS: set = {
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG",
}

LAUNCHPAD_BONUSES: Dict[str, float] = {
    "PumpSwap":      2.5,
    "Raydium AMM":   2.5,
    "Raydium CLMM":  2.5,
    "Meteora DLMM":  2.0,
    "Meteora Pools": 2.0,
    "Moonshot":      1.5,
    "Pump.fun":      0.0,
    "Unknown":       0.5,
}

DEX_TOKEN_URL           = "https://api.dexscreener.com/latest/dex/tokens/{}"
DEX_LATEST_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_LATEST_BOOSTS_URL   = "https://api.dexscreener.com/token-boosts/latest/v1"

IGNORED_MINTS: set = {
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",
}


# ─────────────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BuyEvent:
    wallet: str
    amount_sol: float
    timestamp: float


@dataclass
class AlphaToken:
    mint: str
    launchpad: str
    chain: str = "solana"
    first_seen: float = field(default_factory=time.time)

    # On-chain buy stream (Helius webhook)
    recent_buys: deque = field(default_factory=lambda: deque(maxlen=200))

    # DexScreener live snapshot
    vol_usd_5m: float = 0.0
    vol_usd_1h: float = 0.0
    vol_usd_6h: float = 0.0
    buy_vol_usd_1h: float = 0.0   # Improvement #3: directional when available
    buy_vol_usd_5m: float = 0.0
    sell_vol_usd_1h: float = 0.0  # #3: track sell side too
    liquidity_usd: float = 0.0
    market_cap_usd: float = 0.0
    price_usd: float = 0.0
    price_change_5m: float = 0.0
    price_change_1h: float = 0.0
    txns_5m_buys: int = 0
    txns_5m_sells: int = 0
    name: str = "Unknown"
    symbol: str = "UNK"
    pair_address: str = ""
    dex_id: str = ""
    dex_confirmed: bool = False

    price_change_24h: float = 0.0
    vol_usd_24h: float = 0.0
    txns_1h_buys: int = 0
    txns_1h_sells: int = 0
    total_supply: float = 0.0
    boosted: bool = False

    # #3: flag indicating buy_vol was derived from real transfer direction
    buy_vol_is_directional: bool = False

    # Alert state
    last_alert_ts: float = 0.0
    alert_count: int = 0
    last_score: float = 0.0
    first_alert_msg_id: int = 0
    watch_score: float = 0.0
    watch_ts: float = 0.0
    confirmed_count: int = 0
    source: str = "webhook"


# ─────────────────────────────────────────────────────────────────────────────
# Scoring engine v5  (Improvement #1: soft penalties)
# ─────────────────────────────────────────────────────────────────────────────

def score_token(token: AlphaToken, now: Optional[float] = None) -> Tuple[float, dict]:
    """
    v5 scoring algorithm — soft-gate edition.

    Stage 1: Absolute hard gates (cannot be penalised away).
    Stage 2: Soft gates — violations apply a multiplier penalty to the raw score.
    Stage 3: Normalised 0-100 quality score + launchpad/age bonuses.
    """
    now = now or time.time()
    bd: dict = {}
    penalties: list = []   # collect individual penalty factors — use worst-one-wins, NOT stacked
    violations: list = []  # human-readable gate violations

    # ── Stage 1: Absolute hard gates (zero-score on violation) ───────────────

    age_s = now - token.first_seen
    if age_s < ALPHA_MIN_TOKEN_AGE_S:
        bd["rejected"] = f"too_new ({age_s:.0f}s < {ALPHA_MIN_TOKEN_AGE_S}s)"
        return 0.0, bd
    if age_s > ALPHA_MAX_TOKEN_AGE_H * 3600:
        bd["rejected"] = f"too_old ({age_s/3600:.1f}h > {ALPHA_MAX_TOKEN_AGE_H}h)"
        return 0.0, bd

    # dex_confirmed is now a soft penalty, not a hard gate — catches tokens before DexScreener confirms
    if not token.dex_confirmed:
        violations.append("unconfirmed_dex")
        penalties.append(0.70)

    mc = token.market_cap_usd
    if mc <= 0:
        bd["rejected"] = "mc_zero"
        return 0.0, bd

    # Use total vol if directional buy vol unavailable
    v1h = token.buy_vol_usd_1h if token.buy_vol_usd_1h > 0 else token.vol_usd_1h * 0.6
    if v1h <= 0:
        bd["rejected"] = "vol_1h_missing"
        return 0.0, bd

    liq = token.liquidity_usd
    if liq <= 0:
        # Soft-penalise missing liq instead of hard reject — DexScreener often lags
        violations.append("liq_unconfirmed")
        penalties.append(0.60)
        liq = mc * 0.05  # estimate 5% liq/MC as fallback

    # ── Stage 2: Soft gates — worst-penalty-wins (NO stacking) (#1 fixed) ───────
    # CRITICAL FIX: penalties are collected and only the single worst one is applied.
    # Stacking (0.60×0.55×0.50×...) was crushing scores to ~6% — nothing passed 55.

    # MC band check
    if mc < SMART_MC_MIN_USD:
        violations.append(f"mc_low(${mc:,.0f}<${SMART_MC_MIN_USD:,.0f})")
        penalties.append(MC_PENALTY_FACTOR)
    elif mc > SMART_MC_MAX_USD:
        violations.append(f"mc_high(${mc:,.0f}>${SMART_MC_MAX_USD:,.0f})")
        penalties.append(MC_PENALTY_FACTOR)

    # Vol 1h band check
    if v1h < SMART_BUY_VOL_1H_MIN_USD:
        violations.append(f"vol_low(${v1h:,.0f}<${SMART_BUY_VOL_1H_MIN_USD:,.0f})")
        penalties.append(VOL_PENALTY_FACTOR)
    elif v1h > SMART_BUY_VOL_1H_MAX_USD:
        violations.append(f"vol_high(${v1h:,.0f}>${SMART_BUY_VOL_1H_MAX_USD:,.0f})")
        penalties.append(VOL_PENALTY_FACTOR)

    # Liquidity floor
    if liq < SMART_MIN_LIQUIDITY_USD:
        violations.append(f"liq_low(${liq:,.0f}<${SMART_MIN_LIQUIDITY_USD:,.0f})")
        penalties.append(LIQ_PENALTY_FACTOR)
    elif mc > 0 and (liq / mc) < SMART_MIN_LIQ_MC_RATIO:
        violations.append(f"liq_mc_ratio_low({liq/mc:.3f}<{SMART_MIN_LIQ_MC_RATIO})")
        penalties.append(LIQ_PENALTY_FACTOR)

    # Buy/sell ratio
    total_txns = token.txns_5m_buys + token.txns_5m_sells
    buy_ratio  = (token.txns_5m_buys / total_txns) if total_txns > 0 else 0.55
    if buy_ratio < SMART_MIN_BUY_RATIO:
        violations.append(f"sell_pressure(buy_ratio={buy_ratio:.2f}<{SMART_MIN_BUY_RATIO})")
        penalties.append(BUY_RATIO_PENALTY)

    # Minimum 1h buy transactions
    buy_tx_1h = token.txns_1h_buys if token.txns_1h_buys > 0 else token.txns_5m_buys * 12
    if buy_tx_1h < SMART_MIN_BUY_TX_1H:
        violations.append(f"low_buy_tx({buy_tx_1h}<{SMART_MIN_BUY_TX_1H})")
        penalties.append(TX_COUNT_PENALTY)

    # Vol acceleration
    v5m = token.buy_vol_usd_5m if token.buy_vol_usd_5m > 0 else token.vol_usd_5m * 0.6
    expected_5m = v1h / 12.0
    accel = (v5m / expected_5m) if expected_5m > 0 else 0.0
    if accel < SMART_MIN_ACCEL:
        violations.append(f"no_accel(accel={accel:.2f}<{SMART_MIN_ACCEL})")
        penalties.append(ACCEL_PENALTY)

    # Apply only the single worst penalty (not all stacked)
    penalty = min(penalties) if penalties else 1.0

    if violations:
        bd["soft_gate_penalties"] = f"worst={penalty:.3f}x ({len(violations)} violations): {'; '.join(violations)}"

    # ── Stage 3: Normalised quality score (0-100) ──────────────────────────────

    def clamp(v, lo=0.0, hi=1.0):
        return max(lo, min(hi, v))

    vol_band_q          = clamp(1.0 - abs(v1h - 35_000) / 15_000)
    mc_quality          = clamp(1.0 - abs(mc - 140_000) / 110_000)
    unique_buyers_est   = token.txns_1h_buys if token.txns_1h_buys > 0 else token.txns_5m_buys * 4
    wallet_div          = clamp(unique_buyers_est / 40.0)
    tx_density          = clamp(buy_tx_1h / 70.0)
    accel_score         = clamp(accel / 0.6)
    liq_mc              = liq / mc if mc > 0 else 0
    concentration_safety= clamp(liq_mc / 0.30)
    repeat_conv         = clamp((buy_ratio - 0.5) / 0.5)

    raw_score = 100.0 * (
        0.20 * vol_band_q        +
        0.15 * mc_quality        +
        0.20 * wallet_div        +
        0.15 * tx_density        +
        0.15 * accel_score       +
        0.10 * concentration_safety +
        0.05 * repeat_conv
    )

    # Apply soft-gate penalty
    penalised_score = raw_score * penalty

    # Bonuses
    lp_bonus = min(5.0, LAUNCHPAD_BONUSES.get(token.launchpad, 0.5) * 2.0)

    if 1800 <= age_s <= 21600:
        age_bonus = 3.0
    elif age_s < 1800:
        age_bonus = age_s / 600.0
    else:
        age_bonus = max(0.0, 3.0 - (age_s - 21600) / 14400)

    final_score = penalised_score + lp_bonus + age_bonus

    bd.update({
        "vol_band_q":            round(vol_band_q,          3),
        "mc_quality":            round(mc_quality,           3),
        "wallet_div":            round(wallet_div,           3),
        "tx_density":            round(tx_density,           3),
        "accel":                 round(accel,                3),
        "accel_score":           round(accel_score,          3),
        "concentration_safety":  round(concentration_safety, 3),
        "repeat_conv":           round(repeat_conv,          3),
        "raw_score":             round(raw_score,            2),
        "soft_gate_penalty":     round(penalty,              4),
        "penalised_score":       round(penalised_score,      2),
        "lp_bonus":              round(lp_bonus,             2),
        "age_bonus":             round(age_bonus,            2),
        "final_score":           round(final_score,          2),
        "launchpad":             token.launchpad,
        "age_minutes":           round(age_s / 60,           1),
        "vol_usd_1h":            round(v1h,                  0),
        "vol_usd_5m":            round(v5m,                  0),
        "buy_ratio":             round(buy_ratio,            3),
        "buy_tx_1h_est":         buy_tx_1h,
        "liq_mc_ratio":          round(liq_mc,               3),
        "vol_sol_1h_est":        round(v1h / SOL_PRICE_USD,  2),
        "buy_vol_directional":   token.buy_vol_is_directional,
    })

    return final_score, bd


def should_alert(
    token: AlphaToken,
    score: float,
    now: float,
    global_alert_times: deque,
) -> Tuple[bool, str]:
    """Two-stage gate: watch tier → confirmation → fire alert."""

    recent = [t for t in global_alert_times if now - t < 60]
    if len(recent) >= ALPHA_GLOBAL_CAP_PER_MIN:
        return False, f"global_cap ({len(recent)}/{ALPHA_GLOBAL_CAP_PER_MIN})"

    if token.last_alert_ts > 0:
        remaining = ALPHA_COOLDOWN_SECONDS - (now - token.last_alert_ts)
        if remaining > 0:
            return False, f"cooldown ({int(remaining)}s left)"

    if score < ALPHA_SCORE_WATCH:
        token.confirmed_count = 0
        token.watch_ts = 0.0
        return False, f"score_low ({score:.1f} < {ALPHA_SCORE_WATCH})"

    if score < ALPHA_SCORE_THRESHOLD:
        if token.watch_ts == 0.0:
            token.watch_ts = now
            token.watch_score = score
        token.confirmed_count = 1
        return False, f"watch_tier ({score:.1f} — waiting for ≥{ALPHA_SCORE_THRESHOLD})"

    if token.watch_ts > 0 and (now - token.watch_ts) >= ALPHA_CONFIRM_GAP_S:
        token.confirmed_count += 1
    else:
        token.confirmed_count = 1
        token.watch_ts = now

    if token.confirmed_count < ALPHA_CONFIRMATIONS:
        return False, f"awaiting_confirm ({token.confirmed_count}/{ALPHA_CONFIRMATIONS})"

    token.confirmed_count = 0
    token.watch_ts = 0.0
    return True, "ok"


# ─────────────────────────────────────────────────────────────────────────────
# DexScreener + fallback API helpers  (Improvements #2, #3)
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_birdeye_price(
    session: aiohttp.ClientSession,
    mint: str,
) -> Optional[float]:
    """
    Fallback price lookup via Birdeye public API.
    Returns USD price or None on failure.
    Requires BIRDEYE_API_KEY env var for reliable access.
    """
    try:
        url = BIRDEYE_PRICE_URL.format(mint)
        headers = {}
        if BIRDEYE_API_KEY:
            headers["X-API-KEY"] = BIRDEYE_API_KEY
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return float(data.get("data", {}).get("value", 0) or 0) or None
    except Exception as e:
        logger.debug(f"Birdeye fallback error {mint[:8]}: {e}")
        return None


async def _fetch_jupiter_price(
    session: aiohttp.ClientSession,
    mint: str,
) -> Optional[float]:
    """
    Fallback price lookup via Jupiter Price API v4 (free, no key).
    Returns USD price or None on failure.
    """
    try:
        url = JUPITER_PRICE_URL.format(mint)
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            price_data = (data.get("data") or {}).get(mint, {})
            return float(price_data.get("price", 0) or 0) or None
    except Exception as e:
        logger.debug(f"Jupiter fallback error {mint[:8]}: {e}")
        return None


async def fetch_dex_pair_data(
    session: aiohttp.ClientSession,
    mint: str,
) -> Optional[dict]:
    """
    Fetch best Solana pair for a token.

    Priority:
      1. DexScreener /latest/dex/tokens/{mint}  (full pair data)
      2. Birdeye price API                        (price-only fallback)
      3. Jupiter Price API                        (price-only fallback)

    When a fallback is used, returns a minimal synthetic pair dict so the
    caller can still enrich the token with at least a price and dex_confirmed=False.
    """
    # ── 1. DexScreener (preferred) ────────────────────────────────────────────
    try:
        url = DEX_TOKEN_URL.format(mint)
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get("pairs") or []
                if pairs:
                    sol_pairs = [p for p in pairs if p.get("chainId", "").lower() == "solana"]
                    candidates = sol_pairs or pairs
                    best = max(candidates, key=lambda p: float((p.get("volume") or {}).get("h24", 0) or 0))
                    return best
    except Exception as e:
        logger.debug(f"DexScreener error {mint[:8]}: {e}")

    if not FALLBACK_ENABLED:
        return None

    # ── 2. Birdeye fallback (#2) ─────────────────────────────────────────────
    price = await _fetch_birdeye_price(session, mint)
    if price:
        logger.debug(f"[Fallback] Birdeye price for {mint[:8]}: ${price:.8f}")
        return _synthetic_pair(mint, price, source="birdeye")

    # ── 3. Jupiter fallback (#2) ─────────────────────────────────────────────
    price = await _fetch_jupiter_price(session, mint)
    if price:
        logger.debug(f"[Fallback] Jupiter price for {mint[:8]}: ${price:.8f}")
        return _synthetic_pair(mint, price, source="jupiter")

    return None


def _synthetic_pair(mint: str, price_usd: float, source: str = "fallback") -> dict:
    """
    Build a minimal DexScreener-compatible pair dict from a price-only source.
    dex_confirmed will be set False in enrich_token_from_pair() because
    dexId / pairAddress will be empty.
    """
    return {
        "_fallback_source": source,
        "chainId": "solana",
        "dexId": "",
        "pairAddress": "",
        "baseToken": {"address": mint, "name": "Unknown", "symbol": "UNK"},
        "priceUsd": str(price_usd),
        "fdv": 0,
        "liquidity": {"usd": 0},
        "volume": {"m5": 0, "h1": 0, "h6": 0, "h24": 0},
        "txns": {},
        "priceChange": {},
    }


async def fetch_price_for_mint(mint: str) -> Optional[float]:
    """
    Convenience wrapper: fetch current USD price for a mint address.
    Used by database.run_performance_snapshot_worker().
    """
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
        pair = await fetch_dex_pair_data(session, mint)
        if not pair:
            return None
        try:
            return float(pair.get("priceUsd", 0) or 0) or None
        except Exception:
            return None


def enrich_token_from_pair(token: AlphaToken, pair: dict) -> None:
    """Update AlphaToken in-place from a DexScreener pair object."""
    vol  = pair.get("volume") or {}
    txns = pair.get("txns") or {}
    pc   = pair.get("priceChange") or {}
    liq  = pair.get("liquidity") or {}

    token.vol_usd_5m       = float(vol.get("m5", 0) or 0)
    token.vol_usd_1h       = float(vol.get("h1", 0) or 0)
    token.vol_usd_6h       = float(vol.get("h6", 0) or 0)
    token.liquidity_usd    = float(liq.get("usd", 0) or 0)
    token.market_cap_usd   = float(pair.get("fdv", 0) or 0)
    token.price_usd        = float(pair.get("priceUsd", 0) or 0)
    token.price_change_5m  = float(pc.get("m5", 0) or 0)
    token.price_change_1h  = float(pc.get("h1", 0) or 0)
    token.txns_5m_buys     = int(((txns.get("m5") or {}).get("buys")  or 0))
    token.txns_5m_sells    = int(((txns.get("m5") or {}).get("sells") or 0))
    token.txns_1h_buys     = int(((txns.get("h1") or {}).get("buys")  or 0))
    token.txns_1h_sells    = int(((txns.get("h1") or {}).get("sells") or 0))

    # Improvement #3: derive buy-only volume from txn count ratio
    # (only used when directional data from Helius is NOT available)
    if not token.buy_vol_is_directional:
        total_1h    = token.txns_1h_buys + token.txns_1h_sells
        buy_ratio_1h = (token.txns_1h_buys / total_1h) if total_1h > 0 else 0.5
        token.buy_vol_usd_1h = token.vol_usd_1h * buy_ratio_1h

        total_5m    = token.txns_5m_buys + token.txns_5m_sells
        buy_ratio_5m = (token.txns_5m_buys / total_5m) if total_5m > 0 else 0.5
        token.buy_vol_usd_5m = token.vol_usd_5m * buy_ratio_5m

    token.vol_usd_24h      = float(vol.get("h24", 0) or 0)
    token.price_change_24h = float(pc.get("h24", 0) or 0)
    token.total_supply     = float((pair.get("baseToken") or {}).get("totalSupply", 0) or 0)
    token.pair_address     = pair.get("pairAddress", "")
    token.dex_id           = pair.get("dexId", "")

    # Only mark dex_confirmed for real DexScreener data (not fallbacks)
    is_fallback = bool(pair.get("_fallback_source"))
    if not is_fallback:
        token.dex_confirmed = True

    # Seed first_seen from DexScreener pairCreatedAt if available
    pair_created_at_ms = pair.get("pairCreatedAt")
    if pair_created_at_ms:
        real_first_seen = float(pair_created_at_ms) / 1000.0
        if real_first_seen < token.first_seen:
            token.first_seen = real_first_seen

    # Upgrade launchpad label from DexScreener dexId
    if token.launchpad in ("Unknown", "Pump.fun") and token.dex_id:
        dl = token.dex_id.lower()
        if "pumpswap" in dl or "pump_amm" in dl or "pump-amm" in dl:
            token.launchpad = "PumpSwap"
        elif "raydium" in dl:
            token.launchpad = "Raydium AMM"
        elif "meteora" in dl:
            token.launchpad = "Meteora DLMM"
        elif "moonshot" in dl:
            token.launchpad = "Moonshot"

    base = pair.get("baseToken") or {}
    if base.get("name") and token.name == "Unknown":
        token.name = base["name"]
    if base.get("symbol") and token.symbol == "UNK":
        token.symbol = base["symbol"]


def enrich_token_from_helius_transfers(
    token: AlphaToken,
    transfers: List[dict],
    sol_price_usd: float = SOL_PRICE_USD,
) -> None:
    """
    Improvement #3: use actual Helius enhanced-transaction tokenTransfer data
    to compute directional buy volume instead of the txn-count approximation.

    Each transfer dict is expected to have the Helius shape:
      {
        "mint": str,
        "fromUserAccount": str,
        "toUserAccount": str,
        "tokenAmount": float,          # in raw lamports / decimals
        "type": "transfer" | ...
      }

    Heuristic: if toUserAccount is a known DEX program → it's a sell.
               otherwise → it's a buy.
    This is a best-effort heuristic; it's still more accurate than ratio math.
    """
    buy_sol  = 0.0
    sell_sol = 0.0

    DEX_PROGRAMS = set(LAUNCHPAD_PROGRAMS.keys())

    for xfer in transfers:
        to_acct  = xfer.get("toUserAccount", "") or ""
        from_acct= xfer.get("fromUserAccount", "") or ""
        amount   = float(xfer.get("tokenAmount", 0) or 0)

        if amount <= 0:
            continue

        sol_value = amount * 0.00001  # approximate token_amount → SOL
        if to_acct in DEX_PROGRAMS:
            sell_sol += sol_value
        elif from_acct in DEX_PROGRAMS:
            buy_sol += sol_value
        else:
            buy_sol += sol_value  # default to buy when direction ambiguous

    if buy_sol > 0 or sell_sol > 0:
        # Convert to USD and set as the authoritative buy volume
        token.buy_vol_usd_5m     = buy_sol * sol_price_usd
        token.buy_vol_usd_1h     = token.buy_vol_usd_5m * 12  # rough 1h projection
        token.sell_vol_usd_1h    = sell_sol * sol_price_usd * 12
        token.buy_vol_is_directional = True


def detect_launchpad_from_tx(payload: dict) -> str:
    source = (payload.get("source") or "").lower()
    if source:
        if "pumpswap" in source or "pump_amm" in source:
            return "PumpSwap"
        if "raydium" in source:
            return "Raydium AMM"
        if "meteora" in source:
            return "Meteora DLMM"
        if "moonshot" in source:
            return "Moonshot"
        if "pump" in source:
            return "Pump.fun"

    meta = payload.get("meta") or {}
    for log in (meta.get("logMessages") or []):
        for prog_id, name in LAUNCHPAD_PROGRAMS.items():
            if f"Program {prog_id}" in log:
                return name

    tx  = payload.get("transaction") or {}
    msg = tx.get("message") or {}
    for key in (msg.get("accountKeys") or []):
        k = key if isinstance(key, str) else (key.get("pubkey") or "")
        if k in LAUNCHPAD_PROGRAMS:
            return LAUNCHPAD_PROGRAMS[k]

    return "Unknown"


# ─────────────────────────────────────────────────────────────────────────────
# DexScreener Poller
# ─────────────────────────────────────────────────────────────────────────────

class DexScreenerPoller:
    """
    Polls DexScreener's free /token-profiles and /token-boosts endpoints
    every ALPHA_POLL_INTERVAL seconds to discover new Solana tokens across
    all DEXes without consuming Helius credits.

    Falls back to Birdeye/Jupiter for enrichment when DexScreener is unavailable.
    """

    def __init__(
        self,
        token_registry: Dict[str, AlphaToken],
        registry_lock: asyncio.Lock,
        on_new_token: Optional[Callable[[AlphaToken], Awaitable[None]]] = None,
        on_alert: Optional[Callable[[AlphaToken, float, dict], Awaitable[None]]] = None,
        global_alert_times: Optional[deque] = None,
    ):
        self.registry          = token_registry
        self.lock              = registry_lock
        self.on_new_token      = on_new_token
        self.on_alert          = on_alert
        self.global_alert_times= global_alert_times or deque(maxlen=200)
        self._seen_profiles: set = set()
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"Accept": "application/json"},
            )
        return self._session

    async def _poll_endpoint(self, url: str) -> List[dict]:
        try:
            s = await self._get_session()
            async with s.get(url) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            logger.debug(f"[DexPoller] {url} error: {e}")
            return []

    async def _register(self, mint: str, source: str) -> AlphaToken:
        async with self.lock:
            if mint not in self.registry:
                token = AlphaToken(
                    mint=mint, launchpad="Unknown", chain="solana",
                    first_seen=time.time(), source=source,
                )
                self.registry[mint] = token
                logger.info(f"🆕 [DexPoller] {mint[:16]}… source={source}")
                if self.on_new_token:
                    asyncio.create_task(self.on_new_token(token))
            return self.registry[mint]

    async def _enrich_and_score(self, token: AlphaToken):
        try:
            s   = await self._get_session()
            pair = await fetch_dex_pair_data(s, token.mint)   # uses fallback chain
            now  = time.time()
            async with self.lock:
                t = self.registry.get(token.mint)
                if not t:
                    return
                if pair:
                    enrich_token_from_pair(t, pair)
                score, bd = score_token(t, now)
                t.last_score = score
                ok, reason = should_alert(t, score, now, self.global_alert_times)
                if ok:
                    t.last_alert_ts = now
                    t.alert_count  += 1
                    self.global_alert_times.append(now)
                    bd["alert_count"]        = t.alert_count
                    bd["first_alert_msg_id"] = t.first_alert_msg_id
                    snapshot, alert_score, alert_bd = t, score, bd
                else:
                    snapshot = None
                    if score > ALPHA_SCORE_THRESHOLD * 0.7:
                        logger.info(
                            f"📊 [DexPoller] Near-miss {t.symbol or t.mint[:8]} "
                            f"score={score:.1f} gate={reason} lp={t.launchpad}"
                        )
            if snapshot and self.on_alert:
                logger.info(f"🚨 [DexPoller] ALERT {snapshot.symbol} score={alert_score:.1f}")
                await self.on_alert(snapshot, alert_score, alert_bd)
        except Exception as e:
            logger.debug(f"[DexPoller] enrich_and_score error {token.mint[:8]}: {e}")

    async def run_poll_cycle(self):
        profiles = await self._poll_endpoint(DEX_LATEST_PROFILES_URL)
        boosts   = await self._poll_endpoint(DEX_LATEST_BOOSTS_URL)

        new_mints: Dict[str, str] = {}
        for item in profiles:
            if item.get("chainId", "").lower() != "solana":
                continue
            mint = item.get("tokenAddress", "")
            if mint and mint not in IGNORED_MINTS and mint not in self._seen_profiles:
                new_mints[mint] = "dexscreener_poll"
                self._seen_profiles.add(mint)

        for item in boosts:
            if item.get("chainId", "").lower() != "solana":
                continue
            mint = item.get("tokenAddress", "")
            if mint and mint not in IGNORED_MINTS:
                new_mints[mint] = "boost"

        if new_mints:
            logger.info(f"🔍 [DexPoller] {len(new_mints)} new Solana tokens found")

        for mint, source in new_mints.items():
            await self._register(mint, source)

        cutoff = time.time() - ALPHA_MAX_TOKEN_AGE_H * 3600
        async with self.lock:
            to_check = [t for t in self.registry.values() if t.first_seen > cutoff]

        CHUNK = 10
        for i in range(0, len(to_check), CHUNK):
            await asyncio.gather(
                *(self._enrich_and_score(t) for t in to_check[i:i + CHUNK]),
                return_exceptions=True,
            )
            if i + CHUNK < len(to_check):
                await asyncio.sleep(1)

    async def start(self):
        logger.info(f"🚀 [DexPoller] Running every {ALPHA_POLL_INTERVAL}s")
        while True:
            try:
                await self.run_poll_cycle()
            except Exception as e:
                logger.error(f"[DexPoller] Cycle error: {e}")
            await asyncio.sleep(ALPHA_POLL_INTERVAL)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ─────────────────────────────────────────────────────────────────────────────
# Helius webhook payload processor  (uses #2 fallback, #3 directional vol)
# ─────────────────────────────────────────────────────────────────────────────

async def process_webhook_payload(
    payload: dict,
    token_registry: Dict[str, AlphaToken],
    registry_lock: asyncio.Lock,
    global_alert_times: deque,
    on_alert: Optional[Callable[[AlphaToken, float, dict], Awaitable[None]]] = None,
) -> Optional[str]:
    """
    Process one Helius enhanced-transaction webhook payload.
    Returns mint address or None if skipped.
    """
    mint_str = None
    if "mint" in payload:
        mint_str = payload["mint"]
    elif payload.get("tokenTransfers"):
        for xfer in payload["tokenTransfers"]:
            if xfer.get("mint"):
                mint_str = xfer["mint"]
                break

    if not mint_str or mint_str in IGNORED_MINTS:
        return None

    launchpad = detect_launchpad_from_tx(payload)
    now = time.time()

    async with registry_lock:
        if mint_str not in token_registry:
            token_registry[mint_str] = AlphaToken(
                mint=mint_str, launchpad=launchpad, chain="solana",
                first_seen=now, source="webhook",
            )
        else:
            t = token_registry[mint_str]
            if t.launchpad in ("Unknown", "Pump.fun") and launchpad not in ("Unknown",):
                t.launchpad = launchpad

    # Record buy events
    transfers = payload.get("tokenTransfers") or []
    for xfer in transfers:
        to_wallet = xfer.get("toUserAccount") or xfer.get("toUserAccountOwner") or ""
        amount    = float(xfer.get("tokenAmount", 0) or 0)
        if to_wallet and amount > 0:
            async with registry_lock:
                t = token_registry.get(mint_str)
                if t:
                    t.recent_buys.append(BuyEvent(
                        wallet=to_wallet,
                        amount_sol=amount * 0.00001,
                        timestamp=now,
                    ))

    # Improvement #3: enrich directional buy volume from transfers
    async with registry_lock:
        t = token_registry.get(mint_str)
        if t and transfers:
            enrich_token_from_helius_transfers(t, transfers)

    # DexScreener enrichment + scoring  (with fallback chain #2)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as session:
            pair = await fetch_dex_pair_data(session, mint_str)

        async with registry_lock:
            t = token_registry.get(mint_str)
            if not t:
                return mint_str
            if pair:
                enrich_token_from_pair(t, pair)
            score, bd = score_token(t, now)
            t.last_score = score
            ok, reason = should_alert(t, score, now, global_alert_times)
            if ok:
                t.last_alert_ts = now
                t.alert_count  += 1
                global_alert_times.append(now)
                bd["alert_count"]        = t.alert_count
                bd["first_alert_msg_id"] = t.first_alert_msg_id
                snapshot, alert_score, alert_bd = t, score, bd
            else:
                snapshot = None
                if bd.get("final_score", 0) > ALPHA_SCORE_THRESHOLD * 0.6:
                    logger.info(
                        f"📊 [Webhook] {t.symbol or mint_str[:8]} "
                        f"score={score:.1f} gate={reason} lp={t.launchpad} "
                        f"penalty={bd.get('soft_gate_penalty', 1.0):.3f}"
                    )

        if snapshot and on_alert:
            logger.info(f"🚨 [Webhook Alert] {snapshot.symbol} score={alert_score:.1f}")
            await on_alert(snapshot, alert_score, alert_bd)

    except Exception as e:
        logger.debug(f"[Webhook] Score error {mint_str[:8]}: {e}")

    return mint_str


# ─────────────────────────────────────────────────────────────────────────────
# Alert formatter
# ─────────────────────────────────────────────────────────────────────────────

def format_alpha_alert(token: AlphaToken, score: float, bd: dict, is_update: bool = False) -> str:
    """Format a Telegram HTML alert — Gems Radar card layout, AlphaDegen data."""

    def fmt_usd(v: float) -> str:
        if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
        if v >= 1_000:     return f"${v/1_000:.1f}K"
        return f"${v:.0f}"

    def fmt_price(p: float) -> str:
        if p <= 0:       return "N/A"
        if p >= 1:       return f"${p:,.4f}"
        if p >= 0.01:    return f"${p:.4f}"
        if p >= 0.0001:  return f"${p:.6f}"
        return            f"${p:.8f}"

    addr = token.mint
    name = token.name
    sym  = token.symbol
    lp   = token.launchpad or "Unknown"

    age_m = bd.get("age_minutes", 0)
    if age_m < 60:     age_str = f"{age_m:.0f} mins"
    elif age_m < 1440: age_str = f"{age_m/60:.1f}h"
    else:              age_str = f"{age_m/1440:.1f}d"

    tier = (
        "🔥🔥🔥 ULTRA ALPHA" if score >= 40 else
        "🔥🔥 HIGH ALPHA"    if score >= 35 else
        "🔥 ALPHA"
    )
    if is_update:
        tier = "🔄 UPDATE — " + tier

    # Penalty indicator (#1)
    penalty = bd.get("soft_gate_penalty", 1.0)
    penalty_str = f" ⚠️ penalties={penalty:.2f}x" if penalty < 0.9 else ""

    buy_vol_1h = token.buy_vol_usd_1h or bd.get("vol_usd_1h", 0)
    vol_sol    = buy_vol_1h / SOL_PRICE_USD
    vol_str    = f"{vol_sol:.1f} SOL ({fmt_usd(buy_vol_1h)})"
    dir_tag    = " ✅dir" if token.buy_vol_is_directional else " ~est"

    buys  = token.txns_5m_buys
    sells = token.txns_5m_sells
    total = buys + sells
    buy_blocks  = int((buys / total * 10) + 0.5) if total else 5
    sell_blocks = 10 - buy_blocks
    bar = "🟩" * buy_blocks + "🟥" * sell_blocks

    score_bd = (
        f"vol={bd.get('vol_band_q',0):.2f} "
        f"mc={bd.get('mc_quality',0):.2f} "
        f"div={bd.get('wallet_div',0):.2f} "
        f"acc={bd.get('accel',0):.2f} "
        f"liq={bd.get('concentration_safety',0):.2f}"
    )

    ds_url     = f"https://dexscreener.com/solana/{addr}"
    scan_url   = f"https://solscan.io/token/{addr}"
    pump_url   = f"https://pump.fun/{addr}"
    gt_url     = f"https://www.gmgn.ai/sol/token/{addr}"
    axiom_url  = f"https://axiom.trade/meme/{addr}"
    photon_url = f"https://photon-sol.tinyastro.io/en/lp/{addr}"

    msg  = "🔎 <b>AlphaDegen</b>\n"
    msg += f"<b>{tier}</b>{penalty_str}\n\n"
    msg += f"<b>{name} — #{sym} | ${sym}</b>\n"
    msg += f"<code>{addr}</code>\n\n"
    msg += f"💰 <b>MarketCap:</b> {fmt_usd(token.market_cap_usd)}\n"
    msg += f"🕐 <b>Age:</b> {age_str}\n"
    msg += f"📊 <b>Buy Vol 1h:</b> {vol_str}{dir_tag}\n"
    msg += f"💧 <b>Liquidity:</b> {fmt_usd(token.liquidity_usd)}\n"
    msg += f"📈 <b>Price:</b> {fmt_price(token.price_usd)}"
    if token.price_change_1h:
        msg += f"  ({token.price_change_1h:+.1f}% 1h)"
    msg += "\n"
    msg += f"🔄 <b>5m Txns:</b> {bar}  🟩{buys} 🟥{sells}\n"
    msg += f"   <b>Δ 5m:</b> {token.price_change_5m:+.1f}%  |  <b>Δ 1h:</b> {token.price_change_1h:+.1f}%\n"
    msg += f"🏦 <b>DEX:</b> {lp}\n\n"
    msg += f"🎯 <b>Score: {score:.1f}</b>  [{score_bd}]\n\n"
    msg += f"📉 <a href='{ds_url}'>Chart</a>  "
    msg += f"🔍 <a href='{scan_url}'>Explorer</a>\n\n"
    msg += "⚡ <b>Quick Buy</b>\n"
    msg += f"<a href='{axiom_url}'>Axiom</a> · "
    msg += f"<a href='{photon_url}'>Photon</a> · "
    msg += f"<a href='{pump_url}'>Pump.fun</a> · "
    msg += f"<a href='{gt_url}'>GMGN</a>"

    return msg


async def send_alpha_alert(
    token: AlphaToken,
    score: float,
    bd: dict,
    bot,
    chat_id: str,
) -> None:
    """Send alert to Telegram. On repeat alerts, reply to the original message."""
    alert_count  = bd.get("alert_count", token.alert_count)
    first_msg_id = bd.get("first_alert_msg_id", token.first_alert_msg_id)
    is_update    = alert_count > 1 and first_msg_id > 0

    text = format_alpha_alert(token, score, bd, is_update=is_update)
    kwargs = dict(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    if is_update:
        kwargs["reply_to_message_id"] = first_msg_id

    try:
        sent = await bot.send_message(**kwargs)
        if alert_count == 1:
            token.first_alert_msg_id = sent.message_id
    except Exception as e:
        logger.error(f"send_alpha_alert error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Background cleanup
# ─────────────────────────────────────────────────────────────────────────────

async def cleanup_old_alpha_tokens(
    registry: Dict[str, AlphaToken],
    lock: asyncio.Lock,
    max_age_hours: float = ALPHA_MAX_TOKEN_AGE_H,
    interval_seconds: int = 300,
):
    """Periodically purge stale tokens from the registry."""
    while True:
        try:
            cutoff = time.time() - max_age_hours * 3600
            async with lock:
                stale = [m for m, t in registry.items() if t.first_seen < cutoff]
                for m in stale:
                    del registry[m]
                if stale:
                    logger.info(f"🧹 [Alpha] Purged {len(stale)} stale tokens")
        except Exception as e:
            logger.error(f"[Alpha] Cleanup error: {e}")
        await asyncio.sleep(interval_seconds)
