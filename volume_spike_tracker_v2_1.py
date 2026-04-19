#!/usr/bin/env python3
"""
AlphaDetector Volume Spike Tracker v2.1
======================================
High-signal, low-noise Solana token launch + volume spike detector.

Key Features:
- Launch detection via logs websocket subscription (Pump.fun + others)
- Real 24h volume polling (DexScreener) every 60s
- Trend-aware spike detection (recent avg * multiplier, positive linear slope)
- Cooldown per token to avoid repeated spam
- Milestone alerts only secondary (and deduped for 2h) if no spike
- Telegram alert dedup via DB (was_alert_sent_recently)

Improvements vs provided draft:
- Fixed rate_limiter to use config.max_concurrent_requests
- Corrected last_volume_sync logic (use previous timestamp before updating)
- Explicit mint parameter in send_telegram_alert (no fragile message parsing)
- Safer percent spike calculation (guard prev_volume == 0)
"""
import asyncio
import json
import time
import logging
import numpy as np
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, Optional, List, Set
from functools import lru_cache
import aiohttp
from solana.rpc.async_api import AsyncClient
from solana.rpc.websocket_api import connect
from solana.publickey import PublicKey
from solana.rpc.types import RpcLogsFilter

from config import get_config
from circuit_breaker import get_circuit_breaker
from database import (
    init_db, add_seen_program, is_program_seen, log_token_volume,
    log_alert, was_alert_sent_recently, cleanup_old_data, get_token_history
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

config = get_config()
CB_HELIUS = get_circuit_breaker("helius", failure_threshold=5, recovery_timeout=60)
CB_DEXSCREENER = get_circuit_breaker("dexscreener", failure_threshold=3, recovery_timeout=30)

LAUNCHPADS = {
    "Pump.fun": PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
    "LetsBonk.fun": PublicKey("FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1"),
    "Moonshot": PublicKey("moonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG"),
}

SMART_CRITERIA = {
    "age_days": 7,
    "balance_sol": 5,
    "tx_count": 50
}

CABAL_EARLY_BUYERS = 3
CABAL_SHARED_FUNDERS = 2

VOLUME_MILESTONES = [5, 10, 25, 50]

VOLUME_SPIKE_CONFIG = {
    "min_points_for_trend": 5,
    "spike_multiplier": 2.0,
    "min_volume_for_spike": 3.0,
    "cooldown_minutes": 10,
    "trend_window_minutes": 5,
}

rate_limiter = asyncio.Semaphore(config.max_concurrent_requests)

@dataclass
class WalletInfo:
    age_days: float = 0.0
    balance_sol: float = 0.0
    tx_count: int = 0
    is_smart: bool = False

@dataclass
class TokenInfo:
    mint: PublicKey
    launchpad: str
    created_at: float
    volume_sol: float = 0.0
    smart_volume_sol: float = 0.0
    buyers: Dict[str, float] = None
    smart_wallets: Set[str] = None
    cabal_score: int = 0
    last_volume_sync: float = 0.0
    last_spike_alert: float = 0.0

    def __post_init__(self):
        self.buyers = defaultdict(float)
        self.smart_wallets = set()

active_tokens: Dict[str, TokenInfo] = {}
wallets_cache = lru_cache(maxsize=1000)(lambda addr: None)  # placeholder
processed_logs: Set[str] = set()
token_queue = deque(maxlen=100)

async def get_wallet_info(session: aiohttp.ClientSession, addr: str) -> WalletInfo:
    async with rate_limiter:
        info = WalletInfo()
        try:
            async def fetch_helius():
                url = f"{config.api.helius_url}/addresses/{addr}/balances?api-key={config.api.helius_api_key}"
                async with session.get(url) as resp:
                    data = await resp.json()
                    info.balance_sol = data.get("nativeBalance", 0) / 1e9
                    info.tx_count = data.get("transactionCount", 0)
            await CB_HELIUS.call(fetch_helius)

            async def fetch_age():
                url = f"{config.api.helius_url}/transactions?api-key={config.api.helius_api_key}"
                params = {"accounts": [addr], "limit": 1, "sort": "asc"}
                async with session.get(url, params=params) as resp:
                    txs = await resp.json()
                    if txs:
                        info.age_days = (time.time() - txs[0]["timestamp"]) / 86400
            await CB_HELIUS.call(fetch_age)

            info.is_smart = (
                info.age_days >= SMART_CRITERIA["age_days"] or
                info.balance_sol >= SMART_CRITERIA["balance_sol"] or
                info.tx_count >= SMART_CRITERIA["tx_count"]
            )
        except Exception as e:
            logger.error(f"Wallet info error for {addr}: {e}")
        return info

async def detect_cabal(session: aiohttp.ClientSession, token: TokenInfo) -> int:
    score = 0
    early_buyers = [b for b, amt in token.buyers.items() if time.time() - token.created_at < 300]
    if len(early_buyers) >= CABAL_EARLY_BUYERS:
        score += 4
    funders = set()
    for buyer in early_buyers[:3]:
        try:
            async def fetch_funders():
                url = f"{config.api.helius_url}/transactions?api-key={config.api.helius_api_key}"
                params = {"accounts": [buyer], "limit": 10, "sort": "desc"}
                async with session.get(url, params=params) as resp:
                    txs = await resp.json()
                    for tx in txs:
                        if "transfer" in tx.get("description", "").lower():
                            funders.add(tx.get("source", ""))
            await CB_HELIUS.call(fetch_funders)
        except Exception:
            pass
    if len(funders) <= CABAL_SHARED_FUNDERS:
        score += 3
    score += 3 if score > 0 else 0
    return min(score, 10)

def extract_mint_from_pump_create(tx_json) -> Optional[PublicKey]:
    instructions = tx_json.get("transaction", {}).get("message", {}).get("instructions", [])
    for ix in instructions:
        if str(PublicKey(ix.get("programId", ""))) == str(LAUNCHPADS["Pump.fun"]):
            accounts = ix.get("accounts", [])
            if accounts:
                return PublicKey(accounts[0])
    return None

def extract_mint_from_tx(tx_json) -> Optional[PublicKey]:
    mint = extract_mint_from_pump_create(tx_json)
    if mint:
        return mint
    for log in tx_json.get("meta", {}).get("logMessages", []):
        if "Created token" in log or "InitializeMint" in log:
            accounts = tx_json.get("transaction", {}).get("message", {}).get("accountKeys", [])
            for acc in accounts:
                if len(str(acc["pubkey"])) == 44:
                    return PublicKey(acc["pubkey"])
    inner = tx_json.get("meta", {}).get("innerInstructions", [])
    for iix_group in inner:
        for iix in iix_group.get("instructions", []):
            parsed = iix.get("parsed", {})
            if parsed.get("type") == "initializeMint":
                return PublicKey(parsed["info"]["mint"])
    return None

async def get_real_volume_dexscreener(session: aiohttp.ClientSession, mint: str) -> float:
    async def fetch_volume():
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with session.get(url) as resp:
            data = await resp.json()
            if "pairs" in data and data["pairs"]:
                top_pair = max(data["pairs"], key=lambda p: float(p.get("volume", {}).get("h24", 0)))
                volume_usd = float(top_pair.get("volume", {}).get("h24", 0))
                return volume_usd / 180.0
        return 0.0
    try:
        return await CB_DEXSCREENER.call(fetch_volume)
    except Exception as e:
        logger.error(f"DexScreener error for {mint}: {e}")
        return 0.0

async def detect_volume_spike(mint: str, current_volume: float) -> bool:
    history = await get_token_history(mint, hours_back=1)
    if len(history) < VOLUME_SPIKE_CONFIG["min_points_for_trend"]:
        return False
    cutoff = time.time() - (VOLUME_SPIKE_CONFIG["trend_window_minutes"] * 60)
    recent = [(ts, vol) for ts, vol in history if ts > cutoff]
    if len(recent) < 2:
        return False
    recent_vols = [vol for _, vol in recent]
    avg_volume = np.mean(recent_vols)
    if current_volume < VOLUME_SPIKE_CONFIG["min_volume_for_spike"] or current_volume <= avg_volume * VOLUME_SPIKE_CONFIG["spike_multiplier"]:
        return False
    times = np.array([ts for ts, _ in recent])
    vols = np.array(recent_vols)
    try:
        slope = np.polyfit(times - times.mean(), vols, 1)[0]  # center times to improve stability
    except Exception:
        return False
    if slope <= 0:
        return False
    token = active_tokens.get(mint)
    if token and time.time() - token.last_spike_alert < VOLUME_SPIKE_CONFIG["cooldown_minutes"] * 60:
        return False
    logger.info(f"🔥 Spike detected for {mint}: {current_volume:.2f} SOL (avg: {avg_volume:.2f}, slope: {slope:.2f})")
    return True

async def send_telegram_alert(message: str, alert_type: str = "general", mint: str = ""):
    if mint and await was_alert_sent_recently(mint, alert_type, hours=1):
        logger.info(f"Alert {alert_type} suppressed for {mint}")
        return
    url = f"https://api.telegram.org/bot{config.telegram.bot_token}/sendMessage"
    payload = {
        "chat_id": config.telegram.chat_id,
        "text": message,
        "parse_mode": config.telegram.parse_mode,
        "disable_web_page_preview": config.telegram.disable_web_preview
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                if mint:
                    await log_alert(mint, alert_type, message)
                logger.info(f"Alert sent: {alert_type}")
            else:
                logger.error(f"Telegram send failed: {await resp.text()}")

async def listen_to_launchpads():
    async with connect(config.api.rpc_wss) as ws:
        for name, program in LAUNCHPADS.items():
            filter_ = RpcLogsFilter.Mentions([str(program)])
            await ws.logs_subscribe(filter_, commitment="processed")
            logger.info(f"Subscribed to logs for {name} ({program})")
        async for msg in ws:
            if msg.get("method") != "logsNotification":
                continue
            logs = msg["params"]["result"]["value"]["logs"]
            signature = msg["params"]["result"]["value"]["signature"]
            if signature in processed_logs:
                continue
            processed_logs.add(signature)
            if any("Program log: Instruction: Create" in log for log in logs):
                async with AsyncClient(config.api.rpc_https) as client:
                    tx = await client.get_transaction(signature, encoding="jsonParsed")
                    if not tx or tx["result"]["meta"]["err"]:
                        continue
                    mint = extract_mint_from_tx(tx["result"])
                    if mint:
                        mint_str = str(mint)
                        if mint_str not in active_tokens:
                            program_str = next((name for name, p in LAUNCHPADS.items() if str(p) in [str(acc["pubkey"]) for acc in tx["result"]["transaction"]["message"]["accountKeys"]]), "Unknown")
                            token = TokenInfo(mint=mint, launchpad=program_str, created_at=time.time())
                            active_tokens[mint_str] = token
                            token_queue.append(mint_str)
                            alert_msg = f"🚀 NEW TOKEN: {mint_str[:8]}... on {program_str}"
                            await send_telegram_alert(alert_msg, "launch", mint=mint_str)
                            logger.info(alert_msg)

async def sync_real_volumes(session: aiohttp.ClientSession):
    while True:
        now = time.time()
        to_remove = []
        for mint_str, token in list(active_tokens.items()):
            if now - token.created_at > config.monitor_hours_old * 3600:
                to_remove.append(mint_str)
                continue
            volume = await get_real_volume_dexscreener(session, mint_str)
            prev_volume = token.volume_sol
            prev_last_sync = token.last_volume_sync
            token.volume_sol = volume
            # Log history
            await log_token_volume(mint_str, volume)
            # Smart wallet + cabal update every 5 min
            if now - prev_last_sync > 300 and token.volume_sol > config.detection.min_sol_volume:
                buyers_sample = list(token.buyers.keys())[:5]
                for buyer in buyers_sample:
                    w_info = await get_wallet_info(session, buyer)
                    if w_info.is_smart:
                        token.smart_volume_sol += token.buyers[buyer]
                        token.smart_wallets.add(buyer)
                token.cabal_score = await detect_cabal(session, token)
            token.last_volume_sync = now
            # Spike detection
            is_spike = await detect_volume_spike(mint_str, volume)
            if is_spike and volume >= config.detection.min_sol_volume:
                token.last_spike_alert = now
                pct = ((volume / prev_volume - 1) * 100) if prev_volume > 0 else 0
                alert_msg = (
                    f"🔥 VOLUME SPIKE on {token.launchpad}: {mint_str[:8]}... {volume:.2f} SOL "
                    f"(+{pct:.0f}% vs prev) | Smart: {len(token.smart_wallets)} | Cabal: {token.cabal_score}/10"
                )
                await send_telegram_alert(alert_msg, "spike", mint=mint_str)
            elif prev_volume < volume:
                for milestone in VOLUME_MILESTONES:
                    if prev_volume < milestone <= volume and not await was_alert_sent_recently(mint_str, f"milestone_{milestone}", hours=2):
                        alert_msg = f"🎯 {token.launchpad} {mint_str[:8]}... HIT {milestone} SOL (stable growth) | Smart: {len(token.smart_wallets)}"
                        await send_telegram_alert(alert_msg, f"milestone_{milestone}", mint=mint_str)
        for mint_str in to_remove:
            del active_tokens[mint_str]
        await asyncio.sleep(60)

async def print_table():
    while True:
        if active_tokens:
            now = time.time()
            rows = []
            for mint_str, info in sorted(active_tokens.items(), key=lambda x: x[1].volume_sol, reverse=True):
                age_min = (now - info.created_at) / 60
                rows.append([
                    mint_str[:8] + "..." + mint_str[-6:],
                    info.launchpad,
                    f"{info.volume_sol:.2f}",
                    f"{info.smart_volume_sol:.2f}",
                    info.cabal_score,
                    len(info.smart_wallets),
                    f"{age_min:.1f}m",
                ])
            try:
                import tabulate
                logger.info("\n" + tabulate.tabulate(
                    rows,
                    headers=["Mint", "Launchpad", "Vol", "Smart Vol", "Cabal", "Smart W", "Age"],
                    tablefmt="grid"
                ))
            except Exception:
                logger.info(f"Tracked tokens: {len(rows)}")
        await asyncio.sleep(10)

async def periodic_cleanup():
    while True:
        await cleanup_old_data(days=7)
        await asyncio.sleep(3600)

async def main():
    if not config.api.helius_api_key or config.api.helius_api_key == "your_helius_api_key_here":
        raise ValueError("Set HELIUS_API_KEY in .env!")
    await init_db()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=config.api.api_timeout)) as session:
        logger.info("🚀 Starting v2.1 Volume Spike Tracker (Trend-Aware)")
        await asyncio.gather(
            listen_to_launchpads(),
            sync_real_volumes(session),
            print_table(),
            periodic_cleanup(),
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
