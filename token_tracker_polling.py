#!/usr/bin/env python3
"""
Solana Token Tracker v4.8 - WebSocket + Polling + KOL + Smart Buy Scoring
=======================================================

Features:
- ✅ [v4.5] PumpPortal WebSocket for instant token discovery
- ✅ [v4.5] WebSocket trade feed per token (real buy tracking)
- ✅ [v4.5] Auto-reconnect with exponential backoff
- ✅ [v4.8] Smart buy scoring: buy pressure + wash trade filter + fast velocity + vol accel
- ✅ [v4.7] KOL social polling (Nitter + RapidAPI fallback)
- ✅ [v4.7] /addkol /removekol /kols commands
- ✅ [v4.7] KOL ticker mention → alert + auto-discovery
- ✅ [v4.6] WS spam token deduplication (fingerprint cooldown)
- ✅ [v4.5] WS stats in /status + dashboard
- ✅ [v4.4] State persistence (survives restarts)
- ✅ [v4.4] Alert deduplication across restarts
- ✅ [v4.4] /gems /hot commands
- ✅ [v4.4] Alert rate limiter (max 1 per 30s)
- ✅ [v4.4] Volume acceleration detection
- ✅ [v4.4] DexScreener boost detection
- ✅ [v4.4] Web dashboard at /dashboard
- ✅ [v4.3] Token pool cap (300) + dead token pruning
- ✅ [v4.3] MC velocity tracking
- ✅ DexScreener enrichment (MC, vol, liquidity)
- ✅ Telegram alerts + commands
- ✅ NO Helius credits needed
"""

import sys
import os

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import asyncio
import json
import html
import time
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, List
from contextlib import asynccontextmanager

import aiohttp
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
import uvicorn

from telegram import Bot, BotCommand
from solders.pubkey import Pubkey as PublicKey

load_dotenv()

from config import get_config
from webhook_security import RateLimiter

# Optional runner detector
_RUNNER_DETECTOR_AVAILABLE = False
try:
    from runner_detector_v3_bridge import check_tokens_for_runners, format_runner_alert_v3
    _RUNNER_DETECTOR_AVAILABLE = True
except ImportError:
    pass

# ============================================================================
# Logging
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
config = get_config()

TELEGRAM_BOT_TOKEN = config.telegram.bot_token
TELEGRAM_CHAT_ID = config.telegram.chat_id
TELEGRAM_ENABLED = TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
HELIUS_API_KEY = config.api.helius_api_key
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex"
DEXSCREENER_NEW_PAIRS = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_SEARCH = "https://api.dexscreener.com/latest/dex/search"
DEXSCREENER_BOOSTS = "https://api.dexscreener.com/token-boosts/latest/v1"
# [v4.11] Chain-specific new pairs endpoints — sorted by pairCreatedAt desc
# These return recently created trading pairs per chain, far better than generic search
# DexScreener search queries per chain — returns pairs sorted by recent activity
# /latest/dex/search?q={term} is the correct free-tier endpoint
# We search generic terms to get recently active pairs, then filter by chain client-side
DEXSCREENER_NEW_PAIRS_BY_CHAIN = {
    "ethereum": "https://api.dexscreener.com/latest/dex/search?q=eth",
    "bsc":      "https://api.dexscreener.com/latest/dex/search?q=bsc",
    "base":     "https://api.dexscreener.com/latest/dex/search?q=base",
}
# How often to run the dedicated per-chain poll (seconds)
CHAIN_POLL_INTERVAL = {
    "ethereum": 30,   # ETH is slower — new pairs every 30s is fine
    "bsc":      20,   # BSC is faster
    "base":     20,   # Base is fast
}

# [v4.9] Multi-chain support
# Each chain: enabled, has_ws (PumpPortal only for Solana), dexscreener chain id, explorer base url, skip_symbols
CHAINS: Dict[str, dict] = {
    "solana": {
        "enabled": True,
        "has_ws": True,
        "chain_id": "solana",
        "explorer": "https://dexscreener.com/solana/{}",
        "label": "SOL",
        "emoji": "◎",
    },
    "bsc": {
        "enabled": os.getenv("ENABLE_BSC", "true").lower() == "true",
        "has_ws": False,
        "chain_id": "bsc",
        "explorer": "https://dexscreener.com/bsc/{}",
        "label": "BSC",
        "emoji": "🟡",
    },
    "base": {
        "enabled": os.getenv("ENABLE_BASE", "true").lower() == "true",
        "has_ws": False,
        "chain_id": "base",
        "explorer": "https://dexscreener.com/base/{}",
        "label": "BASE",
        "emoji": "🔵",
    },
    "ethereum": {
        "enabled": os.getenv("ENABLE_ETH", "true").lower() == "true",
        "has_ws": False,
        "chain_id": "ethereum",
        "explorer": "https://dexscreener.com/ethereum/{}",
        "label": "ETH",
        "emoji": "🔷",
    },
}

# [v4.10] Per-chain detection thresholds
# Each chain has different token lifecycle, MC ranges, and trading behaviour
CHAIN_THRESHOLDS = {
    "solana": {
        "age_min":      3 * 60,      # 3 min — catch runners at launch
        "age_max":      6 * 3600,    # 6 hours — runners can sustain
        "mc_min":       10_000,      # catch early before MC pumps
        "mc_max":       2_000_000,   # don't miss mid-cap runners
        "vol_mc_ratio": 0.15,        # lowered — early tokens have less vol
        "liq_min":      5_000,       # young tokens have less liq
        "buy_ratio_min": 0.52,
        "min_buys_h1":  10,
    },
    "bsc": {
        "age_min":      2 * 60,      # 2 min — BSC launches very fast
        "age_max":      12 * 3600,   # 12 hours — BSC runners sustain longer
        "mc_min":       8_000,
        "mc_max":       5_000_000,
        "vol_mc_ratio": 0.08,
        "liq_min":      5_000,
        "buy_ratio_min": 0.52,
        "min_buys_h1":  8,
    },
    "base": {
        "age_min":      3 * 60,
        "age_max":      6 * 3600,
        "mc_min":       10_000,
        "mc_max":       3_000_000,
        "vol_mc_ratio": 0.10,
        "liq_min":      5_000,
        "buy_ratio_min": 0.52,
        "min_buys_h1":  8,
    },
    "ethereum": {
        "age_min":      2 * 60,      # 2 min — catch ETH runners at the start
        "age_max":      24 * 3600,   # 24 hours — ETH runners sustain much longer
        "mc_min":       15_000,
        "mc_max":       500_000,     # widened — don't miss ETH mid-caps
        "vol_mc_ratio": 0.05,        # ETH trades slowly — 5% is meaningful signal
        "liq_min":      5_000,
        "buy_ratio_min": 0.52,
        "min_buys_h1":  3,           # ETH has fewer txns on small caps
    },
}

def get_thresholds(chain_id: str) -> dict:
    """Return detection thresholds for a chain, falling back to Solana defaults."""
    return CHAIN_THRESHOLDS.get(chain_id, CHAIN_THRESHOLDS["solana"])

ENABLED_CHAIN_IDS = [c["chain_id"] for c in CHAINS.values() if c["enabled"]]

def get_chain(chain_id: str) -> dict:
    """Return chain config dict for a chain_id, defaulting to solana."""
    for c in CHAINS.values():
        if c["chain_id"] == chain_id:
            return c
    return CHAINS["solana"]

def dex_url(mint: str, chain_id: str = "solana") -> str:
    return get_chain(chain_id)["explorer"].format(mint)

# [v4.5] PumpPortal WebSocket
PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"
WS_RECONNECT_DELAY_MIN = 2    # seconds before first reconnect
WS_RECONNECT_DELAY_MAX = 60   # cap backoff at 60s
WS_MAX_TRADE_SUBS = 50        # max tokens to subscribe trade feed for
WS_ENRICH_DELAY = 3           # seconds to wait after WS discovery before DexScreener fetch
WS_SOL_PRICE_USD = float(os.getenv("SOL_PRICE_USD", "175.0"))  # used to convert WS SOL vol → USD

POLL_INTERVAL = 15            # [v4.5] Reduced — WS handles discovery now
TOKENS_PER_POLL = 30

# Gem detection thresholds
GEM_AGE_MIN = 3 * 60           # 3 min — catch tokens as early as possible
GEM_AGE_MAX = 6 * 3600         # 6 hours — runners can sustain longer than 90 min
GEM_MC_MIN = 10_000            # lower floor — catch early before MC pumps
GEM_MC_MAX = 2_000_000         # raised ceiling — don't miss mid-cap runners
GEM_VOL_MC_RATIO = 0.15        # lowered — early tokens have lower vol/MC
GEM_LIQUIDITY_MIN = 5_000      # lowered — young tokens have less liq
GEM_COOLDOWN = 300             # 5 min cooldown (was 10) — faster re-alert on runners

# [v4.8] Buy pressure thresholds
GEM_BUY_RATIO_MIN = 0.52       # lowered from 0.60 — less strict buy dominance required
GEM_MIN_BUYS_H1 = 10           # lowered from 30 — catch early momentum
GEM_WS_BUY_PRESSURE = 1.2     # lowered — alert sooner on buy pressure
GEM_FAST_VELOCITY = 15.0       # lowered from 50% — easier to trigger fast-velocity path
GEM_FAST_MC_MIN = 8_000        # lower MC floor for fast-velocity alert path
GEM_VOL_ACCEL_MC_MAX = 500_000 # raised ceiling for vol-acceleration trigger

MULTIPLIER_MILESTONES = [2.0, 3.0, 5.0, 10.0]

# Token pool management
MAX_TRACKED_TOKENS = 750
TOKEN_MAX_AGE_SECONDS = 6 * 3600
TOKEN_STALE_AGE_SECONDS = 90 * 60
TOKEN_STALE_VOL_THRESHOLD = 5_000
TOKEN_PRUNE_INTERVAL = 300

# v4.4 constants
STATE_FILE = "astaroth_state.json"
ALERT_RATE_LIMIT = 30
SNAPSHOT_INTERVAL = 15  # [fix] save more frequently — Railway redeploys are fast
BOOST_POLL_INTERVAL = 120
VOL_ACCEL_THRESHOLD = 1.5

# [v4.6] WS spam deduplication
WS_SYMBOL_COOLDOWN = 30        # seconds — ignore same symbol+buy fingerprint within this window
WS_COOLDOWN_CLEANUP = 300      # clean up expired cooldown entries every 5 min

# [v4.7] KOL social polling
KOL_FILE = "kol_list.json"            # persisted KOL account list
KOL_POLL_INTERVAL = 120               # poll each KOL every 2 minutes
KOL_POST_LOOKBACK = 300               # only consider posts from last 5 minutes
KOL_ALERT_COOLDOWN = 600              # 10 min cooldown per KOL+ticker pair
KOL_MAX_ACCOUNTS = 50                 # hard cap on KOL list size
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")  # Twttr API key from rapidapi.com (twitter241.p.rapidapi.com)
NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.1d4.us",
]
# Regex: match $TICKER (2-10 uppercase letters/digits) in post text
TICKER_RE = __import__('re').compile(r'\$([A-Za-z][A-Za-z0-9]{1,9})')

# ============================================================================
# Global State
# ============================================================================
_last_prune_time: float = 0.0
_last_snapshot_time: float = 0.0
_last_boost_poll: float = 0.0
_last_cooldown_cleanup: float = 0.0
_alert_queue: asyncio.Queue = None
boosted_mints: Set[str] = set()
# [v4.11] Per-chain last poll timestamps for staggered chain polling
_last_chain_poll: dict = {"ethereum": 0.0, "bsc": 0.0, "base": 0.0}

# [v4.6] WS spam dedup: symbol -> list of (timestamp, mint) tuples seen within cooldown window
# Used to detect squatter swarms (many different mints sharing the same ticker in a short burst).
ws_symbol_cooldown: Dict[str, list] = {}
ws_sub_request_queue: asyncio.Queue = None  # [fix] side-channel for trade sub requests

# How many distinct mints for the same symbol in WS_SYMBOL_COOLDOWN seconds = squatter swarm
WS_SQUATTER_THRESHOLD = 4

# [v4.7] KOL state
kol_accounts: Dict[str, dict] = {}    # handle -> {added_at, last_polled, post_ids_seen}
kol_alert_cooldown: Dict[str, float] = {}  # "handle:TICKER" -> last alert timestamp

# [v4.5] WebSocket stats
ws_stats = {
    "connected": False,
    "reconnects": 0,
    "tokens_discovered": 0,
    "trades_received": 0,
    "last_message_at": 0.0,
    "trade_subs": 0,
}

# [v4.5] Queue for WS-discovered tokens needing DexScreener enrichment
ws_discovery_queue: asyncio.Queue = None

# [v4.12] Ring buffer for /analysis endpoint — last 500 scored tokens with outcome metadata
ANALYSIS_RING_SIZE = 500
_analysis_ring: deque = deque(maxlen=ANALYSIS_RING_SIZE)

# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class BuyEvent:
    timestamp: float
    sol_amount: float
    buyer: str

@dataclass
class TokenInfo:
    mint: str
    symbol: str
    name: str
    created_at: float
    launched_at: float = 0.0
    launchpad: str = "Pump.fun"
    market_cap: float = 0.0
    volume_usd: float = 0.0
    liquidity: float = 0.0
    holders: int = 0
    dev_pct: float = 0.0
    top10_pct: float = 0.0
    buy_ratio: float = 0.5
    buys_h1: int = 0
    sells_h1: int = 0
    buy_events: List[BuyEvent] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)
    last_alerted: float = 0.0
    alert_mc: float = 0.0
    alerted: bool = False
    last_mc: float = 0.0
    mc_velocity: float = 0.0
    vol_history: List[float] = field(default_factory=list)
    is_boosted: bool = False
    # [v4.5] WS-specific fields
    ws_discovered: bool = False       # was this token found via WS (vs polling)?
    ws_initial_buy_sol: float = 0.0   # SOL in first buy (from WS create event)
    ws_pre_enrichment: bool = False   # True = stub added before DexScreener fetch completes
    ws_buy_count: int = 0             # live buy count from WS trade feed
    ws_sell_count: int = 0            # live sell count from WS trade feed
    ws_sol_volume: float = 0.0        # live SOL volume from WS trade feed
    ws_buy_vol_usd: float = 0.0       # live buy-side USD volume from WS trade feed
    ws_sell_vol_usd: float = 0.0      # live sell-side USD volume from WS trade feed
    # [v4.9] multi-chain
    chain_id: str = "solana"          # which chain this token is on
    # [v4.11] ETH buy volume tracking
    buy_volume_h1: float = 0.0        # USD value of buy-side volume in last 1h
    volume_m5: float = 0.0            # 5-min volume (whale spike detection)
    vol_usd_history: List[float] = field(default_factory=list)  # buy vol history for accel
    # [v4.11] Anti-dump / sustained signal fields
    price_change_h1: float = 0.0      # 1h price change % from DexScreener
    consecutive_green_polls: int = 0  # polls in a row where MC rose
    alert_suppressed_until: float = 0.0  # timestamp — don't alert before this
    first_seen_mc: float = 0.0        # MC when first tracked (for dump detection)
    # Reply threading — stores the Telegram message_id of the initial alert
    alert_message_id: int = 0         # used to reply-thread P&L / milestone updates

tokens: Dict[str, TokenInfo] = {}
tokens_lock = asyncio.Lock()
seen_mints: Set[str] = set()
telegram_bot: Optional[Bot] = None
volume_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=6))

# ============================================================================
# State Persistence (v4.4)
# ============================================================================

def _token_to_dict(token: TokenInfo) -> dict:
    return {
        "mint": token.mint,
        "symbol": token.symbol,
        "name": token.name,
        "created_at": token.created_at,
        "launched_at": token.launched_at,
        "launchpad": token.launchpad,
        "market_cap": token.market_cap,
        "volume_usd": token.volume_usd,
        "liquidity": token.liquidity,
        "holders": token.holders,
        "buy_ratio": token.buy_ratio,
        "buys_h1": token.buys_h1,
        "sells_h1": token.sells_h1,
        "last_updated": token.last_updated,
        "last_alerted": token.last_alerted,
        "alert_mc": token.alert_mc,
        "alerted": token.alerted,
        "last_mc": token.last_mc,
        "mc_velocity": token.mc_velocity,
        "vol_history": token.vol_history[-6:],
        "is_boosted": token.is_boosted,
        "ws_discovered": token.ws_discovered,
        "ws_initial_buy_sol": token.ws_initial_buy_sol,
        "ws_buy_count": token.ws_buy_count,
        "ws_sell_count": token.ws_sell_count,
        "ws_sol_volume": token.ws_sol_volume,
        "ws_buy_vol_usd": token.ws_buy_vol_usd,
        "ws_sell_vol_usd": token.ws_sell_vol_usd,
        "_sent_milestones": list(getattr(token, '_sent_milestones', set())),
        "chain_id": token.chain_id,
        "buy_volume_h1": token.buy_volume_h1,
        "volume_m5": token.volume_m5,
        "price_change_h1": token.price_change_h1,
        "consecutive_green_polls": token.consecutive_green_polls,
        "first_seen_mc": token.first_seen_mc,
    }


def _token_from_dict(d: dict) -> TokenInfo:
    t = TokenInfo(
        mint=d["mint"],
        symbol=d["symbol"],
        name=d["name"],
        created_at=d["created_at"],
        launched_at=d.get("launched_at", 0.0),
        launchpad=d.get("launchpad", "Pump.fun"),
        market_cap=d.get("market_cap", 0.0),
        volume_usd=d.get("volume_usd", 0.0),
        liquidity=d.get("liquidity", 0.0),
        holders=d.get("holders", 0),
        buy_ratio=d.get("buy_ratio", 0.5),
        buys_h1=d.get("buys_h1", 0),
        sells_h1=d.get("sells_h1", 0),
        last_updated=d.get("last_updated", time.time()),
        last_alerted=d.get("last_alerted", 0.0),
        alert_mc=d.get("alert_mc", 0.0),
        alerted=d.get("alerted", False),
        last_mc=d.get("last_mc", 0.0),
        mc_velocity=d.get("mc_velocity", 0.0),
        vol_history=d.get("vol_history", []),
        is_boosted=d.get("is_boosted", False),
        ws_discovered=d.get("ws_discovered", False),
        ws_initial_buy_sol=d.get("ws_initial_buy_sol", 0.0),
        ws_buy_count=d.get("ws_buy_count", 0),
        ws_sell_count=d.get("ws_sell_count", 0),
        ws_sol_volume=d.get("ws_sol_volume", 0.0),
        ws_buy_vol_usd=d.get("ws_buy_vol_usd", 0.0),
        ws_sell_vol_usd=d.get("ws_sell_vol_usd", 0.0),
        chain_id=d.get("chain_id", "solana"),
        buy_volume_h1=d.get("buy_volume_h1", 0.0),
        volume_m5=d.get("volume_m5", 0.0),
        price_change_h1=d.get("price_change_h1", 0.0),
        consecutive_green_polls=d.get("consecutive_green_polls", 0),
        first_seen_mc=d.get("first_seen_mc", 0.0),
    )
    t._sent_milestones = set(d.get("_sent_milestones", []))
    return t


async def save_state():
    global _last_snapshot_time
    now = time.time()
    if now - _last_snapshot_time < SNAPSHOT_INTERVAL:
        return
    _last_snapshot_time = now
    try:
        async with tokens_lock:
            snapshot = {
                "saved_at": now,
                "version": "4.11",
                "tokens": {mint: _token_to_dict(t) for mint, t in tokens.items()},
                "seen_mints": list(seen_mints),
            }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(snapshot, f)
        logger.debug(f"💾 State saved ({len(snapshot['tokens'])} tokens)")
    except Exception as e:
        logger.error(f"State save error: {e}")


def load_state():
    global seen_mints
    if not os.path.exists(STATE_FILE):
        logger.info("📂 No saved state — starting fresh")
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            snapshot = json.load(f)
        saved_at = snapshot.get("saved_at", 0)
        age_of_save = time.time() - saved_at
        now = time.time()
        loaded = skipped = 0
        for mint, d in snapshot.get("tokens", {}).items():
            if now - d.get("created_at", 0) > TOKEN_MAX_AGE_SECONDS:
                skipped += 1
                continue
            tokens[mint] = _token_from_dict(d)
            loaded += 1
        seen_mints = set(snapshot.get("seen_mints", []))
        logger.info(f"📂 Restored {loaded} tokens, {skipped} expired (save was {int(age_of_save)}s ago)")
    except Exception as e:
        logger.error(f"State load error: {e} — starting fresh")

# ============================================================================
# Token Pool Management (v4.3)
# ============================================================================

def update_mc_velocity(token: TokenInfo, new_mc: float):
    if token.last_mc > 0 and new_mc > 0:
        token.mc_velocity = ((new_mc - token.last_mc) / token.last_mc) * 100
    token.last_mc = new_mc


def is_high_velocity(token: TokenInfo) -> bool:
    return token.mc_velocity > 20.0


def update_vol_history(token: TokenInfo, new_vol: float):
    token.vol_history.append(new_vol)
    if len(token.vol_history) > 6:
        token.vol_history = token.vol_history[-6:]


def is_vol_accelerating(token: TokenInfo) -> bool:
    hist = token.vol_history
    if len(hist) < 3:
        return False
    recent = hist[-1]
    prev_avg = sum(hist[:-1]) / len(hist[:-1])
    if prev_avg <= 0:
        return False
    return recent >= prev_avg * VOL_ACCEL_THRESHOLD


async def prune_dead_tokens():
    global _last_prune_time, _last_cooldown_cleanup
    now = time.time()
    if now - _last_prune_time < TOKEN_PRUNE_INTERVAL:
        return
    _last_prune_time = now

    # [v4.6] Clean up expired WS spam cooldown entries
    if now - _last_cooldown_cleanup > WS_COOLDOWN_CLEANUP:
        _last_cooldown_cleanup = now
        cutoff = now - WS_SYMBOL_COOLDOWN * 2
        expired_syms = []
        for sym, entries in ws_symbol_cooldown.items():
            # entries is a list of (timestamp, mint) tuples
            ws_symbol_cooldown[sym] = [(ts, m) for ts, m in entries if ts > cutoff]
            if not ws_symbol_cooldown[sym]:
                expired_syms.append(sym)
        for sym in expired_syms:
            del ws_symbol_cooldown[sym]
        if expired_syms:
            logger.debug(f"🧹 Cleared {len(expired_syms)} WS cooldown symbol entries")

    to_remove = []
    async with tokens_lock:
        for mint, token in tokens.items():
            age = now - token.created_at
            if age > TOKEN_MAX_AGE_SECONDS:
                to_remove.append((mint, token.symbol, "6h"))
                continue
            if age > TOKEN_STALE_AGE_SECONDS and not token.alerted:
                if token.volume_usd < TOKEN_STALE_VOL_THRESHOLD:
                    to_remove.append((mint, token.symbol, "stale"))
        for mint, sym, reason in to_remove:
            del tokens[mint]
            volume_history.pop(mint, None)
            seen_mints.discard(mint)  # [fix] allow re-discovery after pruning
    if to_remove:
        sample = ", ".join(f"${s}" for _, s, _ in to_remove[:5])
        suffix = "..." if len(to_remove) > 5 else ""
        logger.info(f"🗑️ Pruned {len(to_remove)} dead tokens ({sample}{suffix}) | Pool: {len(tokens)}")


def enforce_token_cap(symbol: str) -> bool:
    """Must be called while holding tokens_lock.

    Score-aware eviction: never evict tokens with consecutive_green_polls >= 2
    (i.e. tokens showing sustained momentum). Among evictable candidates, prefer
    to drop the lowest-scored oldest token rather than simply the oldest.
    """
    if len(tokens) < MAX_TRACKED_TOKENS:
        return True
    # Never evict: alerted tokens or tokens with sustained green momentum
    candidates = [
        (mint, t) for mint, t in tokens.items()
        if not t.alerted and getattr(t, "consecutive_green_polls", 0) < 2
    ]
    if not candidates:
        # All tracked tokens are either alerted or on a hot streak — drop
        # the absolute oldest non-alerted token as a last resort
        fallback = [(mint, t) for mint, t in tokens.items() if not t.alerted]
        if not fallback:
            return False
        oldest_mint, oldest = min(fallback, key=lambda x: x[1].created_at)
        del tokens[oldest_mint]
        volume_history.pop(oldest_mint, None)
        logger.debug(f"♻️ Evicted (fallback) ${oldest.symbol} → ${symbol}")
        return True
    # Sort: primary key = composite_score asc (evict weakest first),
    #        secondary = created_at asc (evict oldest among equally weak)
    candidates.sort(key=lambda x: (getattr(x[1], "composite_score", 0.0), x[1].created_at))
    evict_mint, evict_token = candidates[0]
    del tokens[evict_mint]
    volume_history.pop(evict_mint, None)
    logger.debug(
        f"♻️ Evicted ${evict_token.symbol} "
        f"(score={getattr(evict_token, 'composite_score', 0.0):.2f}) → ${symbol}"
    )
    return True

# ============================================================================
# Alert Rate Limiter (v4.4)
# ============================================================================

async def alert_worker():
    while True:
        try:
            text, parse_mode = await _alert_queue.get()
            await _send_telegram_direct(text, parse_mode)
            _alert_queue.task_done()
            await asyncio.sleep(ALERT_RATE_LIMIT)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Alert worker error: {e}")
            await asyncio.sleep(5)


async def _send_telegram_direct(text: str, parse_mode: str = "HTML", reply_to_message_id: int = 0) -> int:
    """Send a Telegram message. Returns message_id (0 on failure)."""
    if not TELEGRAM_ENABLED or not telegram_bot:
        return 0
    try:
        kwargs = dict(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=parse_mode)
        if reply_to_message_id:
            kwargs["reply_to_message_id"] = reply_to_message_id
        msg = await telegram_bot.send_message(**kwargs)
        return msg.message_id if msg else 0
    except Exception as e:
        logger.error(f"Telegram send error: {e}")
        return 0

# ============================================================================
# Telegram
# ============================================================================

async def init_telegram():
    global telegram_bot
    if not TELEGRAM_ENABLED:
        logger.warning("⚠️  Telegram disabled")
        return
    try:
        telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        me = await telegram_bot.get_me()
        logger.info(f"✅ Telegram bot: {me.username}")
        commands = [
            BotCommand("status", "Bot status"),
            BotCommand("tokens", "All tracked tokens"),
            BotCommand("gems", "Alerted gems sorted by MC"),
            BotCommand("hot", "High velocity tokens"),
            BotCommand("kols", "KOL account list"),
            BotCommand("addkol", "Add KOL to monitor"),
            BotCommand("removekol", "Remove a KOL"),
            BotCommand("help", "Help"),
        ]
        await telegram_bot.set_my_commands(commands)
    except Exception as e:
        logger.error(f"❌ Telegram init: {e}")


async def send_telegram(text: str, parse_mode: str = "HTML") -> int:
    """Queue alert (rate-limited). Returns message_id when sent directly, 0 when queued."""
    if not TELEGRAM_ENABLED or not telegram_bot:
        return 0
    if _alert_queue is not None:
        await _alert_queue.put((text, parse_mode))
        return 0  # message_id unavailable when queued
    return await _send_telegram_direct(text, parse_mode)


async def send_telegram_now(text: str, parse_mode: str = "HTML") -> int:
    """Bypass rate limiter — for commands only. Returns message_id."""
    return await _send_telegram_direct(text, parse_mode)


async def send_telegram_reply(text: str, reply_to_message_id: int, parse_mode: str = "HTML") -> int:
    """Send a message as a threaded reply to a prior alert. Returns message_id."""
    if not TELEGRAM_ENABLED or not telegram_bot or not reply_to_message_id:
        return await _send_telegram_direct(text, parse_mode)
    return await _send_telegram_direct(text, parse_mode, reply_to_message_id=reply_to_message_id)


async def handle_telegram_command(text: str):
    cmd = text.split()[0].lower().replace("/", "").split("@")[0]

    if cmd == "status":
        hot_count = sum(1 for t in tokens.values() if is_high_velocity(t))
        accel_count = sum(1 for t in tokens.values() if is_vol_accelerating(t))
        gem_count = sum(1 for t in tokens.values() if t.alerted)
        ws_disc = sum(1 for t in tokens.values() if t.ws_discovered)
        ws_age = int(time.time() - ws_stats["last_message_at"]) if ws_stats["last_message_at"] else -1
        await send_telegram_now(
            f"✅ <b>ASTAROTH v4.11 Status</b>\n\n"
            f"📊 Tracking: {len(tokens)} / {MAX_TRACKED_TOKENS}\n"
            f"💎 Gems alerted: {gem_count}\n"
            f"🔥 High velocity: {hot_count}\n"
            f"📈 Vol accelerating: {accel_count}\n"
            f"🚀 Boosted: {sum(1 for t in tokens.values() if t.is_boosted)}\n\n"
            f"🔌 <b>WebSocket</b>\n"
            f"  Status: {'🟢 Connected' if ws_stats['connected'] else '🔴 Disconnected'}\n"
            f"  Tokens via WS: {ws_disc}\n"
            f"  Tokens via poll: {len(tokens) - ws_disc}\n"
            f"  Total discovered: {ws_stats['tokens_discovered']}\n"
            f"  Trades received: {ws_stats['trades_received']}\n"
            f"  Trade subs: {ws_stats['trade_subs']}\n"
            f"  Last msg: {ws_age}s ago\n"
            f"  Reconnects: {ws_stats['reconnects']}\n\n"
            f"📬 Alert queue: {_alert_queue.qsize() if _alert_queue else 0}\n"
            f"🏃 Runner detection: {'✅' if _RUNNER_DETECTOR_AVAILABLE else '❌'}"
        )

    elif cmd == "gems":
        alerted = [(mint, t) for mint, t in tokens.items() if t.alerted]
        if not alerted:
            await send_telegram_now("💎 No gems alerted yet.")
            return
        alerted.sort(key=lambda x: x[1].market_cap, reverse=True)
        lines = []
        for mint, t in alerted[:20]:
            mult = f" x{t.market_cap/t.alert_mc:.1f}" if t.alert_mc > 0 else ""
            tags = []
            if is_high_velocity(t): tags.append("🔥")
            if t.is_boosted: tags.append("🚀")
            if t.ws_discovered: tags.append("⚡WS")
            tag_str = " ".join(tags)
            lines.append(
                f"<b>${t.symbol}</b>{mult} {tag_str} | MC: ${t.market_cap:,.0f}\n"
                f"<code>{mint}</code>"
            )
        await send_telegram_now(
            f"💎 <b>Alerted Gems ({len(alerted)}) — by MC</b>\n\n" +
            "\n\n".join(lines)
        )

    elif cmd == "hot":
        hot = [(m, t) for m, t in tokens.items() if is_high_velocity(t) or is_vol_accelerating(t)]
        if not hot:
            await send_telegram_now("🔥 No hot tokens right now.")
            return
        hot.sort(key=lambda x: x[1].mc_velocity, reverse=True)
        lines = []
        for mint, t in hot[:10]:
            tags = []
            if is_high_velocity(t): tags.append(f"MC+{t.mc_velocity:.1f}%")
            if is_vol_accelerating(t): tags.append("Vol↑")
            if t.is_boosted: tags.append("Boosted")
            if t.ws_discovered: tags.append("⚡WS")
            lines.append(
                f"<b>${t.symbol}</b> [{', '.join(tags)}] | MC: ${t.market_cap:,.0f}\n"
                f"<code>{mint}</code>"
            )
        await send_telegram_now(
            f"🔥 <b>Hot Tokens ({len(hot)})</b>\n\n" + "\n\n".join(lines)
        )

    elif cmd == "tokens":
        if not tokens:
            await send_telegram_now("📭 No tokens tracked yet.")
            return
        token_list = sorted(tokens.items(), key=lambda x: x[1].market_cap, reverse=True)
        total = len(token_list)
        chunks = [token_list[i:i+15] for i in range(0, total, 15)]
        for idx, chunk in enumerate(chunks[:3]):
            header = f"📊 <b>Tracked Tokens ({total}) — Page {idx+1}/{min(len(chunks),3)}</b>\n\n"
            lines = []
            for mint, t in chunk:
                vel_str = f" 🔥{t.mc_velocity:+.1f}%" if is_high_velocity(t) else ""
                gem_str = " 💎" if t.alerted else ""
                ws_str = " ⚡" if t.ws_discovered else ""
                lines.append(
                    f"<b>${t.symbol}</b>{gem_str}{ws_str}{vel_str} | MC: ${t.market_cap:,.0f}\n"
                    f"<code>{mint}</code>"
                )
            await send_telegram_now(header + "\n\n".join(lines))

    elif cmd == "addkol":
        parts = text.strip().split()
        if len(parts) < 2:
            await send_telegram_now("Usage: /addkol @handle")
            return
        handle = parts[1].lstrip("@").lower()
        if handle in kol_accounts:
            await send_telegram_now(f"📋 @{handle} is already in your KOL list.")
        elif len(kol_accounts) >= KOL_MAX_ACCOUNTS:
            await send_telegram_now(f"⚠️ KOL list is full ({KOL_MAX_ACCOUNTS} max). Remove one first.")
        else:
            kol_accounts[handle] = {
                "added_at": time.time(),
                "last_polled": 0,
                "post_ids_seen": [],
            }
            save_kols()
            await send_telegram_now(
                f"✅ Added @{handle} to KOL list\n"
                f"📋 Total KOLs: {len(kol_accounts)}"
            )
            logger.info(f"📋 KOL added: @{handle}")

    elif cmd == "removekol":
        parts = text.strip().split()
        if len(parts) < 2:
            await send_telegram_now("Usage: /removekol @handle")
            return
        handle = parts[1].lstrip("@").lower()
        if handle not in kol_accounts:
            await send_telegram_now(f"📋 @{handle} is not in your KOL list.")
        else:
            del kol_accounts[handle]
            save_kols()
            await send_telegram_now(
                f"🗑️ Removed @{handle} from KOL list\n"
                f"📋 Total KOLs: {len(kol_accounts)}"
            )
            logger.info(f"📋 KOL removed: @{handle}")

    elif cmd == "kols":
        if not kol_accounts:
            await send_telegram_now(
                "📋 <b>KOL List</b>\n\n"
                "No KOLs added yet.\n"
                "Use /addkol @handle to add one."
            )
        else:
            now = time.time()
            lines = []
            for handle, data in sorted(kol_accounts.items()):
                last = data.get("last_polled", 0)
                age = int(now - last) if last else -1
                age_str = f"{age}s ago" if age >= 0 else "never"
                lines.append(f"• @{handle} — last polled: {age_str}")
            await send_telegram_now(
                f"📋 <b>KOL List ({len(kol_accounts)})</b>\n\n" +
                "\n".join(lines) +
                "\n\n<i>Use /addkol @handle or /removekol @handle</i>"
            )

    elif cmd in ("help", "start"):
        await send_telegram_now(
            "🤖 <b>ASTAROTH v4.11 Commands</b>\n\n"
            "/status — bot health + WS stats\n"
            "/gems — alerted gems sorted by MC\n"
            "/hot — high velocity tokens\n"
            "/tokens — full tracked list\n"
            "/kols — KOL account list\n"
            "/addkol @handle — add a KOL to monitor\n"
            "/removekol @handle — remove a KOL\n"
            "/help — this message\n\n"
            "⚡ = discovered via WebSocket\n"
            "💎 = gem alert fired\n"
            "🔥 = high MC velocity\n"
            "👁️ = KOL mention alert"
        )
    else:
        logger.debug(f"Unknown command: {cmd}")


# ============================================================================
# [v4.7] KOL Social Polling
# ============================================================================

def load_kols():
    """Load KOL list from disk on startup."""
    global kol_accounts
    if not os.path.exists(KOL_FILE):
        logger.info("📋 No KOL list found — starting empty")
        return
    try:
        with open(KOL_FILE, "r") as f:
            kol_accounts = json.load(f)
        logger.info(f"📋 Loaded {len(kol_accounts)} KOL accounts")
    except Exception as e:
        logger.error(f"KOL load error: {e}")


def save_kols():
    """Persist KOL list to disk."""
    try:
        with open(KOL_FILE, "w") as f:
            json.dump(kol_accounts, f)
    except Exception as e:
        logger.error(f"KOL save error: {e}")


def extract_tickers(text: str) -> List[str]:
    """
    Extract $TICKER mentions from a post.
    Filters out common non-token symbols like $USD, $SOL (the native coin).
    """
    SKIP = {"USD", "SOL", "ETH", "BTC", "USDC", "USDT", "EUR", "GBP",
            "JPY", "CNY", "NFT", "DAO", "DeFi", "DEFI", "WEB3", "AI"}
    found = TICKER_RE.findall(text)
    return [t.upper() for t in found if t.upper() not in SKIP]


async def fetch_kol_posts_nitter(
    handle: str, session: aiohttp.ClientSession
) -> List[dict]:
    """
    Fetch recent posts for a KOL via Nitter RSS.
    Tries multiple Nitter instances in order.
    Returns list of {id, text, timestamp}.
    """
    import xml.etree.ElementTree as ET

    for instance in NITTER_INSTANCES:
        url = f"{instance}/{handle.lstrip('@')}/rss"
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=8),
                headers={"User-Agent": "Mozilla/5.0 ASTAROTH/1.0"},
            ) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.text()
                root = ET.fromstring(raw)
                channel = root.find("channel")
                if channel is None:
                    continue

                posts = []
                now = time.time()
                for item in channel.findall("item"):
                    title = item.findtext("title", "")
                    desc = item.findtext("description", "")
                    text = f"{title} {desc}"
                    link = item.findtext("link", "")
                    pub_date = item.findtext("pubDate", "")

                    # Parse timestamp
                    try:
                        import email.utils
                        ts = email.utils.parsedate_to_datetime(pub_date).timestamp()
                    except Exception:
                        ts = now

                    # Only posts from last KOL_POST_LOOKBACK seconds
                    if now - ts > KOL_POST_LOOKBACK:
                        continue

                    # Unique ID from link
                    post_id = link.split("/")[-1] if link else str(ts)
                    posts.append({"id": post_id, "text": text, "timestamp": ts})

                if posts:
                    logger.debug(f"📋 Nitter [{instance}] got {len(posts)} posts for @{handle}")
                    return posts

        except Exception as e:
            logger.debug(f"Nitter {instance} error for @{handle}: {e}")
            continue

    return []


# Cache: handle (lowercase) -> numeric Twitter user ID
_kol_user_id_cache: Dict[str, str] = {}


async def _resolve_twitter_user_id(
    handle: str, session: aiohttp.ClientSession
) -> str:
    """
    Resolve a Twitter handle to a numeric user ID.
    Uses Get User By Username endpoint on twitter241 API.
    Result is cached in-memory so we only burn 1 API call per handle ever.
    """
    handle_clean = handle.lstrip("@").lower()
    if handle_clean in _kol_user_id_cache:
        return _kol_user_id_cache[handle_clean]

    if not RAPIDAPI_KEY:
        return ""

    try:
        async with session.get(
            "https://twitter241.p.rapidapi.com/user",
            params={"username": handle_clean},
            headers={
                "x-rapidapi-host": "twitter241.p.rapidapi.com",
                "x-rapidapi-key": RAPIDAPI_KEY,
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.debug(f"Twttr user lookup HTTP {resp.status} for @{handle}")
                return ""
            data = await resp.json()
            # Response: result.data.user.result.rest_id  (or legacy.id_str)
            user_id = (
                data.get("result", {})
                    .get("data", {})
                    .get("user", {})
                    .get("result", {})
                    .get("rest_id", "")
            )
            if not user_id:
                # fallback path some versions use
                user_id = str(
                    data.get("data", {})
                        .get("user", {})
                        .get("result", {})
                        .get("rest_id", "")
                )
            if user_id and user_id != "None":
                _kol_user_id_cache[handle_clean] = user_id
                logger.info(f"📋 Resolved @{handle} → user_id {user_id}")
            return user_id if user_id != "None" else ""
    except Exception as e:
        logger.debug(f"Twttr user ID lookup error for @{handle}: {e}")
        return ""


async def fetch_kol_posts_rapidapi(
    handle: str, session: aiohttp.ClientSession
) -> List[dict]:
    """
    Fallback: fetch via Twttr API (twitter241.p.rapidapi.com).
    Endpoint: GET /user-tweets?user={user_id}&count=20
    Requires RAPIDAPI_KEY env var.
    """
    if not RAPIDAPI_KEY:
        return []

    # Step 1: resolve handle -> numeric user ID (cached after first call)
    user_id = await _resolve_twitter_user_id(handle, session)
    if not user_id:
        logger.debug(f"📋 Twttr API: no user ID for @{handle}, skipping")
        return []

    # Step 2: fetch tweets
    try:
        async with session.get(
            "https://twitter241.p.rapidapi.com/user-tweets",
            params={"user": user_id, "count": "20"},
            headers={
                "Content-Type": "application/json",
                "x-rapidapi-host": "twitter241.p.rapidapi.com",
                "x-rapidapi-key": RAPIDAPI_KEY,
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.debug(f"Twttr /user-tweets HTTP {resp.status} for @{handle}")
                return []
            data = await resp.json()
            now = time.time()
            posts = []

            # Response structure: result.timeline.instructions[].entries[]
            instructions = (
                data.get("result", {})
                    .get("timeline", {})
                    .get("instructions", [])
            )
            for instruction in instructions:
                for entry in instruction.get("entries", []):
                    legacy = (
                        entry.get("content", {})
                             .get("itemContent", {})
                             .get("tweet_results", {})
                             .get("result", {})
                             .get("legacy", {})
                    )
                    if not legacy:
                        continue
                    text = legacy.get("full_text", "") or legacy.get("text", "")
                    if not text or text.startswith("RT @"):
                        continue
                    tweet_id = legacy.get("id_str", "")
                    try:
                        import email.utils
                        ts = email.utils.parsedate_to_datetime(
                            legacy.get("created_at", "")
                        ).timestamp()
                    except Exception:
                        ts = now
                    if now - ts > KOL_POST_LOOKBACK:
                        continue
                    posts.append({
                        "id": tweet_id or str(ts),
                        "text": text,
                        "timestamp": ts,
                    })

            if posts:
                logger.debug(f"📋 Twttr API got {len(posts)} posts for @{handle}")
            return posts

    except Exception as e:
        logger.debug(f"Twttr API /user-tweets error for @{handle}: {e}")
        return []


async def process_kol_posts(
    handle: str,
    posts: List[dict],
    session: aiohttp.ClientSession,
):
    """
    For each new post:
    1. Extract $TICKER mentions
    2. If ticker matches a tracked token → fire KOL alert
    3. If ticker not tracked → add to WS discovery queue for enrichment
    """
    if not posts:
        return

    kol_data = kol_accounts.get(handle, {})
    seen_ids: set = set(kol_data.get("post_ids_seen", []))
    now = time.time()
    new_ids = set()
    alerted_tickers = set()

    for post in posts:
        post_id = post["id"]
        if post_id in seen_ids:
            continue
        new_ids.add(post_id)

        tickers = extract_tickers(post["text"])
        if not tickers:
            continue

        logger.info(f"📋 KOL @{handle} mentioned: {tickers} | '{post['text'][:80]}'")

        for ticker in tickers:
            if ticker in alerted_tickers:
                continue

            cooldown_key = f"{handle}:{ticker}"
            if cooldown_key in kol_alert_cooldown:
                if now - kol_alert_cooldown[cooldown_key] < KOL_ALERT_COOLDOWN:
                    continue

            # Find matching tracked token(s)
            matched_tokens = [
                (mint, t) for mint, t in tokens.items()
                if t.symbol.upper() == ticker
            ]

            if matched_tokens:
                # Alert for each matched token
                for mint, token in matched_tokens:
                    kol_alert_cooldown[cooldown_key] = now
                    alerted_tickers.add(ticker)
                    dex_url = f"https://dexscreener.com/solana/{mint}"
                    gem_tag = "💎 Already gem alerted" if token.alerted else "⏳ Not yet gem"
                    msg = (
                        f"👁️ <b>KOL MENTION: @{handle}</b>\n\n"
                        f"💬 <i>{post['text'][:200].strip()}</i>\n\n"
                        f"🎯 Tracked: <b>${token.symbol}</b>\n"
                        f"💰 MC: ${token.market_cap:,.0f}\n"
                        f"📊 Vol: ${token.volume_usd:,.0f}\n"
                        f"{gem_tag}\n\n"
                        f"🔗 <a href='{dex_url}'>DexScreener</a>"
                    )
                    await send_telegram(msg)
                    logger.info(f"👁️ KOL alert: @{handle} → ${ticker} (tracked)")
            else:
                # Ticker not tracked — add to discovery queue (Option B)
                kol_alert_cooldown[cooldown_key] = now
                alerted_tickers.add(ticker)
                logger.info(f"👁️ KOL @{handle} → ${ticker} (unknown — queuing for discovery)")

                # Send a heads-up alert
                msg = (
                    f"👁️ <b>KOL MENTION: @{handle}</b>\n\n"
                    f"💬 <i>{post['text'][:200].strip()}</i>\n\n"
                    f"🔍 <b>${ticker}</b> — not yet tracked\n"
                    "⚡ Queuing for DexScreener lookup..."
                )
                await send_telegram(msg)

                # Queue a DexScreener search for this ticker
                if ws_discovery_queue is not None:
                    await ws_discovery_queue.put({
                        "mint": None,  # unknown — enrich worker will search by symbol
                        "symbol": ticker,
                        "name": ticker,
                        "created_at": now,
                        "ws_discovered": False,
                        "ws_initial_buy_sol": 0.0,
                        "mc_sol_at_creation": 0.0,
                        "kol_source": handle,
                        "search_by_symbol": True,  # flag for enrich worker
                    })

    # Update seen post IDs (keep last 200 to avoid unbounded growth)
    all_ids = list(seen_ids | new_ids)[-200:]
    kol_accounts[handle]["post_ids_seen"] = all_ids
    kol_accounts[handle]["last_polled"] = now


async def kol_polling_loop(session: aiohttp.ClientSession):
    """
    Polls each KOL account every KOL_POLL_INTERVAL seconds.
    Staggers requests across accounts to avoid hammering Nitter.
    """
    logger.info(f"📋 KOL polling loop started ({len(kol_accounts)} accounts)")

    while True:
        try:
            now = time.time()
            handles = list(kol_accounts.keys())

            for i, handle in enumerate(handles):
                kol_data = kol_accounts.get(handle, {})
                last_polled = kol_data.get("last_polled", 0)

                if now - last_polled < KOL_POLL_INTERVAL:
                    continue

                # Try Nitter first, fall back to RapidAPI
                posts = await fetch_kol_posts_nitter(handle, session)
                if not posts:
                    posts = await fetch_kol_posts_rapidapi(handle, session)

                await process_kol_posts(handle, posts, session)
                save_kols()

                # Stagger: 2s between accounts to be polite to Nitter
                if i < len(handles) - 1:
                    await asyncio.sleep(2)

            await asyncio.sleep(10)  # check again in 10s

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"KOL polling error: {e}")
            await asyncio.sleep(15)

# ============================================================================
# [v4.5] PumpPortal WebSocket Listener
# ============================================================================

async def _handle_ws_create(data: dict):
    """
    Handle a new token creation event from PumpPortal WS.
    Data fields: mint, symbol, name, traderPublicKey, initialBuy,
                 marketCapSol, vSolInBondingCurve, bondingCurveKey
    """
    mint = data.get("mint")
    if not mint:
        return

    symbol = data.get("symbol") or data.get("name", "???")[:10]
    name = data.get("name", symbol)
    initial_buy = float(data.get("initialBuy", 0) or 0)
    mc_sol = float(data.get("marketCapSol", 0) or 0)

    # Primary dedup: mint address is the canonical unique key
    if mint in seen_mints:
        return

    # [v4.6] Squatter detection: many distinct mints for the same ticker in a short
    # window = coordinated symbol-squatting attack. Track (timestamp, mint) pairs per symbol.
    sym_key = symbol.upper().strip()
    now = time.time()
    cutoff = now - WS_SYMBOL_COOLDOWN
    existing = ws_symbol_cooldown.get(sym_key, [])
    # Prune stale entries
    existing = [(ts, m) for ts, m in existing if ts > cutoff]
    existing.append((now, mint))
    ws_symbol_cooldown[sym_key] = existing
    if len(existing) > WS_SQUATTER_THRESHOLD:
        logger.debug(
            f"🚫 WS squatter swarm: ${symbol} — {len(existing)} mints in {WS_SYMBOL_COOLDOWN}s"
        )
        return

    seen_mints.add(mint)
    ws_stats["tokens_discovered"] += 1
    logger.info(f"⚡ WS NEW: ${symbol} | MC: {mc_sol:.1f} SOL | Init buy: {initial_buy:.2f} SOL")

    # [v4.12] Pre-track: immediately add a stub token so trade events can accumulate
    # before DexScreener enrichment completes. Stub MC is estimated from WS data.
    created_ts = time.time()
    estimated_mc = mc_sol * WS_SOL_PRICE_USD
    async with tokens_lock:
        if mint not in tokens:
            if enforce_token_cap(symbol):
                tokens[mint] = TokenInfo(
                    mint=mint,
                    symbol=symbol,
                    name=name,
                    created_at=created_ts,
                    market_cap=estimated_mc,
                    ws_discovered=True,
                    ws_initial_buy_sol=initial_buy,
                    ws_pre_enrichment=True,
                    chain_id="solana",
                )
                logger.debug(f"🔬 Pre-tracked ${symbol} (stub MC≈${estimated_mc:,.0f})")

    # Queue for DexScreener enrichment (don't fetch in WS handler — keep it fast)
    if ws_discovery_queue is not None:
        await ws_discovery_queue.put({
            "mint": mint,
            "symbol": symbol,
            "name": name,
            "created_at": created_ts,
            "ws_discovered": True,
            "ws_initial_buy_sol": initial_buy,
            "mc_sol_at_creation": mc_sol,
        })


async def _handle_ws_trade(data: dict):
    """
    Handle a live trade event from PumpPortal WS.
    Data fields: mint, txType (buy/sell), tokenAmount, solAmount, traderPublicKey
    """
    mint = data.get("mint")
    if not mint or mint not in tokens:
        return

    tx_type = data.get("txType", "")
    sol_amount = float(data.get("solAmount", 0) or 0)

    ws_stats["trades_received"] += 1
    ws_stats["last_message_at"] = time.time()

    usd_amount = sol_amount * WS_SOL_PRICE_USD

    async with tokens_lock:
        if mint in tokens:
            tokens[mint].ws_sol_volume += sol_amount
            if tx_type == "buy":
                tokens[mint].ws_buy_count += 1
                tokens[mint].ws_buy_vol_usd += usd_amount
            elif tx_type == "sell":
                tokens[mint].ws_sell_count += 1
                tokens[mint].ws_sell_vol_usd += usd_amount


async def pumpfun_ws_listener():
    """
    Connects to PumpPortal WebSocket, subscribes to:
    - subscribeNewToken: fires on every new Pump.fun token creation
    - subscribeTokenTrade: fires on every trade for tracked tokens
    
    Single connection, auto-reconnects with exponential backoff.
    """
    delay = WS_RECONNECT_DELAY_MIN

    while True:
        try:
            logger.info(f"🔌 Connecting to PumpPortal WS: {PUMPPORTAL_WS}")
            async with websockets.connect(
                PUMPPORTAL_WS,
                ping_interval=45,
                ping_timeout=25,
                close_timeout=5,
            ) as ws:
                ws_stats["connected"] = True
                ws_stats["last_message_at"] = time.time()
                delay = WS_RECONNECT_DELAY_MIN  # reset backoff on successful connect
                logger.info("✅ PumpPortal WS connected")

                # Subscribe to new token creation events
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                logger.info("📡 Subscribed to new token events")

                # Subscribe to trades for currently tracked alerted tokens
                alerted_mints = [
                    mint for mint, t in tokens.items()
                    if t.alerted
                ][:WS_MAX_TRADE_SUBS]
                if alerted_mints:
                    await ws.send(json.dumps({
                        "method": "subscribeTokenTrade",
                        "keys": alerted_mints
                    }))
                    ws_stats["trade_subs"] = len(alerted_mints)
                    logger.info(f"📡 Subscribed to {len(alerted_mints)} token trade feeds")

                async for message in ws:
                    try:
                        data = json.loads(message)
                        ws_stats["last_message_at"] = time.time()

                        tx_type = data.get("txType") or data.get("method", "")

                        if tx_type == "create":
                            await _handle_ws_create(data)
                        elif tx_type in ("buy", "sell"):
                            await _handle_ws_trade(data)

                        # [fix] drain pending trade subscription requests
                        if ws_sub_request_queue is not None:
                            while not ws_sub_request_queue.empty():
                                try:
                                    sub_keys = ws_sub_request_queue.get_nowait()
                                    await ws.send(json.dumps({
                                        "method": "subscribeTokenTrade",
                                        "keys": sub_keys
                                    }))
                                    ws_sub_request_queue.task_done()
                                except Exception:
                                    pass

                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        logger.debug(f"WS message error: {e}")

        except asyncio.CancelledError:
            logger.info("🔌 WS listener cancelled")
            ws_stats["connected"] = False
            break
        except Exception as e:
            ws_stats["connected"] = False
            ws_stats["reconnects"] += 1
            logger.warning(f"🔌 WS disconnected: {e} — reconnecting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_DELAY_MAX)


async def ws_trade_subscription_manager():
    """
    Periodically updates the trade subscriptions as new gems are alerted.
    Runs every 60 seconds — subscribes to newly alerted tokens.
    """
    subscribed: Set[str] = set()

    while True:
        try:
            await asyncio.sleep(15)
            if not ws_stats["connected"]:
                continue

            # Find newly alerted tokens not yet subscribed
            newly_alerted = [
                mint for mint, t in tokens.items()
                if t.alerted and mint not in subscribed
            ]

            if newly_alerted and len(subscribed) < WS_MAX_TRADE_SUBS:
                slots_free = WS_MAX_TRADE_SUBS - len(subscribed)
                to_sub = newly_alerted[:slots_free]
                subscribed.update(to_sub)
                ws_stats["trade_subs"] = len(subscribed)
                # [fix] actually send the subscription via the request queue
                if ws_sub_request_queue is not None:
                    try:
                        ws_sub_request_queue.put_nowait(to_sub)
                    except asyncio.QueueFull:
                        logger.warning("WS sub request queue full — skipping")
                logger.info(f"📡 WS: subscribed to {len(to_sub)} new trade feeds | Total: {len(subscribed)}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Trade sub manager error: {e}")

# ============================================================================
# DexScreener Enrichment
# ============================================================================

async def fetch_chain_new_pairs(
    session: aiohttp.ClientSession,
    chain_id: str,
    max_age_hours: float = 6.0,
) -> List[Dict]:
    """
    [v4.11] Dedicated per-chain new pair discovery using DexScreener /pairs endpoint.
    Fetches recently created pairs for ETH/BSC/Base — much more reliable than
    generic search for catching new launches on these chains.

    Filters:
    - pairCreatedAt within max_age_hours
    - Excludes known SKIP_SYMBOLS
    - Excludes pairs not yet in seen_mints
    """
    url = DEXSCREENER_NEW_PAIRS_BY_CHAIN.get(chain_id)
    if not url:
        return []

    SKIP = {"SOL", "USDC", "USDT", "BTC", "ETH", "BONK", "WIF", "JUP", "RAY",
            "BSC", "BASE", "BNB", "WBNB", "WETH", "WBTC", "DAI", "BUSD", "CAKE",
            "ETHEREUM", "UNISWAP", "UNI", "LINK", "AAVE", "MKR", "COMP", "SNX"}

    new_tokens = []
    cutoff = time.time() - (max_age_hours * 3600)

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                logger.debug(f"Chain pairs endpoint returned {resp.status} for {chain_id}")
                return []
            data = await resp.json()
            pairs = data.get("pairs", [])

            # Filter to correct chain first, then sort by creation time
            pairs = [p for p in pairs if p.get("chainId") == chain_id]
            pairs.sort(key=lambda p: p.get("pairCreatedAt", 0) or 0, reverse=True)

            for pair in pairs[:60]:  # scan top 60 most recent pairs
                # Filter by age (skip very old pairs)
                created_ms = pair.get("pairCreatedAt")
                if created_ms and (created_ms / 1000.0) < cutoff:
                    continue  # don't break — search results aren't in strict time order

                mint = pair.get("baseToken", {}).get("address")
                sym = pair.get("baseToken", {}).get("symbol", "").upper()

                if not mint or mint in seen_mints:
                    continue
                if sym in SKIP:
                    continue

                # Basic liquidity pre-filter — skip pairs with no real liquidity
                liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
                th = get_thresholds(chain_id)
                if liq < th["liq_min"] * 0.5:  # 50% of min threshold as pre-filter
                    continue

                seen_mints.add(mint)
                new_tokens.append({
                    "mint": mint,
                    "symbol": sym or pair.get("baseToken", {}).get("symbol", "???"),
                    "name": pair.get("baseToken", {}).get("name", "Unknown"),
                    "created_at": (created_ms / 1000.0) if created_ms else time.time(),
                    "ws_discovered": False,
                    "chain_id": chain_id,
                })

    except Exception as e:
        logger.debug(f"Chain pair fetch error ({chain_id}): {e}")

    if new_tokens:
        logger.debug(f"🔍 [{chain_id.upper()}] Discovered {len(new_tokens)} new pairs")
    return new_tokens


async def fetch_new_tokens(session: aiohttp.ClientSession) -> List[Dict]:
    """
    [v4.9] Polls DexScreener for new tokens across ALL enabled chains.
    Solana is also covered here as a fallback (WS is primary for Solana).
    BSC and Base are polling-only.
    """
    new_tokens = []
    try:
        async with session.get(
            DEXSCREENER_NEW_PAIRS,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                profiles = data if isinstance(data, list) else data.get("data", [])
                for profile in profiles[:TOKENS_PER_POLL * len(ENABLED_CHAIN_IDS)]:
                    chain = profile.get("chainId")
                    if chain not in ENABLED_CHAIN_IDS:
                        continue
                    mint = profile.get("tokenAddress")
                    if mint and mint not in seen_mints:
                        seen_mints.add(mint)
                        new_tokens.append({
                            "mint": mint,
                            "symbol": profile.get("symbol", "???"),
                            "name": profile.get("name", "Unknown"),
                            "created_at": time.time(),
                            "ws_discovered": False,
                            "chain_id": chain,
                        })
                if new_tokens:
                    return new_tokens
    except Exception as e:
        logger.debug(f"DexScreener profiles error: {e}")

    # Fallback: search each non-WS chain directly
    for chain_id in ENABLED_CHAIN_IDS:
        if CHAINS.get(chain_id, {}).get("has_ws") and chain_id == "solana":
            continue  # Solana WS handles discovery; only fallback-search non-WS chains here
        try:
            async with session.get(
                f"{DEXSCREENER_SEARCH}?q={chain_id}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for pair in data.get("pairs", [])[:TOKENS_PER_POLL]:
                        if pair.get("chainId") != chain_id:
                            continue
                        mint = pair.get("baseToken", {}).get("address")
                        if mint and mint not in seen_mints:
                            seen_mints.add(mint)
                            new_tokens.append({
                                "mint": mint,
                                "symbol": pair.get("baseToken", {}).get("symbol", "???"),
                                "name": pair.get("baseToken", {}).get("name", "Unknown"),
                                "created_at": time.time(),
                                "ws_discovered": False,
                                "chain_id": chain_id,
                            })
        except Exception as e:
            logger.debug(f"DexScreener search error ({chain_id}): {e}")

    return new_tokens


async def get_dex_data(session: aiohttp.ClientSession, mint: str, chain_id: str = "solana") -> Optional[Dict]:
    try:
        url = f"{DEXSCREENER_API}/tokens/{mint}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            pairs = data.get("pairs", [])
            if not pairs:
                return None

            # [v4.9] Filter pairs to the correct chain
            chain_pairs = [p for p in pairs if p.get("chainId") == chain_id]
            if not chain_pairs:
                chain_pairs = pairs  # fallback: use all if chain filter returns nothing
            top_pair = max(chain_pairs, key=lambda x: float(x.get("volume", {}).get("h24", 0) or 0))

            pair_created_ms = top_pair.get("pairCreatedAt")
            launched_at = pair_created_ms / 1000.0 if pair_created_ms else 0.0

            txns_h1 = top_pair.get("txns", {}).get("h1", {})
            buys_h1 = int(txns_h1.get("buys", 0) or 0)
            sells_h1 = int(txns_h1.get("sells", 0) or 0)
            total_txns_h1 = buys_h1 + sells_h1
            h1_vol = float(top_pair.get("volume", {}).get("h1", 0) or 0)

            if total_txns_h1 > 0:
                buy_ratio = buys_h1 / total_txns_h1
                buy_volume_h1 = h1_vol * buy_ratio
            else:
                buy_ratio = 0.5
                buy_volume_h1 = h1_vol * 0.5

            vol_m5 = float(top_pair.get("volume", {}).get("m5", 0) or 0)
            return {
                "symbol": top_pair.get("baseToken", {}).get("symbol", "???"),
                "name": top_pair.get("baseToken", {}).get("name", "Unknown"),
                "volume_usd": buy_volume_h1,
                "volume_h1_total": h1_vol,
                "buy_volume_h1": buy_volume_h1,  # [v4.11] explicit buy vol
                "volume_m5": vol_m5,              # [v4.11] 5-min vol for whale detection
                "buy_ratio": buy_ratio,
                "buys_h1": buys_h1,
                "sells_h1": sells_h1,
                "market_cap": float(top_pair.get("fdv", 0) or 0),
                "liquidity": float(top_pair.get("liquidity", {}).get("usd", 0) or 0),
                "launched_at": launched_at,
                "holders": 0,
                "dev_pct": 0.0,
                "top10_pct": 0.0,
                "price_change_h1": float(top_pair.get("priceChange", {}).get("h1", 0) or 0),
                "chain_id": top_pair.get("chainId", chain_id),
            }
    except Exception as e:
        logger.debug(f"DexScreener error for {mint[:8]}: {e}")
        return None


async def poll_boosted_tokens(session: aiohttp.ClientSession):
    global _last_boost_poll, boosted_mints
    now = time.time()
    if now - _last_boost_poll < BOOST_POLL_INTERVAL:
        return
    _last_boost_poll = now
    try:
        async with session.get(DEXSCREENER_BOOSTS, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                return
            data = await resp.json()
            boosts = data if isinstance(data, list) else data.get("data", [])
            new_boosted = {
                b.get("tokenAddress")
                for b in boosts
                if b.get("chainId") in ENABLED_CHAIN_IDS and b.get("tokenAddress")
            }
            boosted_mints = new_boosted
            newly = []
            async with tokens_lock:
                for mint, token in tokens.items():
                    was = token.is_boosted
                    token.is_boosted = mint in boosted_mints
                    if token.is_boosted and not was:
                        newly.append(token.symbol)
            if newly:
                logger.info(f"🚀 Newly boosted: {', '.join(f'${s}' for s in newly)}")
    except Exception as e:
        logger.debug(f"Boost poll error: {e}")

# ============================================================================
# Detection
# ============================================================================

def format_launch_age(launched_at: float) -> str:
    if not launched_at:
        return "unknown"
    age_s = time.time() - launched_at
    if age_s < 3600:
        return f"{int(age_s // 60)}m"
    return f"{age_s / 3600:.1f}h"


def fmt_usd_short(v: float) -> str:
    """$1.2M / $45.3K / $890"""
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def format_gem_alert(token: TokenInfo, alert_reason: str = "GEM", distro: "DistroResult" = None) -> str:
    age_str = format_launch_age(token.launched_at or token.created_at)
    chain = get_chain(token.chain_id)
    _dex_url = dex_url(token.mint, token.chain_id)
    safe_name   = html.escape(token.name   or "Unknown")
    safe_symbol = html.escape(token.symbol or "???")

    SIGNAL_LABELS = {
        "FAST🔥": "⚡ Fast Runner", "EARLY📈": "📈 Early Entry",
        "VOL💰": "💰 Vol Spike", "VACCEL📊": "📊 Vol Accel", "KOL🐦": "🐦 KOL", "GEM": "💎 Gem",
    }
    signal_label = SIGNAL_LABELS.get(alert_reason, f"💎 {alert_reason}")
    buy_pct = int(token.buy_ratio * 100)
    bp_filled = buy_pct // 10
    bp_bar = "█" * bp_filled + "░" * (10 - bp_filled)

    mc_s  = fmt_usd_short(token.market_cap)
    vol_s = fmt_usd_short(token.volume_usd)
    liq_s = fmt_usd_short(token.liquidity)
    txn_str = f"{token.buys_h1}B / {token.sells_h1}S" if (token.buys_h1 or token.sells_h1) else "—"

    # Signal badges (compact)
    badges = []
    if is_high_velocity(token):          badges.append(f"🔥 +{token.mc_velocity:.0f}%")
    if is_vol_accelerating(token):       badges.append("📈 Vol↑")
    if token.ws_discovered:              badges.append("⚡ Live")
    if token.is_boosted:                 badges.append("🚀 Boosted")
    if token.chain_id in ("ethereum", "bsc", "base") and token.buy_volume_h1 > 0:
        badges.append(f"💰 {fmt_usd_short(token.buy_volume_h1)} buy")
    if token.price_change_h1 and abs(token.price_change_h1) > 1:
        arrow = "▲" if token.price_change_h1 > 0 else "▼"
        badges.append(f"{'🟢' if token.price_change_h1 > 0 else '🔴'} {token.price_change_h1:+.0f}% {arrow}")
    if distro and distro.data_available: badges.append(format_distro_line(distro))

    # Chain-specific links
    if token.chain_id == "solana":
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://pump.fun/{token.mint}'>Pump</a>"
    elif token.chain_id == "bsc":
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://dextools.io/app/en/bnb/pair-explorer/{token.mint}'>Tools</a>"
    elif token.chain_id == "base":
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://basescan.org/token/{token.mint}'>Scan</a>"
    elif token.chain_id == "ethereum":
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://etherscan.io/token/{token.mint}'>Scan</a>"
    else:
        links = f"<a href='{_dex_url}'>DEX</a>"

    lines = [
        f"{signal_label}  {chain['emoji']} <b>{chain['label']}</b>",
        f"<b>${safe_symbol}</b>  <i>{safe_name}</i>",
        f"MC <b>{mc_s}</b>  Vol <b>{vol_s}</b>  Liq <b>{liq_s}</b>  Age <b>{age_str}</b>",
        f"[{bp_bar}] {buy_pct}%  {txn_str}",
    ]
    if badges:
        lines.append("  ".join(badges[:4]))
    lines += [
        f"<code>{token.mint}</code>",
        links,
    ]
    return "\n".join(lines)


def format_multiplier_update(token: TokenInfo, multiplier: float) -> str:
    elapsed_min = int((time.time() - token.last_alerted) // 60)
    chain       = get_chain(token.chain_id)
    _dex_url    = dex_url(token.mint, token.chain_id)
    safe_symbol = html.escape(token.symbol or "???")
    pnl_pct     = (multiplier - 1.0) * 100
    emoji       = "🚀" if multiplier >= 5 else "🔥" if multiplier >= 3 else "📈"
    return (
        f"{emoji} <b>${safe_symbol}</b>  x{multiplier:.1f}  <b>+{pnl_pct:.0f}%</b>  {chain['emoji']}{chain['label']}\n"
        f"{fmt_usd_short(token.alert_mc)} → {fmt_usd_short(token.market_cap)}  ·  {elapsed_min}m after alert\n"
        f"<a href='{_dex_url}'>DEX</a>"
    )


def format_performance_snapshot(token: TokenInfo, window: str, current_mc: float) -> str:
    """Reply card shown at 30m / 1h / 4h after initial alert."""
    if not token.alert_mc or token.alert_mc <= 0:
        return ""
    mult        = current_mc / token.alert_mc
    pnl         = (mult - 1.0) * 100
    color       = "🟢" if pnl >= 0 else "🔴"
    chain       = get_chain(token.chain_id)
    _dex_url    = dex_url(token.mint, token.chain_id)
    safe_symbol = html.escape(token.symbol or "???")
    return (
        f"📊 <b>${safe_symbol}</b>  [{window}]  {color} <b>{pnl:+.1f}%</b>  x{mult:.2f}  {chain['emoji']}{chain['label']}\n"
        f"{fmt_usd_short(token.alert_mc)} → {fmt_usd_short(current_mc)}\n"
        f"<a href='{_dex_url}'>DEX</a>"
    )


# ============================================================================
# [v4.11] ETH Buy Volume Detection Helpers
# ============================================================================

# ETH-specific buy volume thresholds
ETH_MIN_BUY_VOL_H1   = 5_000     # $5k buy-side vol — meaningful at $30-60k MC target
ETH_MIN_BUY_VOL_LIQ  = 0.20      # buy vol >= 20% of liquidity at small MC
ETH_WHALE_M5_MC      = 0.05      # 5-min vol >= 5% of MC = potential whale entry
ETH_VOL_ACCEL_MIN    = 1.5       # current vol >= 1.5x avg of previous polls = accelerating
BSC_MIN_BUY_VOL_H1   = 8_000     # BSC has lower USD values — $8k is meaningful
BASE_MIN_BUY_VOL_H1  = 5_000     # Base is smaller — $5k buy vol is signal


def is_buy_vol_significant(token: TokenInfo) -> bool:
    """
    [v4.11] ETH/BSC/Base buy volume signal check.
    Returns True if buy-side volume pressure is significant for the chain.

    Logic:
    - Absolute buy volume exceeds chain minimum (filters tiny tokens)
    - Buy volume >= 30% of liquidity (relative pressure — more reliable than vol/MC)
    - OR 5-min vol spike relative to MC (whale entry detection)
    """
    chain = token.chain_id
    if chain not in ("ethereum", "bsc", "base"):
        return False  # Solana uses transaction count, not this

    vol = token.buy_volume_h1
    liq = token.liquidity
    mc  = token.market_cap
    m5  = token.volume_m5

    # Floor: minimum absolute buy volume per chain
    if chain == "ethereum":
        min_vol = ETH_MIN_BUY_VOL_H1
    elif chain == "bsc":
        min_vol = BSC_MIN_BUY_VOL_H1
    else:
        min_vol = BASE_MIN_BUY_VOL_H1

    if vol < min_vol:
        return False

    # Buy vol / liquidity pressure check
    vol_liq_pressure = liq > 0 and (vol / liq) >= ETH_MIN_BUY_VOL_LIQ

    # Whale 5-min spike check
    whale_spike = mc > 0 and m5 > 0 and (m5 / mc) >= ETH_WHALE_M5_MC

    return vol_liq_pressure or whale_spike


def is_buy_vol_accelerating(token: TokenInfo) -> bool:
    """
    [v4.11] Buy volume acceleration for ETH/BSC/Base.
    Returns True if buy volume is trending up across recent polls.
    Requires at least 3 data points.
    """
    hist = token.vol_usd_history
    if len(hist) < 3:
        return False
    recent = hist[-2:]        # last 2 polls
    older  = hist[:-2]        # everything before
    avg_old = sum(older) / len(older)
    avg_new = sum(recent) / len(recent)
    return avg_old > 0 and avg_new >= avg_old * ETH_VOL_ACCEL_MIN


# ============================================================================
# [v4.10] Distribution & Bundle Check (Solana / Helius)
# ============================================================================

# Known system/DEX addresses to exclude from holder analysis
_SYSTEM_ADDRESSES = {
    "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM",  # Raydium LP vault
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",  # Raydium AMM authority
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", # Raydium AMM program
    "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",  # Serum DEX
    "11111111111111111111111111111111",               # System program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # Token program
}

# Distro check thresholds
DISTRO_TOP10_MAX = 0.60      # block if top 10 wallets hold >60% of supply
DISTRO_TOP1_MAX  = 0.25      # block if any single wallet holds >25%
BUNDLE_WINDOW_S  = 4         # seconds — buys within this window = bundle
BUNDLE_MIN_WALLETS = 5       # min distinct wallets buying in window = bundle
DISTRO_TIMEOUT   = 4.0       # seconds max to spend on Helius call

@dataclass
class DistroResult:
    passed: bool = True           # True = safe to alert
    top1_pct: float = 0.0
    top10_pct: float = 0.0
    holder_count: int = 0
    bundled: bool = False
    bundle_wallets: int = 0
    skip_reason: str = ""         # why it was skipped (empty = passed)
    data_available: bool = False  # False = Helius unavailable, don't block


async def _helius_rpc(session: aiohttp.ClientSession, method: str, params: list) -> Optional[dict]:
    """Single Helius JSON-RPC call with tight timeout."""
    if not HELIUS_RPC:
        return None
    try:
        async with session.post(
            HELIUS_RPC,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=aiohttp.ClientTimeout(total=DISTRO_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("result")
    except Exception as e:
        logger.debug(f"Helius RPC error ({method}): {e}")
        return None


async def check_distro_and_bundle(
    session: aiohttp.ClientSession,
    mint: str,
    token_created_at: float,
) -> DistroResult:
    """
    [v4.10] Two-stage Solana distribution + bundle check using Helius RPC.

    Stage 1 — Holder concentration (getTokenLargestAccounts):
      - Fetches top 20 largest holder accounts
      - Excludes known DEX/LP/system addresses
      - Blocks if top1 > 25% or top10 > 60% of supply

    Stage 2 — Bundle detection (getSignaturesForAddress):
      - Fetches first ~30 transactions on the mint address
      - Groups by transaction time within BUNDLE_WINDOW_S seconds of creation
      - If BUNDLE_MIN_WALLETS+ distinct signers bought in that window → bundled

    Returns DistroResult with passed=True if safe, or reason for blocking.
    Degrades gracefully: if Helius is unavailable, returns passed=True (don't block).
    """
    result = DistroResult()

    # ── Stage 1: Holder concentration ────────────────────────────────────────
    largest = await _helius_rpc(session, "getTokenLargestAccounts", [mint])
    if not largest or "value" not in largest:
        # Helius unavailable — don't block the alert
        logger.debug(f"Distro check skipped for {mint[:8]} (Helius unavailable)")
        return result

    result.data_available = True
    accounts = largest["value"]

    # Filter out system/DEX addresses
    real_accounts = [
        a for a in accounts
        if a.get("address") not in _SYSTEM_ADDRESSES
    ]

    if not real_accounts:
        return result

    # Get total supply for percentage calculation
    supply_result = await _helius_rpc(session, "getTokenSupply", [mint])
    total_supply = None
    if supply_result and "value" in supply_result:
        total_supply = float(supply_result["value"].get("uiAmount", 0) or 0)

    if not total_supply or total_supply == 0:
        # Estimate from top holders
        amounts = [float(a.get("uiAmount", 0) or 0) for a in real_accounts]
        total_supply = sum(amounts) * 2.0  # rough estimate — double top holders

    amounts = sorted(
        [float(a.get("uiAmount", 0) or 0) for a in real_accounts],
        reverse=True
    )

    result.holder_count = len(amounts)
    top1  = amounts[0] / total_supply if amounts else 0
    top10 = sum(amounts[:10]) / total_supply if len(amounts) >= 1 else 0

    result.top1_pct  = round(top1 * 100, 1)
    result.top10_pct = round(top10 * 100, 1)

    if top1 > DISTRO_TOP1_MAX:
        result.passed = False
        result.skip_reason = f"Top wallet holds {result.top1_pct:.0f}% of supply"
        logger.info(f"🚫 Distro block [{mint[:8]}]: {result.skip_reason}")
        return result

    if top10 > DISTRO_TOP10_MAX:
        result.passed = False
        result.skip_reason = f"Top 10 wallets hold {result.top10_pct:.0f}% of supply"
        logger.info(f"🚫 Distro block [{mint[:8]}]: {result.skip_reason}")
        return result

    # ── Stage 2: Bundle detection ─────────────────────────────────────────────
    # Get first transactions on this mint address, sorted oldest-first
    sigs_result = await _helius_rpc(session, "getSignaturesForAddress", [
        mint,
        {"limit": 30, "commitment": "confirmed"}
    ])
    if not sigs_result or not isinstance(sigs_result, list):
        return result  # no bundle data — still passed stage 1

    # Filter to signatures near token creation time
    bundle_end = token_created_at + BUNDLE_WINDOW_S + 30  # generous window
    early_sigs = [
        s for s in sigs_result
        if s.get("blockTime") and token_created_at <= s["blockTime"] <= bundle_end
    ]

    if not early_sigs:
        return result

    # Group signers by the bundle window around the first transaction
    first_block_time = min(s["blockTime"] for s in early_sigs)
    window_sigs = [
        s for s in early_sigs
        if s["blockTime"] <= first_block_time + BUNDLE_WINDOW_S
    ]

    # Count distinct signers in that window (each signer = distinct wallet)
    distinct_signers = {s.get("memo") or s.get("signature", "")[:8] for s in window_sigs}
    # Use signature as proxy — each unique signature = distinct wallet tx
    distinct_count = len(window_sigs)

    if distinct_count >= BUNDLE_MIN_WALLETS:
        result.bundled = True
        result.bundle_wallets = distinct_count
        result.passed = False
        result.skip_reason = f"Bundled launch: {distinct_count} wallets bought in first {BUNDLE_WINDOW_S}s"
        logger.info(f"🚫 Bundle block [{mint[:8]}]: {result.skip_reason}")
        return result

    return result


def format_distro_line(dr: DistroResult) -> str:
    """One-line distro summary for Telegram alert."""
    if not dr.data_available:
        return ""
    risk = "🟢" if dr.top10_pct < 40 else "🟡" if dr.top10_pct < 55 else "🔴"
    return f"{risk} <b>Distro:</b> Top1={dr.top1_pct:.0f}% Top10={dr.top10_pct:.0f}% Holders≥{dr.holder_count}"


async def run_detections(token: TokenInfo, session: aiohttp.ClientSession = None):
    """
    [v4.10] Per-chain gem detection with 5 signal layers:
      1. Standard gate   — age + MC + vol/MC ratio + liquidity (per-chain thresholds)
      2. Buy pressure    — buy_ratio gate (filters dumps, per-chain)
      3. Min buys gate   — buys_h1 minimum (filters wash trading, per-chain)
      4. Fast velocity   — MC velocity >= 50% bypasses age_min (catches rockets early)
      5. Vol accel early — vol accelerating + young token = early runner signal
    WS live buy/sell pressure boosts sensitivity when buys >> sells (Solana only).
    """
    now = time.time()
    ref_time = token.launched_at if token.launched_at else token.created_at
    age_s = now - ref_time

    if not token.alerted and now - token.last_alerted > GEM_COOLDOWN:
        mc = token.market_cap
        vol = token.volume_usd
        liq = token.liquidity

        # [v4.10] Load per-chain thresholds
        th = get_thresholds(token.chain_id)
        age_min      = th["age_min"]
        age_max      = th["age_max"]
        mc_min       = th["mc_min"]
        mc_max       = th["mc_max"]
        vol_mc_ratio = th["vol_mc_ratio"]
        liq_min      = th["liq_min"]
        buy_ratio_min = th["buy_ratio_min"]
        min_buys_h1  = th["min_buys_h1"]

        # ── Base gates ──────────────────────────────────────────────────────
        age_ok   = age_min <= age_s <= age_max
        mc_ok    = mc_min <= mc <= mc_max
        vol_ok   = mc > 0 and (vol / mc) >= vol_mc_ratio
        liq_ok   = liq >= liq_min

        # ── Buy pressure gates ────────────────────────────────────────────
        # Buy ratio: must be buys-dominant (filters dumps)
        buy_ratio_ok = token.buy_ratio >= buy_ratio_min
        # Min buys: filters wash trading
        # [fix] buys_h1==0 means no data fetched yet — don't block on missing data
        min_buys_ok = token.buys_h1 >= min_buys_h1 or token.buys_h1 == 0

        # ── WS live signal: boost if live buys >> sells (Solana) ─────────
        ws_total = token.ws_buy_count + token.ws_sell_count
        ws_buy_pressure = (
            token.ws_buy_count / max(token.ws_sell_count, 1) >= GEM_WS_BUY_PRESSURE
            if ws_total >= 5 else False
        )
        # If WS pressure is strong, relax min_buys gate for new tokens
        if ws_buy_pressure and ws_total >= 10:
            min_buys_ok = True

        # ── WS directional USD volume: net buy flow from live trade feed ──
        # ws_buy_vol_usd / ws_sell_vol_usd give dollar-denominated pressure.
        # Strong net buy flow (>60% buy-side) reinforces the buy_pressure signal.
        ws_usd_total = token.ws_buy_vol_usd + token.ws_sell_vol_usd
        ws_usd_buy_dominant = (
            ws_usd_total >= 500  # at least $500 total flow before trusting the ratio
            and token.ws_buy_vol_usd / max(token.ws_sell_vol_usd, 1.0) >= 1.5
        )
        if ws_usd_buy_dominant and not ws_buy_pressure:
            # USD flow says buy-dominant even if txn count is low — relax gate
            ws_buy_pressure = True
        if ws_usd_buy_dominant and ws_usd_total >= 2000:
            # Strong dollar flow → unconditionally relax min_buys
            min_buys_ok = True

        # ── [v4.11] Anti-dump gates ──────────────────────────────────────
        # Gate 1: Price not already crashing — if price is down >20% in 1h it's a dump
        price_not_crashing = token.price_change_h1 > -20.0

        # Gate 2: MC hasn't collapsed from first observation
        # If token peaked and is now <40% of its first seen MC → dump in progress
        mc_not_collapsed = (
            token.first_seen_mc == 0.0  # not enough data yet — let it through
            or mc >= token.first_seen_mc * 0.40
        )

        # Gate 3: Sustained momentum — require at least 2 consecutive green polls
        # This filters single-poll spikes (classic pump-and-dump signature)
        # WS-discovered tokens get 1 free pass since we don't have poll history yet
        sustained_momentum = (
            token.consecutive_green_polls >= 2
            or token.ws_discovered  # WS tokens don't have poll history, use other gates
        )

        # Gate 4: Suppression — don't re-alert a suppressed token
        not_suppressed = time.time() > token.alert_suppressed_until

        # ── Fast velocity path: bypass age_min for rockets ───────────────
        # Token is pumping so fast the normal age window is too slow
        fast_velocity = (
            token.mc_velocity >= GEM_FAST_VELOCITY
            and GEM_FAST_MC_MIN <= mc <= mc_max
            and vol_ok and liq_ok
            and buy_ratio_ok
            and age_s <= age_max
            and price_not_crashing and mc_not_collapsed and not_suppressed
        )

        # ── Vol acceleration early trigger ────────────────────────────────
        # Catches tokens that haven't hit age_min yet but vol is spiking
        vol_accel_early = (
            is_vol_accelerating(token)
            and age_s < age_min
            and age_s >= 2 * 60
            and GEM_FAST_MC_MIN <= mc <= GEM_VOL_ACCEL_MC_MAX
            and liq_ok
            and buy_ratio_ok
            and token.buys_h1 >= max(min_buys_h1 // 2, 10)
            and price_not_crashing and mc_not_collapsed and not_suppressed
        )

        # ── Standard path ────────────────────────────────────────────────
        standard = (age_ok and mc_ok and vol_ok and liq_ok and buy_ratio_ok and min_buys_ok
                    and price_not_crashing and mc_not_collapsed and sustained_momentum
                    and not_suppressed)

        # ── [v4.11] ETH/BSC/Base buy volume path ─────────────────────────
        # Separate from Solana's txn-count based detection.
        # Triggers when buy-side USD volume shows real accumulation pressure.
        eth_vol_signal = (
            token.chain_id in ("ethereum", "bsc", "base")
            and is_buy_vol_significant(token)
            and buy_ratio_ok
            and liq_ok
            and mc_ok
            and price_not_crashing
            and mc_not_collapsed
            and not_suppressed
            and age_s >= 5 * 60        # at least 5 min old
            and age_s <= age_max
        )

        eth_vol_accel = (
            token.chain_id in ("ethereum", "bsc", "base")
            and is_buy_vol_accelerating(token)
            and is_buy_vol_significant(token)
            and liq_ok
            and price_not_crashing
            and mc_not_collapsed
            and not_suppressed
            and age_s >= 10 * 60
            and age_s <= age_max
        )

        alert_reason = None
        if standard:
            alert_reason = "GEM"
        elif fast_velocity:
            alert_reason = "FAST🔥"
        elif vol_accel_early:
            alert_reason = "EARLY📈"
        elif eth_vol_signal:
            alert_reason = "VOL💰"
        elif eth_vol_accel:
            alert_reason = "VACCEL📊"

        if alert_reason:
            # [v4.10] Distribution + bundle check (Solana only, non-blocking)
            distro = DistroResult()  # default: passed=True
            if token.chain_id == "solana" and HELIUS_RPC:
                try:
                    import asyncio
                    distro = await asyncio.wait_for(
                        check_distro_and_bundle(session, token.mint, token.created_at),
                        timeout=DISTRO_TIMEOUT + 1
                    )
                except Exception as e:
                    logger.debug(f"Distro check error for {token.mint[:8]}: {e}")

            if not distro.passed:
                logger.info(f"🚫 Alert blocked [{token.symbol}]: {distro.skip_reason}")
                # [v4.12] Record distro-blocked near-miss in analysis ring
                _analysis_ring.append({
                    "ts": now, "mint": token.mint, "symbol": token.symbol,
                    "chain": token.chain_id, "mc": mc, "vol": vol, "liq": liq,
                    "age_m": round(age_s / 60, 1), "buy_ratio": token.buy_ratio,
                    "buys_h1": token.buys_h1, "ws_buys": token.ws_buy_count,
                    "ws_sells": token.ws_sell_count, "signal": alert_reason,
                    "alerted": False, "skip": distro.skip_reason,
                })
                return  # silently drop — don't alert on bundled/concentrated tokens

            token.alerted = True
            token.last_alerted = now
            token.alert_mc = mc
            msg_id = await _send_telegram_direct(format_gem_alert(token, alert_reason, distro))
            if msg_id:
                token.alert_message_id = msg_id  # store for reply-threading
            src = "⚡WS" if token.ws_discovered else "📡Poll"
            buy_info = f"BR={token.buy_ratio:.0%} B1h={token.buys_h1}"
            ws_info = f"WS={token.ws_buy_count}B/{token.ws_sell_count}S" if ws_total > 0 else ""
            distro_info = f"Top10={distro.top10_pct:.0f}%" if distro.data_available else "NoDistro"
            green_info = f"Green={token.consecutive_green_polls}polls"
            logger.info(
                f"💎 {alert_reason} [{src}]: ${token.symbol} "
                f"MC=${mc:,.0f} Vol={vol:,.0f} Age={int(age_s//60)}m "
                f"{buy_info} {ws_info} {distro_info} {green_info}"
            )
            # [v4.12] Record successful alert in analysis ring
            _analysis_ring.append({
                "ts": now, "mint": token.mint, "symbol": token.symbol,
                "chain": token.chain_id, "mc": mc, "vol": vol, "liq": liq,
                "age_m": round(age_s / 60, 1), "buy_ratio": token.buy_ratio,
                "buys_h1": token.buys_h1, "ws_buys": token.ws_buy_count,
                "ws_sells": token.ws_sell_count, "signal": alert_reason,
                "alerted": True, "skip": "",
            })
            return

        # [v4.12] No detection path fired — record near-miss for /analysis visibility
        # Skip stubs (pre-enrichment tokens have no real market data yet)
        if not getattr(token, "ws_pre_enrichment", False):
            # Determine which gate first failed
            if not age_ok:
                skip = f"age {age_s/60:.1f}m (need {age_min/60:.0f}-{age_max/3600:.0f}h)"
            elif not mc_ok:
                skip = f"MC ${mc:,.0f} (need ${mc_min:,.0f}-${mc_max:,.0f})"
            elif not vol_ok:
                skip = f"vol/MC {vol/max(mc,1):.2%} (need {vol_mc_ratio:.0%})"
            elif not liq_ok:
                skip = f"liq ${liq:,.0f} (need ${liq_min:,.0f})"
            elif not buy_ratio_ok:
                skip = f"buy_ratio {token.buy_ratio:.0%} (need {buy_ratio_min:.0%})"
            elif not min_buys_ok:
                skip = f"buys_h1 {token.buys_h1} (need {min_buys_h1})"
            else:
                skip = "no signal path matched"
            _analysis_ring.append({
                "ts": now, "mint": token.mint, "symbol": token.symbol,
                "chain": token.chain_id, "mc": mc, "vol": vol, "liq": liq,
                "age_m": round(age_s / 60, 1), "buy_ratio": token.buy_ratio,
                "buys_h1": token.buys_h1, "ws_buys": token.ws_buy_count,
                "ws_sells": token.ws_sell_count, "signal": None,
                "alerted": False, "skip": skip,
            })

    if token.alerted and token.alert_mc > 0:
        current_mult = token.market_cap / token.alert_mc
        # [v4.11] If token is dumping hard after alert, suppress further alerts
        if current_mult < 0.3 and token.alert_suppressed_until == 0.0:
            token.alert_suppressed_until = time.time() + 3600  # suppress 1hr
            logger.info(f"🚫 Suppressed ${token.symbol} — dumped to x{current_mult:.2f}")
        if not hasattr(token, '_sent_milestones'):
            token._sent_milestones = set()
        for milestone in MULTIPLIER_MILESTONES:
            if current_mult >= milestone and milestone not in token._sent_milestones:
                token._sent_milestones.add(milestone)
                msg = format_multiplier_update(token, milestone)
                # Reply to the original alert message if we have its ID
                if token.alert_message_id:
                    await send_telegram_reply(msg, reply_to_message_id=token.alert_message_id)
                else:
                    await send_telegram(msg)
                logger.info(f"🚀 MILESTONE: ${token.symbol} x{milestone} MC=${token.market_cap:,.0f}")
                break

# ============================================================================
# Web Dashboard (v4.4 + v4.5 WS stats)
# ============================================================================

def build_dashboard_html() -> str:
    now = time.time()
    gems = sorted([(m, t) for m, t in tokens.items() if t.alerted], key=lambda x: x[1].market_cap, reverse=True)
    hot = sorted([(m, t) for m, t in tokens.items() if is_high_velocity(t) or is_vol_accelerating(t)], key=lambda x: x[1].mc_velocity, reverse=True)

    def age_str(t):
        ref = t.launched_at or t.created_at
        s = now - ref
        return f"{int(s//60)}m" if s < 3600 else f"{s/3600:.1f}h"

    ws_age = int(now - ws_stats["last_message_at"]) if ws_stats["last_message_at"] else -1
    ws_color = "#00ff88" if ws_stats["connected"] else "#ff4444"
    ws_label = "🟢 Connected" if ws_stats["connected"] else "🔴 Disconnected"

    gem_rows = ""
    for mint, t in gems[:30]:
        mult = f"x{t.market_cap/t.alert_mc:.1f}" if t.alert_mc > 0 else "—"
        badges = ""
        if t.ws_discovered: badges += " ⚡"
        if is_high_velocity(t): badges += " 🔥"
        if is_vol_accelerating(t): badges += " 📈"
        if t.is_boosted: badges += " 🚀"
        gem_rows += (
            f"<tr><td><b>${t.symbol}</b>{badges}</td>"
            f"<td>${t.market_cap:,.0f}</td><td>{mult}</td>"
            f"<td>{age_str(t)}</td>"
            f"<td>{get_chain(t.chain_id)['emoji']}</td>"
            f"<td><a href='{dex_url(mint, t.chain_id)}' target='_blank'>DEX</a></td></tr>"
        )

    hot_rows = ""
    for mint, t in hot[:15]:
        tags = []
        if is_high_velocity(t): tags.append(f"MC+{t.mc_velocity:.1f}%")
        if is_vol_accelerating(t): tags.append("Vol↑")
        if t.ws_discovered: tags.append("⚡WS")
        if t.is_boosted: tags.append("Boosted")
        hot_rows += (
            f"<tr><td><b>${t.symbol}</b></td>"
            f"<td>${t.market_cap:,.0f}</td>"
            f"<td>{', '.join(tags)}</td>"
            f"<td>{get_chain(t.chain_id)['emoji']}</td>"
            f"<td><a href='{dex_url(mint, t.chain_id)}' target='_blank'>DEX</a></td></tr>"
        )

    return f"""<!DOCTYPE html>
<html>
<head>
<title>ASTAROTH v4.11</title>
<meta http-equiv="refresh" content="15">
<style>
body{{font-family:monospace;background:#0a0a0a;color:#e0e0e0;margin:20px;}}
h1{{color:#00ff88;}} h2{{color:#00ccff;margin-top:30px;}}
table{{border-collapse:collapse;width:100%;margin-top:10px;}}
th{{background:#1a1a2e;color:#00ff88;padding:8px;text-align:left;}}
td{{padding:6px 8px;border-bottom:1px solid #1a1a1a;}}
tr:hover{{background:#111;}}
a{{color:#00ccff;}}
.stat{{display:inline-block;margin:5px 15px 5px 0;}}
.badge{{background:#1a1a2e;padding:2px 8px;border-radius:3px;font-size:0.85em;}}
.ws-box{{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:10px 15px;margin:10px 0;display:inline-block;}}
</style>
</head>
<body>
<h1>🔱 ASTAROTH v4.11</h1>
<div class="ws-box">
  <b style="color:{ws_color}">{ws_label}</b> &nbsp;|&nbsp;
  Last msg: {ws_age}s ago &nbsp;|&nbsp;
  Discovered: {ws_stats['tokens_discovered']} &nbsp;|&nbsp;
  Trades: {ws_stats['trades_received']} &nbsp;|&nbsp;
  Trade subs: {ws_stats['trade_subs']} &nbsp;|&nbsp;
  Reconnects: {ws_stats['reconnects']}
</div>
<p>
  <span class="stat">📊 Tracking: <b>{len(tokens)}</b>/{MAX_TRACKED_TOKENS}</span>
  <span class="stat">💎 Gems: <b>{len(gems)}</b></span>
  <span class="stat">🔥 Hot: <b>{len(hot)}</b></span>
  <span class="stat">⚡ Via WS: <b>{sum(1 for t in tokens.values() if t.ws_discovered)}</b></span>
  <span class="stat badge">Auto-refresh 15s</span>
</p>

<h2>💎 Alerted Gems</h2>
<table>
<tr><th>Token</th><th>Market Cap</th><th>Mult</th><th>Age</th><th>Link</th></tr>
{gem_rows or '<tr><td colspan="5">No gems yet</td></tr>'}
</table>

<h2>🔥 Hot / Accelerating</h2>
<table>
<tr><th>Token</th><th>Market Cap</th><th>Signals</th><th>Link</th></tr>
{hot_rows or '<tr><td colspan="4">None right now</td></tr>'}
</table>
</body>
</html>"""

# ============================================================================
# [v4.5] WS Discovery Queue Processor
# ============================================================================

async def ws_enrich_worker(session: aiohttp.ClientSession):
    """
    Drains ws_discovery_queue. For each WS-discovered token:
    1. Waits WS_ENRICH_DELAY seconds (let DexScreener index it first)
    2. Fetches enriched data from DexScreener
    3. Adds to token pool
    Also handles KOL-sourced symbol searches (mint=None, search_by_symbol=True).
    """
    while True:
        try:
            token_data = await ws_discovery_queue.get()
            mint = token_data.get("mint")

            # [v4.7] KOL symbol search: no mint known, search DexScreener by symbol
            if token_data.get("search_by_symbol") and not mint:
                symbol = token_data.get("symbol", "").upper()
                kol_source = token_data.get("kol_source", "unknown")
                try:
                    async with session.get(
                        f"{DEXSCREENER_SEARCH}?q={symbol}",
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for pair in data.get("pairs", [])[:3]:
                                if pair.get("chainId") not in ENABLED_CHAIN_IDS:
                                    continue
                                if pair.get("baseToken", {}).get("symbol", "").upper() != symbol:
                                    continue
                                found_mint = pair.get("baseToken", {}).get("address")
                                if found_mint and found_mint not in seen_mints:
                                    # Re-queue with mint resolved
                                    await ws_discovery_queue.put({
                                        "mint": found_mint,
                                        "symbol": symbol,
                                        "name": pair.get("baseToken", {}).get("name", symbol),
                                        "created_at": token_data["created_at"],
                                        "ws_discovered": False,
                                        "ws_initial_buy_sol": 0.0,
                                        "mc_sol_at_creation": 0.0,
                                        "kol_source": kol_source,
                                    })
                                    logger.info(f"👁️ KOL ${symbol} resolved mint: {found_mint[:8]}...")
                                break
                except Exception as e:
                    logger.debug(f"KOL symbol search error ${symbol}: {e}")
                ws_discovery_queue.task_done()
                continue

            # Brief delay — DexScreener needs a moment to index new tokens
            await asyncio.sleep(WS_ENRICH_DELAY)

            ws_chain_id = token_data.get("chain_id", "solana")
            dex_data = await get_dex_data(session, mint, ws_chain_id)

            mc = dex_data["market_cap"] if dex_data else 0.0
            sym = (dex_data["symbol"] if dex_data else token_data["symbol"]).upper()

            SKIP_SYMBOLS = {"SOL", "USDC", "USDT", "BTC", "ETH", "BONK", "WIF", "JUP", "RAY", "BSC", "BASE", "BNB", "WBNB", "WETH", "WBTC", "DAI", "BUSD", "CAKE", "ETHEREUM", "UNISWAP", "UNI", "LINK", "AAVE", "MKR", "COMP", "SNX"}
            if sym in SKIP_SYMBOLS or mc > 5_000_000:
                ws_discovery_queue.task_done()
                continue
            if mc < 1_000 and dex_data:
                ws_discovery_queue.task_done()
                continue

            async with tokens_lock:
                real_launch = (dex_data.get("launched_at", 0.0) if dex_data else 0.0)
                final_chain = (dex_data.get("chain_id") if dex_data else None) or ws_chain_id

                if mint in tokens and tokens[mint].ws_pre_enrichment:
                    # [v4.12] Upgrade the stub token with real DexScreener data.
                    # Preserve trade counters that accumulated during the delay.
                    t = tokens[mint]
                    t.symbol = dex_data["symbol"] if dex_data else t.symbol
                    t.name = dex_data["name"] if dex_data else t.name
                    if real_launch:
                        t.created_at = real_launch
                        t.launched_at = real_launch
                    t.market_cap = mc if mc else t.market_cap
                    t.last_mc = t.market_cap
                    t.volume_usd = dex_data["volume_usd"] if dex_data else t.volume_usd
                    t.liquidity = dex_data["liquidity"] if dex_data else t.liquidity
                    t.buy_ratio = dex_data.get("buy_ratio", t.buy_ratio) if dex_data else t.buy_ratio
                    t.buys_h1 = dex_data.get("buys_h1", t.buys_h1) if dex_data else t.buys_h1
                    t.sells_h1 = dex_data.get("sells_h1", t.sells_h1) if dex_data else t.sells_h1
                    t.vol_history = [t.volume_usd] if t.volume_usd else t.vol_history
                    t.chain_id = final_chain
                    t.is_boosted = mint in boosted_mints
                    t.ws_pre_enrichment = False  # fully enriched
                    chain_label = get_chain(final_chain)["label"]
                    logger.info(f"⚡ WS→Enriched [{chain_label}] ${sym} | MC: ${mc:,.0f} (stub upgraded)")

                elif mint not in tokens:
                    if not enforce_token_cap(sym):
                        ws_discovery_queue.task_done()
                        continue

                    tokens[mint] = TokenInfo(
                        mint=mint,
                        symbol=dex_data["symbol"] if dex_data else token_data["symbol"],
                        name=dex_data["name"] if dex_data else token_data["name"],
                        created_at=real_launch if real_launch else token_data["created_at"],
                        launched_at=real_launch,
                        market_cap=mc,
                        volume_usd=dex_data["volume_usd"] if dex_data else 0.0,
                        liquidity=dex_data["liquidity"] if dex_data else 0.0,
                        buy_ratio=dex_data.get("buy_ratio", 0.5) if dex_data else 0.5,
                        buys_h1=dex_data.get("buys_h1", 0) if dex_data else 0,
                        sells_h1=dex_data.get("sells_h1", 0) if dex_data else 0,
                        last_mc=mc,
                        is_boosted=mint in boosted_mints,
                        vol_history=[dex_data["volume_usd"]] if dex_data else [],
                        ws_discovered=True,
                        ws_initial_buy_sol=token_data.get("ws_initial_buy_sol", 0.0),
                        chain_id=final_chain,
                    )
                    chain_label = get_chain(final_chain)["label"]
                    logger.info(f"⚡ WS→Tracked [{chain_label}] ${sym} | MC: ${mc:,.0f}")

            ws_discovery_queue.task_done()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"WS enrich error: {e}")
            try:
                ws_discovery_queue.task_done()
            except Exception:
                pass

# ============================================================================
# Main Polling Loop
# ============================================================================

async def polling_loop():
    """
    In v4.5 this is a FALLBACK loop — WS handles primary discovery.
    Still runs every POLL_INTERVAL (now 15s) for:
    - Tokens missed by WS
    - Enrichment updates for existing tokens
    - Maintenance tasks
    """
    global _last_chain_poll
    logger.info("📡 Fallback Polling Loop Started (WS is primary)")

    consecutive_errors = 0
    last_poll = 0

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:

        # Start WS enrichment worker using this session
        enrich_task = asyncio.create_task(ws_enrich_worker(session))

        try:
            while True:
                try:
                    current_time = time.time()
                    if current_time - last_poll < POLL_INTERVAL:
                        await asyncio.sleep(0.5)
                        continue
                    last_poll = current_time

                    # Maintenance
                    await prune_dead_tokens()
                    await save_state()
                    await poll_boosted_tokens(session)

                    # Fallback: fetch tokens WS may have missed (Solana primary)
                    new_tokens = await fetch_new_tokens(session)

                    # [v4.11] Dedicated per-chain discovery for ETH/BSC/Base
                    for cid in ["ethereum", "bsc", "base"]:
                        if cid not in ENABLED_CHAIN_IDS:
                            continue
                        interval = CHAIN_POLL_INTERVAL.get(cid, 30)
                        if current_time - _last_chain_poll.get(cid, 0) >= interval:
                            _last_chain_poll[cid] = current_time
                            chain_tokens = await fetch_chain_new_pairs(session, cid)
                            new_tokens.extend(chain_tokens)
                    for token_data in new_tokens:
                        try:
                            mint = token_data["mint"]
                            tok_chain_id = token_data.get("chain_id", "solana")
                            dex_data = await get_dex_data(session, mint, tok_chain_id)
                            if not dex_data:
                                continue

                            mc = dex_data["market_cap"]
                            sym = dex_data["symbol"].upper()
                            SKIP_SYMBOLS = {"SOL", "USDC", "USDT", "BTC", "ETH", "BONK", "WIF", "JUP", "RAY", "BSC", "BASE", "BNB", "WBNB", "WETH", "WBTC", "DAI", "BUSD", "CAKE", "ETHEREUM", "UNISWAP", "UNI", "LINK", "AAVE", "MKR", "COMP", "SNX"}
                            chain_id_tok = token_data.get("chain_id", "solana")
                            _th = get_thresholds(chain_id_tok)
                            mc_floor = max(5_000, _th["mc_min"] // 4)  # dynamic per-chain floor (quarter of detection min)
                            if sym in SKIP_SYMBOLS or mc > 5_000_000:
                                seen_mints.add(mint)
                                continue
                            if mc < mc_floor:
                                continue

                            async with tokens_lock:
                                if mint not in tokens:
                                    if not enforce_token_cap(sym):
                                        continue
                                    real_launch = dex_data.get("launched_at", 0.0)
                                    tok_chain = dex_data.get("chain_id", token_data.get("chain_id", "solana"))
                                    tokens[mint] = TokenInfo(
                                        mint=mint,
                                        symbol=dex_data["symbol"],
                                        name=dex_data["name"],
                                        created_at=real_launch if real_launch else time.time(),
                                        launched_at=real_launch,
                                        market_cap=mc,
                                        volume_usd=dex_data["volume_usd"],
                                        liquidity=dex_data["liquidity"],
                                        buy_ratio=dex_data.get("buy_ratio", 0.5),
                                        buys_h1=dex_data.get("buys_h1", 0),
                                        sells_h1=dex_data.get("sells_h1", 0),
                                        last_mc=mc,
                                        is_boosted=mint in boosted_mints,
                                        vol_history=[dex_data["volume_usd"]],
                                        ws_discovered=False,
                                        chain_id=tok_chain,
                                    )
                                    chain_label = get_chain(tok_chain)["label"]
                                    logger.info(f"📡 Poll→Tracked [{chain_label}] ${dex_data['symbol']} | MC: ${mc:,.0f}")
                            await asyncio.sleep(0.2)  # [fix] rate limit
                        except Exception as e:
                            logger.error(f"Poll token error: {e}")

                    # Enrich + detect on all tracked tokens
                    async with tokens_lock:
                        token_list = list(tokens.items())

                    for mint, token in token_list:
                        try:
                            dex_data = await get_dex_data(session, mint, token.chain_id)
                            if dex_data:
                                new_mc = dex_data["market_cap"]
                                async with tokens_lock:
                                    if mint in tokens:
                                        update_mc_velocity(tokens[mint], new_mc)
                                        update_vol_history(tokens[mint], dex_data["volume_usd"])
                                        tokens[mint].volume_usd = dex_data["volume_usd"]
                                        tokens[mint].market_cap = new_mc
                                        tokens[mint].liquidity = dex_data["liquidity"]
                                        if dex_data.get("launched_at") and not tokens[mint].launched_at:
                                            tokens[mint].launched_at = dex_data["launched_at"]
                                        tokens[mint].buy_ratio = dex_data.get("buy_ratio", tokens[mint].buy_ratio)
                                        tokens[mint].buys_h1 = dex_data.get("buys_h1", tokens[mint].buys_h1)
                                        tokens[mint].sells_h1 = dex_data.get("sells_h1", tokens[mint].sells_h1)
                                        tokens[mint].price_change_h1 = dex_data.get("price_change_h1", 0.0)
                                        tokens[mint].buy_volume_h1 = dex_data.get("buy_volume_h1", 0.0)
                                        tokens[mint].volume_m5 = dex_data.get("volume_m5", 0.0)
                                        # [v4.11] Vol history for acceleration detection
                                        bvol = dex_data.get("buy_volume_h1", 0.0)
                                        if bvol > 0:
                                            tokens[mint].vol_usd_history.append(bvol)
                                            if len(tokens[mint].vol_usd_history) > 8:
                                                tokens[mint].vol_usd_history = tokens[mint].vol_usd_history[-8:]
                                        tokens[mint].last_updated = time.time()
                                        # [v4.11] Track first MC for dump detection
                                        if tokens[mint].first_seen_mc == 0.0 and new_mc > 0:
                                            tokens[mint].first_seen_mc = new_mc
                                        # [v4.11] Track consecutive green polls
                                        if new_mc > tokens[mint].last_mc > 0:
                                            tokens[mint].consecutive_green_polls += 1
                                        elif new_mc < tokens[mint].last_mc * 0.95:
                                            tokens[mint].consecutive_green_polls = 0

                            # [fix] use live tokens[mint] not stale local copy
                            async with tokens_lock:
                                live_token = tokens.get(mint)
                            if live_token:
                                await run_detections(live_token, session)
                            await asyncio.sleep(0.2)  # [fix] rate limit: 300 tokens × 0.2s = 60s/cycle
                        except Exception as e:
                            logger.error(f"Detection error {mint[:8]}: {e}")

                    if len(tokens) > 0:
                        ws_tag = "🟢WS" if ws_stats["connected"] else "🔴WS"
                        logger.info(f"📊 Tracking {len(tokens)} tokens [{ws_tag} | {ws_stats['tokens_discovered']} WS-discovered]")

                    consecutive_errors = 0

                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Polling error #{consecutive_errors}: {e}")
                    if consecutive_errors > 5:
                        await asyncio.sleep(10)
                        consecutive_errors = 0
                    else:
                        await asyncio.sleep(2)

        finally:
            enrich_task.cancel()
            try:
                await enrich_task
            except asyncio.CancelledError:
                pass

# ============================================================================
# FastAPI
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _alert_queue, ws_discovery_queue

    logger.info("🚀 Starting ASTAROTH v4.11 (Anti-Dump | Per-Chain Discovery | ETH/BSC/BASE Improved)")
    logger.info(f"🔌 PumpPortal WS: {PUMPPORTAL_WS}")
    logger.info(f"📡 Fallback poll: {POLL_INTERVAL}s | Cap: {MAX_TRACKED_TOKENS} | Alert gap: {ALERT_RATE_LIMIT}s")

    load_state()
    await init_telegram()

    _alert_queue = asyncio.Queue(maxsize=50)
    ws_discovery_queue = asyncio.Queue(maxsize=500)
    global ws_sub_request_queue
    ws_sub_request_queue = asyncio.Queue(maxsize=100)  # [fix] trade sub requests

    load_kols()

    alert_task = asyncio.create_task(alert_worker())
    ws_task = asyncio.create_task(pumpfun_ws_listener())
    ws_sub_task = asyncio.create_task(ws_trade_subscription_manager())
    polling_task = asyncio.create_task(polling_loop())

    if _RUNNER_DETECTOR_AVAILABLE:
        logger.info("🏃 Runner detection enabled")

    # KOL polling runs inside its own session (shared with enrich worker via polling_loop)
    # We launch it as a standalone task using a fresh session
    async def _kol_task_wrapper():
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as kol_session:
            await kol_polling_loop(kol_session)

    kol_task = asyncio.create_task(_kol_task_wrapper())
    logger.info(f"📋 KOL polling started ({len(kol_accounts)} accounts)")

    active_chains = ", ".join(
        f"{c['emoji']}{c['label']}" + (" ⚡WS" if c["has_ws"] else " 📡Poll")
        for c in CHAINS.values() if c["enabled"]
    )
    kol_line = f"{len(kol_accounts)} accounts" if kol_accounts else "none — /addkol @handle"
    await send_telegram_now(
        "🚀 <b>ASTAROTH</b> online\n"
        f"{active_chains}\n"
        f"👁 KOLs: {kol_line}"
    )

    logger.info("✅ All tasks started")
    yield

    for task in [polling_task, ws_task, ws_sub_task, alert_task, kol_task]:
        task.cancel()
    for task in [polling_task, ws_task, ws_sub_task, alert_task, kol_task]:
        try:
            await task
        except asyncio.CancelledError:
            pass

    _last_snapshot_time = 0.0
    await save_state()
    logger.info("👋 Stopped")


app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "ok", "tokens": len(tokens), "version": "4.11", "ws": ws_stats["connected"]}

@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"alive": True, "tokens": len(tokens), "ws": ws_stats["connected"]}

@app.get("/status")
async def status():
    return {
        "version": "4.11",
        "tokens": len(tokens),
        "token_cap": MAX_TRACKED_TOKENS,
        "gems_alerted": sum(1 for t in tokens.values() if t.alerted),
        "ws": ws_stats,
        "alert_queue": _alert_queue.qsize() if _alert_queue else 0,
        "mode": "websocket+polling",
        "runner_detection": _RUNNER_DETECTOR_AVAILABLE,
    }

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return build_dashboard_html()


@app.get("/analysis")
async def analysis(limit: int = 100, alerted_only: bool = False, chain: str = ""):
    """
    [v4.12] Near-miss scoring visibility.
    Returns the last `limit` evaluated tokens (capped at ANALYSIS_RING_SIZE=500).
    Shows composite gate results and skip reasons so you can tune thresholds.
    Query params:
      - limit: how many records to return (default 100)
      - alerted_only: if true, only return tokens that fired an alert
      - chain: filter by chain_id (e.g. 'solana', 'ethereum')
    """
    records = list(_analysis_ring)
    if alerted_only:
        records = [r for r in records if r.get("alerted")]
    if chain:
        records = [r for r in records if r.get("chain") == chain]
    records = records[-min(limit, ANALYSIS_RING_SIZE):]
    records.reverse()  # most recent first

    # Near-miss distribution: group by first failing gate
    skip_counts: dict = {}
    for r in records:
        if not r.get("alerted"):
            key = r.get("skip", "unknown")
            # Normalise to gate name only (strip numbers)
            if key.startswith("age"): key = "age"
            elif key.startswith("MC"): key = "mc"
            elif key.startswith("vol/MC"): key = "vol_mc_ratio"
            elif key.startswith("liq"): key = "liq"
            elif key.startswith("buy_ratio"): key = "buy_ratio"
            elif key.startswith("buys_h1"): key = "buys_h1"
            skip_counts[key] = skip_counts.get(key, 0) + 1

    return JSONResponse({
        "ring_size": ANALYSIS_RING_SIZE,
        "total_in_ring": len(list(_analysis_ring)),
        "returned": len(records),
        "alerted_count": sum(1 for r in records if r.get("alerted")),
        "near_miss_distribution": dict(sorted(skip_counts.items(), key=lambda x: -x[1])),
        "records": records,
    })

@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    try:
        data = await request.json()
        message = data.get("message") or data.get("edited_message")
        if message:
            text = message.get("text", "")
            if text.startswith("/"):
                asyncio.create_task(handle_telegram_command(text))
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        return JSONResponse({"ok": False})

# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")