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
import base64
import html
import time
import logging
import re
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
import database as alert_db

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

# [v4.24] Simple env-var helpers so detection knobs below can be retuned via
# Railway env vars without a code change + redeploy.
def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() == "true"

def _env_num(name: str, default, cast=float):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return cast(v)
    except (TypeError, ValueError):
        return default

# [v4.24] Per-request rate limiting on all HTTP endpoints (dashboard, /analysis,
# the Telegram webhook). RATE_LIMIT_PER_MIN overrides the default of 120/min/IP.
rate_limiter = RateLimiter(
    max_requests=int(_env_num("RATE_LIMIT_PER_MIN", 120, int)),
    window_seconds=60,
)

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex"
# [v4.43] GoPlus Security API — free, no key required, used to screen EVM
# tokens for honeypot/scam red flags before they go out in an alert.
GOPLUS_API = "https://api.gopluslabs.io/api/v1"
DEXSCREENER_NEW_PAIRS = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_SEARCH = "https://api.dexscreener.com/latest/dex/search"
DEXSCREENER_BOOSTS = "https://api.dexscreener.com/token-boosts/latest/v1"
# [v4.11] Chain-specific new pairs endpoints — sorted by pairCreatedAt desc
# These return recently created trading pairs per chain, far better than generic search
# DexScreener search queries per chain — returns pairs sorted by recent activity
# /latest/dex/search?q={term} is the correct free-tier endpoint
# We search generic terms to get recently active pairs, then filter by chain client-side
DEXSCREENER_NEW_PAIRS_BY_CHAIN = {
    "ethereum":  "https://api.dexscreener.com/latest/dex/search?q=eth",
    "bsc":       "https://api.dexscreener.com/latest/dex/search?q=bsc",
    "base":      "https://api.dexscreener.com/latest/dex/search?q=base",
    # [v4.37] Robinhood Chain — dexscreener.com/robinhood confirms this slug
    # is indexed (verified 2026-08-29); free-text search on the chain name
    # mirrors the pattern already used for the other three chains above.
    "robinhood": "https://api.dexscreener.com/latest/dex/search?q=robinhood",
}
# How often to run the dedicated per-chain poll (seconds)
CHAIN_POLL_INTERVAL = {
    "ethereum":  30,   # ETH is slower — new pairs every 30s is fine
    "bsc":       20,   # BSC is faster
    "base":      20,   # Base is fast
    "robinhood": 20,   # New, high-volume meme activity — same cadence as BSC/Base
}

# [v4.25] Alchemy WS — push-based new-pair detection for BSC/Base/Ethereum.
# One free Alchemy account/API key works across all three (Solana keeps its
# existing PumpPortal WS, which is purpose-built for pump.fun and stays as-is).
# Leave ALCHEMY_API_KEY unset to keep these three chains on DexScreener polling
# only — nothing below changes behavior until a key is configured.
#
# Alchemy apps are network-scoped in the dashboard — a key only works on the
# networks enabled for that specific app (a wrong/un-enabled network fails
# WS auth with HTTP 403, not the "invalid key" 401 you'd expect from a bad
# key). If one chain 403s while another with the same key works, that's the
# fix: open the app in the Alchemy dashboard and add the missing network to
# it. As an alternative (e.g. separate apps per chain), an optional
# per-chain override is supported: ALCHEMY_API_KEY_BSC / _BASE / _ETHEREUM,
# each falling back to ALCHEMY_API_KEY if unset.
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "").strip()

def _alchemy_key_for(chain_id: str) -> str:
    return os.getenv(f"ALCHEMY_API_KEY_{chain_id.upper()}", "").strip() or ALCHEMY_API_KEY

ALCHEMY_WS_URLS = {
    "ethereum":  f"wss://eth-mainnet.g.alchemy.com/v2/{_alchemy_key_for('ethereum')}",
    "bsc":       f"wss://bnb-mainnet.g.alchemy.com/v2/{_alchemy_key_for('bsc')}",
    "base":      f"wss://base-mainnet.g.alchemy.com/v2/{_alchemy_key_for('base')}",
    # [v4.37] Robinhood Chain — Arbitrum Orbit L2, chain id 4663, confirmed
    # live on Alchemy 2026-07 (per Alchemy's own "Robinhood Chain Mainnet Is
    # Live" post). Same per-chain-key override pattern: ALCHEMY_API_KEY_ROBINHOOD.
    "robinhood": f"wss://robinhood-mainnet.g.alchemy.com/v2/{_alchemy_key_for('robinhood')}",
}

# keccak256 topic0 hashes — identical across every V2/V3-style fork since the
# event signatures never change. Verified against live BscScan/Etherscan logs.
# [v4.30 CRITICAL FIX] Both constants below were truncated by one trailing hex
# character (63 hex chars instead of the required 64 = 32 bytes) — an invalid
# topic0 can never equal a real on-chain log's topic0 (log_topics[0].lower()
# is always exactly 64 hex chars), so PancakeV2/V3 (BSC) and UniswapV2/V3
# (Ethereum + Base) WS discovery has been silently dead on arrival: every
# factory log using these two topics failed the `topic0 == ...` check and was
# dropped, forcing those chains onto slow DexScreener-polling-only discovery
# the whole time. Only Aerodrome on Base (a distinct, correctly-length topic)
# was ever actually reaching WS discovery. Recomputed via keccak256 of the
# canonical signatures and independently confirmed against multiple real
# Etherscan/BscScan/PolygonScan/BaseScan transaction logs carrying these exact
# corrected hashes as their topic0.
_PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"  # PairCreated(address,address,address,uint256)
_POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"  # PoolCreated(address,address,uint24,int24,address)
# [v4.28] Solidly-fork PoolCreated (Aerodrome on Base, Velodrome-style forks
# elsewhere) — different signature than Uniswap V3's PoolCreated (adds an
# indexed `stable` bool, drops fee/tickSpacing) so it needs its own topic0.
# Computed via keccak256("PoolCreated(address,address,bool,address,uint256)")
# and cross-verified against a live PoolCreated log on BaseScan
# (tx 0x72e7300690df07a176ac65da67b86af0f744db818968f9ee445e3b89410ab344).
_SOLIDLY_POOL_CREATED_TOPIC = "0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e"

# Factory contracts to watch per chain. Addresses verified against BscScan /
# Etherscan / BaseScan on 2026-08-22 (SushiSwap entries added/verified
# 2026-08-25) — these are long-lived, canonical deployments and shouldn't
# need updating, but if a chain silently stops producing WS discoveries,
# check here first (a DEX may have shipped a new factory version since).
EVM_FACTORIES = {
    "bsc": [
        {"address": "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73", "topic": _PAIR_CREATED_TOPIC, "dex": "PancakeV2"},
        {"address": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865", "topic": _POOL_CREATED_TOPIC, "dex": "PancakeV3"},
    ],
    "ethereum": [
        {"address": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f", "topic": _PAIR_CREATED_TOPIC, "dex": "UniswapV2"},
        {"address": "0x1F98431c8aD98523631AE4a59f267346ea31F984", "topic": _POOL_CREATED_TOPIC, "dex": "UniswapV3"},
        # [v4.30] SushiSwap — a straight Uniswap V2/V3 fork so it reuses the
        # same topics. Addresses cross-checked against Etherscan's own labels
        # ("SushiSwap: SushiV2Factory" / "SushiSwap V3: Factory").
        {"address": "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac", "topic": _PAIR_CREATED_TOPIC, "dex": "SushiV2"},
        {"address": "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F", "topic": _POOL_CREATED_TOPIC, "dex": "SushiV3"},
    ],
    "base": [
        {"address": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD", "topic": _POOL_CREATED_TOPIC, "dex": "UniswapV3"},
        # [v4.28] Aerodrome is Base's dominant DEX by volume/TVL — it was
        # entirely unwatched before, which was a real chunk of why Base
        # discovery lagged Solana's. Verified factory address + topic0
        # against a live BaseScan PoolCreated log (see topic comment above).
        {"address": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da", "topic": _SOLIDLY_POOL_CREATED_TOPIC, "dex": "Aerodrome"},
        # [v4.30] SushiSwap V3 on Base — same Uniswap V3 fork, same topic0.
        # Address cross-checked against BaseScan's own label ("SushiSwap V3: Factory").
        {"address": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4", "topic": _POOL_CREATED_TOPIC, "dex": "SushiV3"},
    ],
    # [v4.37] Robinhood Chain (chain id 4663) — new Arbitrum Orbit L2, live
    # since ~2026-07. Only Uniswap V3 verified so far, address sourced
    # directly from Uniswap's own developer deployments doc
    # (developers.uniswap.org/docs/protocols/v3/deployments/v3-robinhood-chain-deployments,
    # checked 2026-08-29) — no V2 factory is listed there, and the chain's
    # active meme-launchpads (robinhood.fun, NOXA Fun, Uniswap's own
    # Pools.trade) are NOT covered here; those use their own bonding-curve
    # contracts, not this factory, and would need the same kind of dedicated
    # decode work four.meme got (deliberately out of scope for this pass —
    # see the scope note in .env.example).
    "robinhood": [
        {"address": "0x1f7d7550b1b028f7571e69a784071f0205fd2efa", "topic": _POOL_CREATED_TOPIC, "dex": "UniswapV3"},
    ],
}

# ============================================================================
# [v4.35] four.meme -- BSC's dominant pump.fun-style bonding-curve launchpad.
# ============================================================================
# The gap this closes: our only BSC discovery was PancakeV2/V3 pair-creation
# events, which only fire once a token MIGRATES off a bonding curve onto a
# real Pancake pair -- exactly like pump.fun tokens don't hit a DexScreener
# pair until graduation. A large share of BSC "runner" activity happens
# *during* the four.meme bonding-curve phase, entirely invisible to that
# factory watch. This adds a second, address-filtered logs subscription on
# the SAME Alchemy BSC WS connection for TokenManager2's TokenCreate /
# TokenPurchase / TokenSale events.
#
# Contract address and event ABI cross-verified across three independent
# sources before writing a single line of decode logic (same rigor as the
# earlier topic0 fix this session, precisely because this failure mode is
# silent): Bitquery's four.meme API docs, a QuickNode BSC copytrading guide,
# and -- most authoritative -- the four-meme-community/fourmeme-docs GitHub
# repo's own machine-generated ABI JSON (abi/TokenManager2.lite.json) and
# Solidity interface (contracts/interfaces/ITokenManager2.sol), which
# explicitly show "indexed": false / no `indexed` keyword on every single
# event parameter -- meaning NONE of these events can be filtered by topic
# beyond topic0; every field must be decoded from `data`. Topic0 hashes were
# then independently recomputed here via keccak256 of the canonical
# signatures and matched byte-for-byte against the value reported by two of
# the three sources above.
#
# What this deliberately does NOT do: estimate a real USD market cap from
# four.meme's `price` field. Their own docs explicitly scope out "bonding-
# curve algorithms" as proprietary/undocumented, so there's no verified
# fixed-point convention to trust (unlike Solana pump.fun, where a published
# IDL make computing a real MC possible). Instead it tracks cumulative BNB
# raised (TokenInfo.fourmeme_raised_usd) as a curve-progress proxy, feeding
# a separate CURVE🌊 alert path in run_detections that never touches or
# mixes with the real-dollar-MC-gated paths every other signal uses.
FOURMEME_TOKEN_MANAGER2 = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"  # BSC, TokenManager2 (V2 -- current launches)
# keccak256("TokenCreate(address,address,uint256,string,string,uint256,uint256,uint256)")
_FOURMEME_CREATE_TOPIC = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"
# keccak256("TokenPurchase(address,address,uint256,uint256,uint256,uint256,uint256,uint256)")
_FOURMEME_PURCHASE_TOPIC = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"
# keccak256("TokenSale(address,address,uint256,uint256,uint256,uint256,uint256,uint256)")
_FOURMEME_SALE_TOPIC = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"

# FOURMEME_MODE mirrors PUMPFUN_DIRECT_MODE's off/shadow/live rollout pattern
# from earlier this session -- same reasoning: a brand-new decoder for a
# numeric proxy (fourmeme_raised_usd) that can't be verified against live
# chain data from this environment shouldn't default to feeding real alerts.
#   off    (default) -- four.meme subscription not started. Zero behavior change.
#   shadow -- decodes and logs CREATE/BUY/SELL events (see fourmeme_stats via
#             /status) but does not touch `tokens` or fire alerts. Use this
#             to sanity-check decoded symbols/names/raised-amounts against
#             the real four.meme.com site or BscScan before trusting it.
#   live   -- creates a real pre-enrichment TokenInfo stub per new token,
#             tracks live buy/sell pressure, and can fire the CURVE🌊 path.
FOURMEME_MODE = os.getenv("FOURMEME_MODE", "off").strip().lower()
FOURMEME_GRADUATION_BNB = 24.0  # fixed platform raise target (documented, not customizable)
FOURMEME_MIN_RAISED_USD = float(os.getenv("FOURMEME_MIN_RAISED_USD", "2000"))
FOURMEME_MAX_RAISED_USD = float(os.getenv("FOURMEME_MAX_RAISED_USD", "12000"))
fourmeme_stats = {
    "connected": False, "creates_decoded": 0, "purchases_decoded": 0,
    "sales_decoded": 0, "decode_errors": 0, "tokens_tracked": 0, "curve_alerts": 0,
}
fourmeme_sub_id: Dict[str, Optional[str]] = {}  # only "bsc" key ever used

# The "other side" of a pair — when a factory event fires, whichever token is
# NOT one of these is the new listing. If neither side matches (two unknown
# tokens paired together) we fall back to token0 as a best-effort guess.
EVM_BASE_TOKENS = {
    "bsc": {
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
        "0x55d398326f99059ff775485246999027b3197955",  # USDT (BSC-USD)
        "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    },
    "ethereum": {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    },
    "base": {
        "0x4200000000000000000000000000000000000006",  # WETH
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC
    },
    # [v4.37] Robinhood Chain. WETH address confirmed from Robinhood's own
    # contracts doc (docs.robinhood.com/chain/contracts, checked 2026-08-29).
    # Deliberately NOT including USDG (Global Dollar) here yet — it's a real,
    # confirmed base asset on this chain (see the USDG/WETH Uniswap V3 pool
    # on GeckoTerminal), but its decimals() couldn't be verified from this
    # environment (Robinhood's public RPC isn't on the egress allowlist) and
    # guessing wrong would silently corrupt USD volume math for every
    # USDG-quoted pair. A meme/USDG pair still gets pre-tracked fine (pair
    # discovery doesn't depend on base-token recognition) — it just falls
    # into the existing "neither side recognized" best-effort bucket instead
    # of getting priced. Add USDG here (+ to EVM_BASE_TOKEN_DECIMALS) once
    # its decimals are confirmed on-chain or from Paxos's own docs.
    # Also note: several meme pairs on this chain quote against tokenized
    # stock tokens (NVDA, COIN, SPY, PLTR, HIMS, ...) instead of WETH/USDG —
    # those aren't tracked as recognized base assets either, since pricing
    # them would need a live stock price feed, not a crypto price.
    "robinhood": {
        "0x0bd7d308f8e1639fab988df18a8011f41eacad73",  # WETH
    },
}

WS_EVM_RECONNECT_DELAY_MIN = 2
WS_EVM_RECONNECT_DELAY_MAX = 60

# [v4.26] EVM push-based trade/volume feed. Mirrors the Solana WS trade model
# (ws_buy_count/ws_sell_count/ws_buy_vol_usd/ws_sell_vol_usd on TokenInfo) —
# those fields already exist and are already read by is_buy_vol_significant /
# is_buy_vol_accelerating / the eth_vol_signal & eth_vol_accel alert paths, but
# were dead code for EVM chains until now because nothing populated them.
#
# Approach: subscribe to Swap events (V2 and V3 event shapes) on the pair/pool
# contracts of currently-tracked tokens, over the SAME Alchemy WS connection
# used for pair discovery. Topic0 hashes verified two ways: computed locally
# via keccak256 of the canonical event signatures, and cross-checked against
# live BaseScan/PolygonScan/Arbiscan/OP-Etherscan transaction logs.
_V2_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"  # Swap(address,uint256,uint256,uint256,uint256,address)
_V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"  # Swap(address,address,int256,int256,uint160,uint128,int24)

EVM_MAX_SWAP_SUBS = int(os.getenv("EVM_MAX_SWAP_SUBS", "150"))  # cap on pairs subscribed per chain
EVM_SWAP_RESUB_INTERVAL = 20  # seconds between subscription-list refreshes

# Decimals for each known base/quote token, needed to turn a raw on-chain
# integer swap amount into a real quantity. Wrapped-native tokens (WBNB/WETH)
# are converted to USD via a static, env-overridable price constant — same
# pattern already used for SOL (WS_SOL_PRICE_USD) — rather than a live oracle.
WS_BNB_PRICE_USD = float(os.getenv("BNB_PRICE_USD", "600.0"))
WS_ETH_PRICE_USD = float(os.getenv("ETH_PRICE_USD", "3000.0"))

EVM_BASE_TOKEN_DECIMALS = {
    "bsc": {
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": 18,  # WBNB
        "0x55d398326f99059ff775485246999027b3197955": 18,  # USDT (BSC-USD)
        "0xe9e7cea3dedca5984780bafc599bd69add087d56": 18,  # BUSD
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": 18,  # USDC
    },
    "ethereum": {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 18,  # WETH
        "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,   # USDT
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,    # USDC
    },
    "base": {
        "0x4200000000000000000000000000000000000006": 18,  # WETH
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": 6,     # USDC
    },
    "robinhood": {
        "0x0bd7d308f8e1639fab988df18a8011f41eacad73": 18,  # WETH — standard wrapped-native decimals
    },
}

# Which base tokens are wrapped-native (priced via WS_*_PRICE_USD) vs
# stablecoins (priced at a flat $1) — everything not listed here is a stable.
_EVM_NATIVE_WRAPPED = {
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": "bnb",   # WBNB
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "eth",   # WETH (ethereum)
    "0x4200000000000000000000000000000000000006": "eth",  # WETH (base)
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73": "eth",   # WETH (robinhood)
}


def _evm_base_usd_price(base_addr: str) -> float:
    kind = _EVM_NATIVE_WRAPPED.get(base_addr)
    if kind == "bnb":
        return WS_BNB_PRICE_USD
    if kind == "eth":
        return WS_ETH_PRICE_USD
    return 1.0  # stablecoin side — flat $1 peg estimate


def _evm_amount_to_usd(chain_id: str, base_addr: str, raw_amount: int) -> float:
    decimals = EVM_BASE_TOKEN_DECIMALS.get(chain_id, {}).get(base_addr, 18)
    qty = raw_amount / (10 ** decimals)
    return qty * _evm_base_usd_price(base_addr)


def _hex_word_to_signed_int(word_hex: str) -> int:
    """Decode a 32-byte (64 hex char) word as a two's-complement signed int256."""
    val = int(word_hex, 16)
    if val >= (1 << 255):
        val -= (1 << 256)
    return val

# [v4.9] Multi-chain support
# Each chain: enabled, has_ws (PumpPortal for Solana, Alchemy for the rest —
# only on if ALCHEMY_API_KEY is set), dexscreener chain id, explorer base url
#
# [v4.30] Solana disabled by default — all tracking effort/capacity goes to
# BSC/Base/Ethereum instead. This is a config flip, not a code removal: every
# Solana code path (PumpPortal WS, the direct pump.fun indexer, squat-guard
# dedup) is still here and fully working, just never started while this is
# off. Set ENABLE_SOLANA=true (env var) to bring it back with no code change.
CHAINS: Dict[str, dict] = {
    "solana": {
        "enabled": _env_bool("ENABLE_SOLANA", False),
        "has_ws": True,
        "chain_id": "solana",
        "explorer": "https://dexscreener.com/solana/{}",
        "label": "SOL",
        "emoji": "◎",
    },
    "bsc": {
        "enabled": _env_bool("ENABLE_BSC", True),
        "has_ws": bool(_alchemy_key_for("bsc")),
        "chain_id": "bsc",
        "explorer": "https://dexscreener.com/bsc/{}",
        "label": "BSC",
        "emoji": "🟡",
    },
    "base": {
        "enabled": _env_bool("ENABLE_BASE", True),
        "has_ws": bool(_alchemy_key_for("base")),
        "chain_id": "base",
        "explorer": "https://dexscreener.com/base/{}",
        "label": "BASE",
        "emoji": "🔵",
    },
    "ethereum": {
        "enabled": _env_bool("ENABLE_ETH", True),
        "has_ws": bool(_alchemy_key_for("ethereum")),
        "chain_id": "ethereum",
        "explorer": "https://dexscreener.com/ethereum/{}",
        "label": "ETH",
        "emoji": "🔷",
    },
    # [v4.37] Robinhood Chain — Arbitrum Orbit L2 (chain id 4663), live since
    # ~2026-07. Uniswap-V3-pairs coverage only for this pass (see EVM_FACTORIES
    # comment) — no bonding-curve launchpad coverage yet.
    "robinhood": {
        "enabled": _env_bool("ENABLE_ROBINHOOD", True),
        "has_ws": bool(_alchemy_key_for("robinhood")),
        "chain_id": "robinhood",
        "explorer": "https://dexscreener.com/robinhood/{}",
        "label": "RH",
        "emoji": "🪶",
    },
}

# [v4.10] Per-chain detection thresholds
# Each chain has different token lifecycle, MC ranges, and trading behaviour.
# [v4.24] Every value below is overridable via env var (e.g. SOL_MC_MIN=15000)
# so retuning a chain no longer requires a code change + redeploy — the
# literal below each one is just the default when no env var is set.
def _chain_thresholds(prefix: str, age_min, age_max, mc_min, mc_max,
                       vol_min, vol_max, liq_min, buy_ratio_min, min_buys_h1) -> dict:
    return {
        "age_min":       _env_num(f"{prefix}_AGE_MIN", age_min, int),
        "age_max":       _env_num(f"{prefix}_AGE_MAX", age_max, int),
        "mc_min":        _env_num(f"{prefix}_MC_MIN", mc_min, float),
        "mc_max":        _env_num(f"{prefix}_MC_MAX", mc_max, float),
        # [v4.39] Replaces the old vol_mc_ratio (a floor-only vol/mc PERCENTAGE
        # gate) with an absolute-dollar volume BAND — a token must show
        # between vol_min and vol_max of real dollar volume, independent of
        # its exact mc within the mc_min..mc_max window. See CHAIN_THRESHOLDS
        # below for the concrete per-chain numbers.
        "vol_min":       _env_num(f"{prefix}_VOL_MIN", vol_min, float),
        "vol_max":       _env_num(f"{prefix}_VOL_MAX", vol_max, float),
        "liq_min":       _env_num(f"{prefix}_LIQ_MIN", liq_min, float),
        "buy_ratio_min": _env_num(f"{prefix}_BUY_RATIO_MIN", buy_ratio_min, float),
        "min_buys_h1":   _env_num(f"{prefix}_MIN_BUYS_H1", min_buys_h1, int),
    }

# [v4.28] Narrowed to a $10k-$50k MC catch window on every chain — this bot
# is meant to catch tokens early, before they've already pumped past the
# point where getting in is still worth it. (Started at a $30k ceiling;
# raised to $50k after live data showed the tighter window intersected too
# hard with the vol/buy-ratio/min-buys gates — too few tokens had time to
# clear all of them before aging out of range.) mc_min/mc_max stay per-chain
# env overridable (SOL_MC_MIN/SOL_MC_MAX etc.) in case one chain needs retuning.
#
# [v4.30] With Solana disabled, ALERT_RATE_LIMIT (global 30s gap) and
# GEM_COOLDOWN (global 5min per-token re-alert) are no longer shared with by
# far the highest-volume/highest-alert-frequency chain — BSC/Base/Ethereum
# now have the whole alert budget to themselves. Loosened buy_ratio_min
# (0.52 -> 0.48, same relative move as the v4.8 0.60 -> 0.52 change) and
# min_buys_h1 for bsc/base (8 -> 6) to take advantage of that freed room —
# these two chains can now afford a slightly less-proven signal without
# risking getting crowded out by SOL noise, since there isn't any anymore.
# Ethereum's min_buys_h1 stays at 3 (already the lowest — ETH's low tx
# velocity means further lowering risks quality, not just volume) and
# solana's thresholds are untouched/dormant while ENABLE_SOLANA=false.
#
# [v4.34] "Balanced" tuning pass, aimed at catching more runners without
# gutting the anti-dump/distro safety nets. Reasoning: with Solana back on
# and the new RUNNER momentum path (below) absorbing anything that outgrows
# this window, these thresholds only gate the "catch it cheap" phase, so
# there's real room to relax them a notch further than v4.30 did:
#   - buy_ratio_min: -0.03 across the board (0.52->0.50 solana, 0.48->0.45
#     bsc/base/eth) — still requires clear buy dominance, just not quite as
#     strict, so tokens qualify a little earlier in their run.
#   - min_buys_h1: -1 to -2 per chain (solana 10->8, bsc/base 6->5, eth
#     3->2) — eth stays the most conservative since its low tx velocity
#     makes small buy counts less statistically meaningful, but 2 vs 3 is a
#     minor move, not a quality cliff.
#   - vol_mc_ratio: ~20-25% relative reduction per chain — the vol/MC ratio
#     gate was tuned when the catch window was narrower; a slightly lower
#     bar still filters genuinely dead tokens while not excluding early
#     movers whose volume hasn't caught up to their MC yet.
# All still per-chain env-overridable if any of these need dialing back.
#
# [v4.39] Explicit request: replace the mc_max=$50k-for-everyone / vol_mc_ratio
# percentage-floor design with (a) per-chain-family MC windows and (b) an
# absolute-dollar volume band, and stop tracking a token at all unless its
# FIRST observed data already lands inside both — no pre-track stub, no
# re-checking a token later if it was outside the window the first time it
# was seen (see the entry-gate comments at each tokens[mint] = TokenInfo(...)
# creation site). Concretely:
#   - solana:   MC $10k-$30k,  volume $5k-$30k
#   - bsc/base/ethereum/robinhood (all EVM chains): MC $5k-$50k, volume $5k-$50k
# Both are per-chain env-overridable (SOL_MC_MIN/SOL_VOL_MAX/BSC_MC_MIN/etc.)
# same as every other threshold here.
# [v4.44] vol_min lowered from $10k to $5k across every chain, per user
# request. See effective_liq_vol_buyratio for the matching change to what
# "volume" itself means here (buy-side only now, not total buy+sell).
CHAIN_THRESHOLDS = {
    "solana":    _chain_thresholds("SOL",  3 * 60,  6 * 3600, 10_000, 30_000,  5_000, 30_000, 5_000, 0.50, 8),
    "bsc":       _chain_thresholds("BSC",  2 * 60, 12 * 3600,  5_000, 50_000,  5_000, 50_000, 5_000, 0.45, 5),
    "base":      _chain_thresholds("BASE", 3 * 60,  6 * 3600,  5_000, 50_000,  5_000, 50_000, 5_000, 0.45, 5),
    "ethereum":  _chain_thresholds("ETH",  2 * 60, 24 * 3600,  5_000, 50_000,  5_000, 50_000, 5_000, 0.45, 2),
    # [v4.37] Robinhood Chain — brand new, no live tuning history yet. Starts
    # as a copy of BSC's thresholds (same "fast, high meme volume" profile
    # DexScreener's own numbers for this chain suggest) — fully
    # env-overridable (RH_MC_MIN, RH_AGE_MAX, etc.) once real data comes in.
    "robinhood": _chain_thresholds("RH",   2 * 60, 12 * 3600,  5_000, 50_000,  5_000, 50_000, 5_000, 0.45, 5),
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
# [v4.26] If that first fetch comes back empty (DexScreener hasn't indexed the
# pair yet — common on brand-new pairs, and common enough on EVM chains that
# the new Alchemy WS discovery would otherwise mostly finalize as empty
# stubs), retry in the background at these additional delays before giving up.
# Without this, a token's liquidity/volume/buys got permanently locked at
# zero on a single miss and could never pass any alert threshold again.
WS_ENRICH_RETRY_DELAYS = [7, 15, 30]
WS_SOL_PRICE_USD = float(os.getenv("SOL_PRICE_USD", "175.0"))  # used to convert WS SOL vol → USD

# ============================================================================
# [v4.29] Direct on-chain pump.fun indexing — bypasses the PumpPortal relay
# ============================================================================
# PumpPortal is itself just a third party reading pump.fun's on-chain Anchor
# program and republishing it as JSON over WS. This reads the same program
# directly via Helius's logsSubscribe, decoding the raw Anchor event logs
# ourselves. Two upsides over PumpPortal: (1) one less hop = lower latency,
# (2) PumpPortal only streams trades for up to WS_MAX_TRADE_SUBS (50)
# explicitly-subscribed mints — direct indexing gets creates *and* trades for
# every single pump.fun token, no subscription cap.
#
# PUMPFUN_DIRECT_MODE:
#   off    (default) — direct listener disabled entirely; PumpPortal WS is the
#                       only feed. No behavior change from before this exists.
#   shadow — direct listener runs alongside PumpPortal, decodes events and logs
#            stats, but does NOT feed tokens/trades into the bot. Use this to
#            verify the decoder is producing sane data against PumpPortal's
#            parallel feed in the live logs before trusting it.
#   live   — direct listener replaces PumpPortal entirely as the sole
#            discovery/trade feed (PumpPortal WS + its trade-sub manager are
#            not started, avoiding double-counted creates/trades).
PUMPFUN_DIRECT_MODE = os.getenv("PUMPFUN_DIRECT_MODE", "off").strip().lower()
PUMPFUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
HELIUS_WS = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""
# Anchor 8-byte event discriminators, from pump.fun's official IDL
# (https://github.com/pump-fun/pump-public-docs) — sha256("event:<Name>")[:8].
_CREATE_EVENT_DISC = bytes([27, 114, 169, 77, 222, 235, 99, 118])
_TRADE_EVENT_DISC = bytes([189, 219, 127, 211, 78, 230, 97, 238])
# Standard pump.fun bonding-curve total supply (1B tokens @ 6 decimals). Used
# as a fallback for trades on tokens whose CreateEvent we never saw (listener
# connected after launch) — TradeEvent doesn't carry token_total_supply, only
# CreateEvent does, so this is the best available default.
_PUMPFUN_DEFAULT_SUPPLY = 1_000_000_000 * 10 ** 6

POLL_INTERVAL = 15            # [v4.5] Reduced — WS handles discovery now
# [v4.30] Bumped 30 -> 50 now that BSC/Base/ETH are the only chains getting
# per-chain dedicated-search budget (see fetch_chain_new_pairs) — no reason to
# hold back page depth per chain when there are only 3 to cover instead of 4.
TOKENS_PER_POLL = 50
# [v4.30] How deep into DexScreener's ALL-CHAINS combined new-pairs feed to
# scan before filtering down to our enabled chains. See fetch_new_tokens() for
# why this must scale up, not down, as fewer chains are enabled — the profile
# feed's global volume doesn't shrink just because we stopped tracking Solana.
NEW_PAIRS_SCAN_DEPTH = int(os.getenv("NEW_PAIRS_SCAN_DEPTH", "400"))

# Gem detection thresholds
GEM_AGE_MIN = 3 * 60           # 3 min — catch tokens as early as possible
GEM_AGE_MAX = 6 * 3600         # 6 hours — runners can sustain longer than 90 min
GEM_MC_MIN = 10_000            # lower floor — catch early before MC pumps
GEM_MC_MAX = 50_000            # [v4.28] narrowed catch window — only alert 10k-50k MC
GEM_VOL_MC_RATIO = 0.15        # lowered — early tokens have lower vol/MC
GEM_LIQUIDITY_MIN = 5_000      # lowered — young tokens have less liq
GEM_COOLDOWN = 300             # 5 min cooldown (was 10) — faster re-alert on runners

# [v4.8] Buy pressure thresholds
GEM_BUY_RATIO_MIN = 0.52       # lowered from 0.60 — less strict buy dominance required
GEM_MIN_BUYS_H1 = 10           # lowered from 30 — catch early momentum
GEM_WS_BUY_PRESSURE = 1.2     # lowered — alert sooner on buy pressure
GEM_FAST_VELOCITY = 15.0       # lowered from 50% — easier to trigger fast-velocity path
GEM_FAST_MC_MIN = 10_000       # [v4.28] fast-velocity floor aligned to the 10k-50k catch window
GEM_VOL_ACCEL_MC_MAX = 50_000  # [v4.28] vol-accel ceiling aligned to the 10k-50k catch window

# [v4.34] Every path above requires mc <= mc_max (the ~$50k "catch it cheap"
# ceiling) — which structurally means NONE of them can ever fire on a token
# that already broke past that ceiling while running, no matter how strong
# its current buy pressure is. That's backwards for a bot whose whole job is
# to catch runners: a real runner, by definition, often blows through $50k
# in its first minute or two -- faster than buy_ratio/min_buys/vol_ratio can
# accumulate enough data to clear their gates. This is a SEPARATE, additive
# path (see `momentum_running` in run_detections) for tokens already above
# mc_max but still showing strong, currently-accelerating buy pressure -- it
# does not touch or loosen any existing path, it just stops silently
# dropping the tokens all the other paths are structurally blind to.
# GEM_MOMENTUM_MC_MAX is a sanity ceiling, not a "still cheap" window --
# picked high enough to not exclude a real still-running token, low enough
# to not alert on tokens that are just large/established (RESURGENCE_MODE
# already covers "established token regaining volume" as its own category).
GEM_MOMENTUM_MC_MAX = float(os.getenv("GEM_MOMENTUM_MC_MAX", "2000000"))  # $2M sanity ceiling

# [v4.41] Kill switch for the RUNNER🚀 path (momentum_running below), off by
# default per user request. Left as an env toggle rather than deleting the
# path outright so it can be turned back on later without a code change.
RUNNER_ENABLED = _env_bool("RUNNER_ENABLED", False)

# [v4.36] FRESH🌱 path — real on-chain buy pressure on a vanilla EVM DEX pair
# (PancakeV2/V3, Uniswap, Sushi, Aerodrome) that DexScreener hasn't indexed
# yet. See the pre-track stub in evm_ws_listener and the matching gate in
# run_detections. No liquidity/MC data exists for these stubs (no proxy like
# a bonding curve's raised amount — a PairCreated event carries no reserves),
# so this requires a real dollar floor and real trade-count floor on the raw
# WS-derived swap counters instead, to keep it from firing on dust.
EVM_FRESH_MIN_TRADES = int(os.getenv("EVM_FRESH_MIN_TRADES", "8"))
EVM_FRESH_MIN_USD_VOL = float(os.getenv("EVM_FRESH_MIN_USD_VOL", "1000"))

MULTIPLIER_MILESTONES = [2.0, 3.0, 5.0, 10.0]

# Token pool management
# [v4.30] This cap is shared across all enabled chains, not per-chain — with
# Solana disabled (by far the highest-churn chain, it was likely consuming
# most of this pool on its own) BSC/Base/ETH now get effectively the whole
# 750-slot budget to themselves with no code change needed here.
MAX_TRACKED_TOKENS = 750
TOKEN_MAX_AGE_SECONDS = 6 * 3600
TOKEN_STALE_AGE_SECONDS = 90 * 60
TOKEN_STALE_VOL_THRESHOLD = 5_000
TOKEN_PRUNE_INTERVAL = 300

# [v4.26] Persistent storage directory. Railway's container filesystem is
# ephemeral — anything written outside a mounted Volume is wiped on every
# redeploy/restart. If you attach a Volume in the Railway dashboard, Railway
# auto-injects RAILWAY_VOLUME_MOUNT_PATH pointing at it; state files then
# survive redeploys automatically. With no Volume attached, this falls back to
# the working directory (today's behavior — state resets on every deploy).
_DATA_DIR = (os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
             or os.getenv("DATA_DIR", "").strip()
             or ".")
if _DATA_DIR != "." and not os.path.isdir(_DATA_DIR):
    try:
        os.makedirs(_DATA_DIR, exist_ok=True)
    except Exception:
        _DATA_DIR = "."  # mount path not writable/available — fail open to cwd

# v4.4 constants
STATE_FILE = os.path.join(_DATA_DIR, "astaroth_state.json")
ALERT_RATE_LIMIT = 30
SNAPSHOT_INTERVAL = 15  # [fix] save more frequently — Railway redeploys are fast
BOOST_POLL_INTERVAL = 120
VOL_ACCEL_THRESHOLD = 1.5

# [v4.6] WS spam deduplication
WS_SYMBOL_COOLDOWN = 30        # seconds — ignore same symbol+buy fingerprint within this window
WS_COOLDOWN_CLEANUP = 300      # clean up expired cooldown entries every 5 min

# [v4.7] KOL social polling
KOL_FILE = os.path.join(_DATA_DIR, "kol_list.json")  # persisted KOL account list
KOL_POLL_INTERVAL = 120               # poll each KOL every 2 minutes
KOL_POST_LOOKBACK = 300               # only consider posts from last 5 minutes
KOL_ALERT_COOLDOWN = 600              # 10 min cooldown per KOL+ticker pair
KOL_MAX_ACCOUNTS = 50                 # hard cap on KOL list size
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")  # Twttr API key from rapidapi.com (twitter241.p.rapidapi.com)
# [v4.32] The previous three instances were all confirmed dead (checked against
# a live Nitter instance-health tracker on 2026-08-25: nitter.poast.org shown
# down, the other two no longer even tracked) — meaning KOL/tweet polling had
# been silently returning zero posts every cycle with nothing logged to show
# it. Swapped in the instances that tracker showed with the best uptime.
#
# Caveat, found while double-checking these before shipping: direct RSS
# fetches against all four of them returned 403/410 errors rather than a
# feed. That may be specific to how the check was made (some Nitter fronts
# gate scraper-looking requests without blocking regular use) rather than
# proof they're broken for this bot's own requests — Nitter's whole ecosystem
# is under sustained pressure from X and instance health/RSS availability is
# known to fluctuate day to day. Treat this list as a real improvement over
# three confirmed-dead entries, not a guarantee — if KOL alerts still aren't
# firing after this, Nitter's RSS access itself is likely blocked platform-
# wide right now, and RAPIDAPI_KEY (see fetch_kol_posts_rapidapi below) is
# the more durable fix at that point, not another instance swap.
NITTER_INSTANCES = [
    "https://xcancel.com",
    "https://nitter.net",
    "https://nitter.kareem.one",
    "https://nitter.catsarch.com",
]
# Regex: match $TICKER (2-10 uppercase letters/digits) in post text
# [v4.33] fix: this line previously had a stray literal backspace byte just
# before the closing quote (invisible in a normal editor/diff), which made
# the compiled pattern require a literal backspace character immediately
# after every ticker match. Since real tweet text never contains one,
# TICKER_RE.findall() has been silently returning zero matches this whole
# time -- the $CASHTAG side of KOL matching was never actually firing.
TICKER_RE = re.compile(r'\$([A-Za-z][A-Za-z0-9]{1,9})')

# [v4.33] Word/phrase-overlap KOL matching -- catches the common memecoin
# pattern where a viral word/phrase gets "tokenized" into a new token
# name/symbol WITHOUT an explicit $CASHTAG mention (e.g. a KOL tweets "based
# frog just took over my feed" and a token later launches named/symboled
# BASEDFROG). Purely deterministic word/phrase overlap -- no LLM/semantic
# summarization -- per explicit choice. Extends, doesn't replace, the
# extract_tickers()/$CASHTAG path above.
KOL_KEYWORD_ALERT_COOLDOWN = 600        # per KOL+match, same window as the ticker cooldown
KOL_KEYWORD_SEARCH_COOLDOWN = 1800      # 30 min, GLOBAL per-keyword -- gates the DexScreener
                                         # API call itself, so a phrase mentioned by many KOLs
                                         # inside the same half hour only gets searched once
KOL_MAX_KEYWORDS_PER_POST = 8           # cap extracted word/phrase candidates per tweet
KOL_MAX_KEYWORD_SEARCHES_PER_POST = 3   # cap live DexScreener searches fired per tweet

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
_last_chain_poll: dict = {"ethereum": 0.0, "bsc": 0.0, "base": 0.0, "robinhood": 0.0}

# [v4.40] Solana pending-entry state — see _handle_ws_create/_finalize_sol_pending_entry.
# mint -> {"symbol", "name", "created_at", "mc_sol", "vsol_curve", "initial_buy",
#          "ws_buy_count", "ws_sell_count", "ws_buy_vol_usd", "ws_sell_vol_usd"}
# A brand-new pump.fun mint lands here (NOT in `tokens`) the instant it's created;
# it's promoted to `tokens` (or dropped for good) exactly once, when its one-shot
# delayed check fires. Never re-checked after that either way.
_pending_sol_entries: Dict[str, dict] = {}
SOL_ENTRY_CHECK_DELAY = float(os.getenv("SOL_ENTRY_CHECK_DELAY", "45"))
# Trade-feed subscription slots reserved for pending mints, out of the shared
# WS_MAX_TRADE_SUBS=50 PumpPortal cap (the rest stays available for already-
# alerted tokens, which use the same subscription pool via ws_sub_request_queue).
SOL_PENDING_SUB_CAP = int(os.getenv("SOL_PENDING_SUB_CAP", "15"))

# [v4.6] WS spam dedup: symbol -> list of (timestamp, mint) tuples seen within cooldown window
# Used to detect squatter swarms (many different mints sharing the same ticker in a short burst).
ws_symbol_cooldown: Dict[str, list] = {}

# [v4.26] symbol -> lockout expiry timestamp. Set once a symbol trips the swarm
# threshold; any new mint for that exact symbol is dropped outright until this
# expires, regardless of the (much shorter) burst-detection window above.
ws_symbol_lockout: Dict[str, float] = {}

# [v4.26] symbol -> {"mint", "chain_id", "alerted_at"} for the most recent token
# that actually alerted under that ticker. Used to block later same-symbol mints
# from being tracked as squats on an already-proven gem.
alerted_symbol_registry: Dict[str, dict] = {}
ws_sub_request_queue: asyncio.Queue = None  # [fix] side-channel for trade sub requests

# How many distinct mints for the same symbol in WS_SYMBOL_COOLDOWN seconds = squatter swarm
WS_SQUATTER_THRESHOLD = 4

# [v4.26] Extended squatter lockout. The original burst window (WS_SYMBOL_COOLDOWN,
# 30s) resets itself — a squatter swarm that drips mints in slightly slower than
# 30s apart never re-triggers the threshold and slips through indefinitely. Once a
# symbol trips the swarm threshold, lock it out for a much longer window instead of
# only during the triggering burst.
WS_SYMBOL_LOCKOUT_SECONDS = int(os.getenv("WS_SYMBOL_LOCKOUT_SECONDS", "900"))  # 15 min

# [v4.26] Guard against copycats of a symbol that already produced a REAL alerted
# gem. Once $TICKER passes every threshold and alerts, any other distinct mint
# reusing that exact ticker within this window is almost always a clone trying to
# ride the original's momentum/search traffic — block it from ever being tracked.
ALERTED_SYMBOL_GUARD_SECONDS = int(os.getenv("ALERTED_SYMBOL_GUARD_SECONDS", "3600"))  # 1 hour

# [v4.29] Direct pump.fun on-chain indexing state. mint -> raw token_total_supply,
# cached off each CreateEvent since TradeEvent doesn't carry it.
_pumpfun_supply_cache: Dict[str, int] = {}
pumpfun_direct_stats = {
    "connected": False,
    "mode": PUMPFUN_DIRECT_MODE,
    "creates_decoded": 0,
    "trades_decoded": 0,
    "decode_errors": 0,
    "reconnects": 0,
    "last_message_at": 0.0,
}

# [v4.7] KOL state
kol_accounts: Dict[str, dict] = {}    # handle -> {added_at, last_polled, post_ids_seen}
kol_alert_cooldown: Dict[str, float] = {}  # "handle:TICKER" -> last alert timestamp
# [v4.33] word/phrase-overlap match cooldowns reuse the dict above with
# distinct key prefixes ("handle:mint:<mint>" for a matched tracked token,
# never colliding with the plain "handle:TICKER" keys used for cashtags).
# This one is separate and GLOBAL (not per-handle) -- it gates the actual
# DexScreener search call for a given keyword/phrase, independent of which
# KOL said it or whether an alert ends up firing.
kol_keyword_search_at: Dict[str, float] = {}  # keyword(concat form) -> last DexScreener search time

# [v4.5] WebSocket stats
ws_stats = {
    "connected": False,
    "reconnects": 0,
    "tokens_discovered": 0,
    "trades_received": 0,
    "last_message_at": 0.0,
    "trade_subs": 0,
}

# [v4.25] Alchemy WS stats — one entry per EVM chain with has_ws enabled
# [v4.36] pre_tracked / fresh_alerts added — see the pre-track stub added in
# evm_ws_listener and the FRESH🌱 path in run_detections.
evm_ws_stats: Dict[str, dict] = {
    cid: {"connected": False, "reconnects": 0, "pairs_discovered": 0, "last_message_at": 0.0,
          "trades_received": 0, "swap_subs": 0, "pre_tracked": 0, "fresh_alerts": 0}
    for cid in ("bsc", "base", "ethereum", "robinhood")
}

# [v4.32] KOL/tweet-tracker visibility — both fetch paths (Nitter, RapidAPI)
# fail by simply returning an empty list, with nothing logged to distinguish
# "no new tweets right now" from "every fetch has been silently failing for
# hours." This surfaces that distinction in /status instead of requiring a
# code read to notice — see kol_polling_loop.
kol_stats = {
    "last_fetch_attempt": 0.0,
    "last_post_found_at": 0.0,
    "consecutive_empty_polls": 0,
    "warned_stale": False,
    # [v4.33] word/phrase-overlap matching counters
    "keyword_matches_tracked": 0,   # matched an already-tracked token by word/phrase overlap
    "keyword_live_searches": 0,     # live DexScreener searches fired for untracked phrases
    "keyword_live_hits": 0,         # of those searches, how many found a real token match
}
# How many fully-empty poll rounds (every KOL, zero posts from either path)
# before logging a one-time warning that the fetch paths might be dead.
KOL_STALE_WARN_THRESHOLD = 20  # roughly ~1-2 KOL_POLL_INTERVAL cycles per handle

# [v4.26] EVM swap/volume feed state — per chain.
# pair_address(lower) -> {"mint", "base_token", "base_is_token0", "dex"}
evm_pair_meta: Dict[str, Dict[str, dict]] = {cid: {} for cid in ("bsc", "base", "ethereum", "robinhood")}
# pair addresses currently included in the live Swap-event subscription
evm_swap_subscribed: Dict[str, Set[str]] = {cid: set() for cid in ("bsc", "base", "ethereum", "robinhood")}
# JSON-RPC subscription id returned by Alchemy for the swap-logs filter (needed
# to route incoming eth_subscription pushes and to eth_unsubscribe on refresh)
evm_swap_sub_id: Dict[str, Optional[str]] = {cid: None for cid in ("bsc", "base", "ethereum", "robinhood")}
evm_factory_sub_id: Dict[str, Optional[str]] = {cid: None for cid in ("bsc", "base", "ethereum", "robinhood")}

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
    # [v4.27] Live bonding-curve estimates (Solana pre-graduation tokens). DexScreener
    # has no pair at all until a pump.fun token migrates off the bonding curve, so
    # market_cap/liquidity/volume/buy_ratio would otherwise stay frozen at their
    # creation-time stub forever — see run_detections for how these are used as a
    # fallback in place of the (permanently absent) DexScreener fields.
    ws_liquidity_estimate: float = 0.0  # vSolInBondingCurve * SOL price — proxy for liq_min
    # [v4.9] multi-chain
    chain_id: str = "solana"          # which chain this token is on
    # [v4.11] ETH buy volume tracking
    buy_volume_h1: float = 0.0        # USD value of buy-side volume in last 1h
    # [v4.42] Real total (buy+sell) 1h volume straight off DexScreener's own
    # volume.h1 field. Distinct from volume_usd, which is actually an
    # estimated buy-only slice of h1 volume (h1_vol * buy_ratio, see
    # get_dex_data) -- this is what vol_ok/vol_ok_momentum in run_detections
    # now check the entry-gate vol_min/vol_max window against, per user
    # request that the volume threshold be measured on real 1h volume rather
    # than a derived estimate.
    volume_h1_total: float = 0.0
    volume_m5: float = 0.0            # 5-min volume (whale spike detection)
    vol_usd_history: List[float] = field(default_factory=list)  # buy vol history for accel
    # [v4.11] Anti-dump / sustained signal fields
    price_change_h1: float = 0.0      # 1h price change % from DexScreener
    consecutive_green_polls: int = 0  # polls in a row where MC rose
    alert_suppressed_until: float = 0.0  # timestamp — don't alert before this
    first_seen_mc: float = 0.0        # MC when first tracked (for dump detection)
    # Reply threading — stores the Telegram message_id of the initial alert
    alert_message_id: int = 0         # used to reply-thread P&L / milestone updates
    # [v4.27] Cap-eviction ranking score — see update_composite_score(). Was
    # previously read via getattr(t, "composite_score", 0.0) with no such
    # field ever actually set, so every eviction candidate silently tied at
    # 0.0 and enforce_token_cap degraded to pure oldest-first eviction.
    composite_score: float = 0.0
    # [v4.35] four.meme (BSC bonding-curve launchpad) curve-progress proxy.
    # Deliberately NOT market_cap -- four.meme's actual bonding-curve pricing
    # formula isn't published anywhere verifiable, so unlike the Solana
    # pump.fun direct indexer (which computes a real MC from a documented
    # IDL), there's no trustworthy way to turn their `price` field into a
    # real USD market cap here. This holds cumulative BNB raised (a real,
    # decimal-safe number straight off the TokenPurchase/TokenSale events),
    # used only by the separate CURVE🌊 path in run_detections -- it can
    # never reach or be confused with the real-dollar-MC-gated paths.
    fourmeme_raised_usd: float = 0.0

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
        "ws_liquidity_estimate": token.ws_liquidity_estimate,
        "_sent_milestones": list(getattr(token, '_sent_milestones', set())),
        "chain_id": token.chain_id,
        "buy_volume_h1": token.buy_volume_h1,
        "volume_h1_total": token.volume_h1_total,
        "volume_m5": token.volume_m5,
        "price_change_h1": token.price_change_h1,
        "consecutive_green_polls": token.consecutive_green_polls,
        "first_seen_mc": token.first_seen_mc,
        "composite_score": token.composite_score,
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
        ws_liquidity_estimate=d.get("ws_liquidity_estimate", 0.0),
        chain_id=d.get("chain_id", "solana"),
        buy_volume_h1=d.get("buy_volume_h1", 0.0),
        volume_h1_total=d.get("volume_h1_total", 0.0),
        volume_m5=d.get("volume_m5", 0.0),
        price_change_h1=d.get("price_change_h1", 0.0),
        consecutive_green_polls=d.get("consecutive_green_polls", 0),
        first_seen_mc=d.get("first_seen_mc", 0.0),
        composite_score=d.get("composite_score", 0.0),
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
    if _DATA_DIR == ".":
        logger.warning(
            "⚠️ No Railway Volume detected (RAILWAY_VOLUME_MOUNT_PATH unset) — "
            "state/KOL files live on the ephemeral container disk and WILL be "
            "wiped on the next redeploy. Attach a Volume in the Railway dashboard "
            "to persist across deploys."
        )
    else:
        logger.info(f"💾 Persistent storage active — state/KOL files saved to {_DATA_DIR}")
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


def update_composite_score(token: TokenInfo) -> None:
    """
    [v4.27] Lightweight, always-computable momentum score used ONLY for
    cap-eviction ranking in enforce_token_cap — not alert gating. Built from
    signals available even for bonding-curve tokens with no DexScreener data
    yet (mc_velocity, live WS buy pressure, trade count), so a real early
    runner isn't evicted from the tracked pool just for being "unscored" —
    which is what happened before this field existed (see its docstring).
    Call after any update to a token's mc_velocity/ws_buy_count/ws_sell_count.
    """
    ws_total = token.ws_buy_count + token.ws_sell_count
    buy_pressure = (token.ws_buy_count / ws_total) if ws_total > 0 else 0.5
    token.composite_score = (
        max(token.mc_velocity, 0.0) * 1.0   # rewards fast MC growth
        + buy_pressure * 20.0                # rewards buy-dominant live flow
        + min(ws_total, 50) * 0.5            # rewards real trade activity, capped
    )


def effective_liq_vol_buyratio(token: TokenInfo) -> tuple:
    """
    [v4.27] Returns (liq, vol, buy_ratio), preferring real DexScreener data
    when available and falling back to live WS-derived bonding-curve
    estimates for pre-graduation Solana tokens (token.liquidity == 0, i.e.
    DexScreener has no pair yet). Used by both run_detections (to decide
    whether to alert) and format_gem_alert (so the Telegram card doesn't show
    a misleading $0 Vol/Liq on an alert that fired specifically because of
    real bonding-curve activity).
    """
    # [v4.44] Every branch below returns BUY-side volume only (not buy+sell)
    # per user request -- sell volume is dump pressure, not the "is this
    # getting bought up" signal the vol_ok/vol_ok_momentum entry-gate bands
    # are meant to measure. Combined with vol_min dropping to $5k (see
    # CHAIN_THRESHOLDS), this makes the gate noticeably easier to clear on
    # its volume side than the two prior designs (the original vol/mc ratio,
    # then the v4.39-v4.42 total-dollar-band).
    if token.chain_id == "solana" and token.liquidity == 0:
        ws_total = token.ws_buy_count + token.ws_sell_count
        liq = token.ws_liquidity_estimate
        vol = token.ws_buy_vol_usd  # real buy-side volume from live WS trades
        buy_ratio = token.ws_buy_count / max(ws_total, 1) if ws_total >= 5 else token.buy_ratio
        return liq, vol, buy_ratio
    # [v4.35] Same fallback for a BSC four.meme pre-migration stub — real
    # buy/sell counts exist (from TokenPurchase/TokenSale), just no
    # DexScreener liquidity/volume yet. Uses fourmeme_raised_usd (cumulative
    # BNB raised) as the liquidity proxy — NOT market_cap, see TokenInfo.
    if token.chain_id == "bsc" and token.ws_pre_enrichment and token.liquidity == 0:
        ws_total = token.ws_buy_count + token.ws_sell_count
        liq = token.fourmeme_raised_usd
        vol = token.ws_buy_vol_usd  # real buy-side volume from live WS trades
        buy_ratio = token.ws_buy_count / max(ws_total, 1) if ws_total >= 5 else token.buy_ratio
        return liq, vol, buy_ratio
    # [v4.36] Generic pre-DexScreener-index fallback for vanilla EVM DEX pairs
    # (PancakeV2/V3, Uniswap, Sushi, Aerodrome — see the pre-track stub added
    # in evm_ws_listener). Unlike Solana/four.meme there's no liquidity proxy
    # available (a PairCreated event carries no reserves), so liq stays 0 —
    # but vol/buy_ratio come from real WS swap counters instead of sitting at
    # the dataclass default (0 / 0.5) for the whole pre-enrichment window.
    if token.ws_pre_enrichment and token.liquidity == 0:
        ws_total = token.ws_buy_count + token.ws_sell_count
        vol = token.ws_buy_vol_usd  # real buy-side volume from live WS trades
        buy_ratio = token.ws_buy_count / max(ws_total, 1) if ws_total >= 5 else token.buy_ratio
        return token.liquidity, vol, buy_ratio
    # [v4.44] Real DexScreener data available -- use buy_volume_h1 (DexScreener
    # doesn't split its h1 volume into buy/sell dollar amounts, so this is
    # still h1_vol * buy_ratio under the hood, see get_dex_data -- the best
    # buy-only estimate available from that API) instead of volume_h1_total
    # (v4.42's real-total figure, one message ago) or volume_usd (same
    # buy_volume_h1 number, just not kept in sync after token creation --
    # see the TokenInfo(...) call sites in _finalize_ws_token / the polling
    # loop, which is why this reads buy_volume_h1 specifically).
    return token.liquidity, token.buy_volume_h1, token.buy_ratio


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

        # [v4.26] Clear expired lockouts and stale alerted-symbol guard entries
        expired_locks = [s for s, exp in ws_symbol_lockout.items() if exp <= now]
        for s in expired_locks:
            del ws_symbol_lockout[s]

        guard_cutoff = now - ALERTED_SYMBOL_GUARD_SECONDS
        expired_guards = [
            s for s, g in alerted_symbol_registry.items() if g["alerted_at"] <= guard_cutoff
        ]
        for s in expired_guards:
            del alerted_symbol_registry[s]

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

        # [v4.26] Drop evm_pair_meta entries for pruned tokens too, else the
        # swap-feed metadata dict grows unbounded over long uptimes.
        pruned_mints = {mint for mint, _, _ in to_remove}
        for cid_meta in evm_pair_meta.values():
            stale_pairs = [p for p, info in cid_meta.items() if info["mint"] in pruned_mints]
            for p in stale_pairs:
                del cid_meta[p]


def enforce_token_cap(symbol: str) -> bool:
    """Must be called while holding tokens_lock.

    Score-aware eviction: never evict tokens with consecutive_green_polls >= 2
    (i.e. tokens showing sustained momentum). Among evictable candidates, prefer
    to drop the lowest-scored oldest token rather than simply the oldest.

    [v4.27] composite_score is now a real, continuously-updated field (see
    update_composite_score) — it used to be read via getattr(t,
    "composite_score", 0.0) with no such attribute ever set anywhere, so every
    candidate silently tied at 0.0 and this degraded to pure oldest-first
    eviction regardless of which tokens actually showed momentum.
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


# [v4.33] Common filler/noise words excluded from word/phrase-overlap
# extraction -- English stopwords plus Twitter/crypto-slang filler that would
# otherwise dominate every tweet's keyword set and make overlap meaningless.
KOL_KEYWORD_STOPWORDS = {
    "the", "and", "for", "with", "this", "that", "just", "like", "your", "you",
    "are", "was", "were", "have", "has", "had", "will", "would", "could",
    "should", "about", "into", "over", "out", "not", "but", "all", "can",
    "get", "got", "now", "new", "big", "one", "two", "see", "look", "looks",
    "looking", "check", "checking", "going", "gonna", "here", "there", "what",
    "when", "where", "why", "how", "who", "its", "im", "on", "in", "to", "of",
    "is", "it", "at", "as", "be", "by", "an", "or", "if", "so", "up", "we",
    "us", "our", "they", "them", "their", "he", "she", "his", "her", "from",
    "than", "then", "some", "any", "more", "most", "very", "really", "still",
    "even", "also", "back", "off", "down", "again", "any", "few", "many",
    # crypto/twitter filler
    "gm", "gn", "wagmi", "ngmi", "lfg", "wen", "ser", "fren", "frens", "moon",
    "mooning", "mooned", "pump", "pumping", "pumped", "dump", "dumping",
    "dumped", "buy", "buying", "sell", "selling", "bullish", "bearish",
    "chart", "charts", "coin", "coins", "token", "tokens", "crypto", "huge",
    "massive", "insane", "crazy", "wow", "lol", "lmao", "amp", "rt", "via",
    "today", "tonight", "yall", "dont", "cant", "thats", "whats",
}

# Candidate words: letters/digits/apostrophe, length >= 3.
_KOL_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9']{2,}")


def extract_keywords(text: str) -> List[dict]:
    """
    Extract candidate word/phrase-overlap targets from a tweet -- the broader
    signal that catches the common memecoin pattern where a viral word or
    phrase gets "tokenized" into a new token name/symbol even with no
    $CASHTAG mention (e.g. "based frog" trending -> a token later launches
    named/symboled BASEDFROG).

    Each candidate is {"words": [...], "phrase": "based frog", "concat":
    "basedfrog"} -- single significant words AND adjacent-word bigrams. The
    bigram's concatenated form is what a token symbol usually looks like once
    a two-word phrase gets tokenized, so it's checked directly against
    token symbols; the word list is checked against token *names*.

    Deliberately separate from extract_tickers() ($CASHTAG-only, exact) --
    this is a noisier signal, so results are capped and downstream matching
    requires real overlap, not just any shared word.
    """
    clean = re.sub(r"https?://\S+", " ", text)
    clean = re.sub(r"@\w+", " ", clean)
    clean = re.sub(r"\$[A-Za-z][A-Za-z0-9]{1,9}\b", " ", clean)  # cashtags: extract_tickers' job
    clean = re.sub(r"^\s*RT\b", " ", clean)

    raw_words = [w.lower() for w in _KOL_WORD_RE.findall(clean)]
    words = [w for w in raw_words if w not in KOL_KEYWORD_STOPWORDS and len(w) >= 3]
    if not words:
        return []

    candidates: List[dict] = []
    seen_concat = set()

    # Bigrams first (more specific -> lower false-positive rate), built from
    # adjacency in the FILTERED sequence so "based frog" stays a phrase even
    # once unrelated filler words around it have been stripped out.
    for i in range(len(words) - 1):
        w1, w2 = words[i], words[i + 1]
        concat = w1 + w2
        if concat in seen_concat:
            continue
        seen_concat.add(concat)
        candidates.append({"words": [w1, w2], "phrase": f"{w1} {w2}", "concat": concat})

    # Single words -- only long enough ones stand alone as a candidate; short
    # single words are too noisy to search DexScreener for on their own.
    for w in words:
        if len(w) < 5 or w in seen_concat:
            continue
        seen_concat.add(w)
        candidates.append({"words": [w], "phrase": w, "concat": w})

    return candidates[:KOL_MAX_KEYWORDS_PER_POST]


def _name_to_words(name: str) -> Set[str]:
    """Normalize a token name string into significant lowercase words."""
    raw = re.findall(r"[A-Za-z0-9']{2,}", name or "")
    return {w.lower() for w in raw if w.lower() not in KOL_KEYWORD_STOPWORDS and len(w) >= 3}


def _keyword_match_score(kw: dict, symbol: str, name_words: Set[str]) -> int:
    """
    0 = no match. Higher = stronger match, so a caller checking several
    candidate keywords against the same token can prefer the best one for
    the alert message (e.g. an exact "based frog" phrase over some other,
    weaker same-token overlap like "talking based" that happens to share
    just one word). Matches in two ways:
      1. The candidate's concatenated form equals the token's symbol exactly
         -- catches "based frog" -> $BASEDFROG, or a single word like "pepe"
         mentioned in plain prose matching a token symboled PEPE. Always
         outranks an overlap-only match.
      2. The candidate's words overlap meaningfully with the token's name --
         for a multi-word name, at least half its significant words (and at
         least one) must appear in the candidate; for a single-word name, it
         must match that word exactly. This avoids one generic word shared
         between an unrelated tweet and an unrelated token name counting as
         a "match". Score is the overlap size, so a fuller-phrase overlap
         ranks above a partial one.
    """
    if not symbol and not name_words:
        return 0
    if kw["concat"] == (symbol or "").lower():
        return 1000
    if not name_words:
        return 0
    kw_words = set(kw["words"])
    overlap = kw_words & name_words
    if not overlap:
        return 0
    if len(name_words) == 1:
        return 500 if kw_words == name_words else 0
    needed = max(1, len(name_words) // 2)
    if len(overlap) < needed:
        return 0
    return len(overlap)


def _keyword_matches_token(kw: dict, symbol: str, name_words: Set[str]) -> bool:
    """Boolean convenience wrapper around _keyword_match_score() -- used on
    the live-DexScreener-search path, where each search result is judged on
    its own rather than ranked against sibling candidates."""
    return _keyword_match_score(kw, symbol, name_words) > 0


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
                # [v4.32] Was "Mozilla/5.0 ASTAROTH/1.0" — self-identifying as a
                # bot in the UA string is an easy target for basic WAF/bot-
                # detection rules in front of a Nitter instance. A plain
                # browser-shaped UA at least doesn't hand-wave a free block.
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"},
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
    1. Extract $TICKER mentions (exact) AND word/phrase-overlap keywords
       (broader -- catches "tokenized" phrases with no cashtag at all).
    2. If a match (cashtag or word/phrase) hits a tracked token → fire a
       KOL alert.
    3. If a $TICKER doesn't match a tracked token → queue a DexScreener
       symbol lookup (as before). If a word/phrase doesn't match a tracked
       token → fire a *live* DexScreener search using the phrase itself,
       rate-limited per keyword (see kol_keyword_search_at), and queue any
       real hit for enrichment.
    """
    if not posts:
        return

    kol_data = kol_accounts.get(handle, {})
    seen_ids: set = set(kol_data.get("post_ids_seen", []))
    now = time.time()
    new_ids = set()

    for post in posts:
        post_id = post["id"]
        if post_id in seen_ids:
            continue
        new_ids.add(post_id)

        text = post["text"]
        tickers = extract_tickers(text)
        keywords = extract_keywords(text)
        if not tickers and not keywords:
            continue

        alerted_tickers = set()
        alerted_mints = set()      # mints already alerted on for THIS post (any match type)
        matched_concats = set()    # keyword concats already resolved to a tracked token

        if tickers:
            logger.info(f"📋 KOL @{handle} mentioned: {tickers} | '{text[:80]}'")

        # --- 1. Exact $CASHTAG matching (unchanged logic, now that TICKER_RE
        #     itself is fixed -- see the [v4.33] fix note on TICKER_RE) ---
        for ticker in tickers:
            if ticker in alerted_tickers:
                continue

            cooldown_key = f"{handle}:{ticker}"
            if now - kol_alert_cooldown.get(cooldown_key, 0) < KOL_ALERT_COOLDOWN:
                continue

            matched_tokens = [
                (mint, t) for mint, t in tokens.items()
                if t.symbol.upper() == ticker
            ]

            if matched_tokens:
                for mint, token in matched_tokens:
                    kol_alert_cooldown[cooldown_key] = now
                    alerted_tickers.add(ticker)
                    alerted_mints.add(mint)
                    d_url = dex_url(mint, token.chain_id)
                    gem_tag = "💎 Already gem alerted" if token.alerted else "⏳ Not yet gem"
                    msg = (
                        f"👁️ <b>KOL MENTION: @{handle}</b>\n\n"
                        f"💬 <i>{text[:200].strip()}</i>\n\n"
                        f"🎯 Tracked: <b>${token.symbol}</b>\n"
                        f"💰 MC: ${token.market_cap:,.0f}\n"
                        f"📊 Vol: ${token.volume_usd:,.0f}\n"
                        f"{gem_tag}\n\n"
                        f"🔗 <a href='{d_url}'>DexScreener</a>"
                    )
                    await send_telegram(msg)
                    logger.info(f"👁️ KOL alert: @{handle} → ${ticker} (tracked)")
            else:
                kol_alert_cooldown[cooldown_key] = now
                alerted_tickers.add(ticker)
                logger.info(f"👁️ KOL @{handle} → ${ticker} (unknown — queuing for discovery)")

                msg = (
                    f"👁️ <b>KOL MENTION: @{handle}</b>\n\n"
                    f"💬 <i>{text[:200].strip()}</i>\n\n"
                    f"🔍 <b>${ticker}</b> — not yet tracked\n"
                    "⚡ Queuing for DexScreener lookup..."
                )
                await send_telegram(msg)

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

        # --- 2. Word/phrase overlap against currently TRACKED tokens ---
        # No cashtag required -- this is what catches "based frog" style
        # mentions of an already-launched token nobody put a $ in front of.
        if keywords:
            # Loop TOKENS outer, keywords inner, so each token is judged
            # against every candidate and alerted using the BEST-scoring one
            # (e.g. an exact "based frog" phrase, not an incidental single-
            # word overlap like "talking based" that happens to appear first).
            for mint, token in list(tokens.items()):
                if mint in alerted_mints:
                    continue
                name_words = _name_to_words(token.name)
                best_kw, best_score = None, 0
                for kw in keywords:
                    if kw["concat"] in matched_concats:
                        continue
                    score = _keyword_match_score(kw, token.symbol, name_words)
                    if score > best_score:
                        best_score, best_kw = score, kw
                if best_kw is None:
                    continue

                cooldown_key = f"{handle}:mint:{mint}"
                if now - kol_alert_cooldown.get(cooldown_key, 0) < KOL_KEYWORD_ALERT_COOLDOWN:
                    continue
                kol_alert_cooldown[cooldown_key] = now
                alerted_mints.add(mint)
                matched_concats.add(best_kw["concat"])
                kol_stats["keyword_matches_tracked"] += 1
                d_url = dex_url(mint, token.chain_id)
                gem_tag = "💎 Already gem alerted" if token.alerted else "⏳ Not yet gem"
                msg = (
                    f"👁️ <b>KOL WORD MATCH: @{handle}</b>\n\n"
                    f"💬 <i>{text[:200].strip()}</i>\n\n"
                    f"🎯 Tracked: <b>${token.symbol}</b> (matched \"{best_kw['phrase']}\")\n"
                    f"💰 MC: ${token.market_cap:,.0f}\n"
                    f"📊 Vol: ${token.volume_usd:,.0f}\n"
                    f"{gem_tag}\n\n"
                    f"🔗 <a href='{d_url}'>DexScreener</a>"
                )
                await send_telegram(msg)
                logger.info(f"👁️ KOL word-match alert: @{handle} → ${token.symbol} via '{best_kw['phrase']}'")

        # --- 3. Word/phrase overlap against a LIVE DexScreener search ---
        # For any keyword that didn't already resolve to a tracked token,
        # actively search DexScreener for a token whose name/symbol matches
        # the phrase -- this is what catches a phrase getting tokenized into
        # something the bot's own discovery hasn't picked up (yet).
        searches_done = 0
        for kw in keywords:
            if searches_done >= KOL_MAX_KEYWORD_SEARCHES_PER_POST:
                break
            if kw["concat"] in matched_concats:
                continue  # already resolved against a tracked token above

            concat = kw["concat"]
            if now - kol_keyword_search_at.get(concat, 0) < KOL_KEYWORD_SEARCH_COOLDOWN:
                continue
            kol_keyword_search_at[concat] = now
            searches_done += 1
            kol_stats["keyword_live_searches"] += 1

            try:
                async with session.get(
                    f"{DEXSCREENER_SEARCH}?q={kw['phrase']}",
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    for pair in data.get("pairs", [])[:5]:
                        if pair.get("chainId") not in ENABLED_CHAIN_IDS:
                            continue
                        base = pair.get("baseToken", {})
                        found_mint = base.get("address")
                        if not found_mint or found_mint in tokens or found_mint in alerted_mints:
                            continue
                        p_symbol = base.get("symbol", "")
                        p_name = base.get("name", "")
                        if not _keyword_matches_token(kw, p_symbol, _name_to_words(p_name)):
                            continue

                        alerted_mints.add(found_mint)
                        kol_stats["keyword_live_hits"] += 1
                        pair_chain = pair.get("chainId", "solana")
                        if ws_discovery_queue is not None:
                            await ws_discovery_queue.put({
                                "mint": found_mint,
                                "symbol": p_symbol.upper(),
                                "name": p_name or p_symbol,
                                "chain_id": pair_chain,
                                "created_at": now,
                                "ws_discovered": False,
                                "ws_initial_buy_sol": 0.0,
                                "mc_sol_at_creation": 0.0,
                                "kol_source": handle,
                            })
                        msg = (
                            f"👁️ <b>KOL WORD MATCH: @{handle}</b>\n\n"
                            f"💬 <i>{text[:200].strip()}</i>\n\n"
                            f"🔍 Phrase <b>\"{html.escape(kw['phrase'])}\"</b> ↔ <b>${html.escape(p_symbol.upper())}</b>"
                            f" (\"{html.escape(p_name)}\") — not yet tracked\n"
                            "⚡ Queuing for DexScreener enrichment..."
                        )
                        await send_telegram(msg)
                        logger.info(
                            f"👁️ KOL @{handle} phrase '{kw['phrase']}' → "
                            f"${p_symbol.upper()} ({found_mint[:8]}...) queued"
                        )
                        break  # one hit per keyword search is enough
            except Exception as e:
                logger.debug(f"KOL keyword search error '{kw['phrase']}': {e}")

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
                kol_stats["last_fetch_attempt"] = now
                posts = await fetch_kol_posts_nitter(handle, session)
                if not posts:
                    posts = await fetch_kol_posts_rapidapi(handle, session)

                # [v4.32] Track whether fetches are actually finding anything —
                # both fetch functions return [] on total failure (dead Nitter
                # instances, missing RAPIDAPI_KEY) exactly the same as they do
                # on "this account genuinely hasn't posted lately," so this is
                # the only way to tell the two apart from the logs.
                if posts:
                    kol_stats["last_post_found_at"] = now
                    kol_stats["consecutive_empty_polls"] = 0
                    kol_stats["warned_stale"] = False
                else:
                    kol_stats["consecutive_empty_polls"] += 1
                    if (kol_stats["consecutive_empty_polls"] >= KOL_STALE_WARN_THRESHOLD
                            and not kol_stats["warned_stale"] and handles):
                        kol_stats["warned_stale"] = True
                        logger.warning(
                            f"📋 KOL tracker: {kol_stats['consecutive_empty_polls']} consecutive "
                            f"empty polls across {len(handles)} account(s) — Nitter instances may "
                            f"all be down and RAPIDAPI_KEY may be unset. Check /status -> kol."
                        )

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
    vsol_curve = float(data.get("vSolInBondingCurve", 0) or 0)

    # Primary dedup: mint address is the canonical unique key
    if mint in seen_mints:
        return

    # [v4.6] Squatter detection: many distinct mints for the same ticker in a short
    # window = coordinated symbol-squatting attack. Track (timestamp, mint) pairs per symbol.
    sym_key = symbol.upper().strip()
    now = time.time()

    # [v4.26] Hard lockout — if this symbol already tripped the swarm threshold
    # recently, keep blocking it for the full lockout window even though the
    # original 30s burst window has long since rolled over.
    lock_until = ws_symbol_lockout.get(sym_key, 0.0)
    if lock_until > now:
        logger.debug(f"🚫 WS squat-locked: ${symbol} ({int(lock_until - now)}s left)")
        return

    # [v4.26] Copycat-of-a-winner guard — a different mint reusing the exact
    # ticker of a token that already alerted as a real gem is almost always a
    # clone riding the original's momentum, not a coincidence.
    guard = alerted_symbol_registry.get(sym_key)
    if guard and guard["mint"] != mint and (now - guard["alerted_at"]) < ALERTED_SYMBOL_GUARD_SECONDS:
        remaining = int(ALERTED_SYMBOL_GUARD_SECONDS - (now - guard["alerted_at"]))
        logger.debug(
            f"🚫 WS squat-on-winner: ${symbol} — new mint reuses a ticker that "
            f"already alerted ({guard['mint'][:8]}…), {remaining}s guard left"
        )
        return

    cutoff = now - WS_SYMBOL_COOLDOWN
    existing = ws_symbol_cooldown.get(sym_key, [])
    # Prune stale entries
    existing = [(ts, m) for ts, m in existing if ts > cutoff]
    existing.append((now, mint))
    ws_symbol_cooldown[sym_key] = existing
    if len(existing) > WS_SQUATTER_THRESHOLD:
        ws_symbol_lockout[sym_key] = now + WS_SYMBOL_LOCKOUT_SECONDS
        logger.debug(
            f"🚫 WS squatter swarm: ${symbol} — {len(existing)} mints in {WS_SYMBOL_COOLDOWN}s "
            f"(locking ${symbol} for {WS_SYMBOL_LOCKOUT_SECONDS}s)"
        )
        return

    seen_mints.add(mint)
    ws_stats["tokens_discovered"] += 1
    logger.info(f"⚡ WS NEW: ${symbol} | MC: {mc_sol:.1f} SOL | Init buy: {initial_buy:.2f} SOL")

    # [v4.12 -> v4.40] No more instant tracking at creation. Judging a brand-
    # new mint's MC at t=0 (its very first data point) means judging it before
    # it's had any chance to trade -- most pump.fun tokens start well under
    # $10k (commonly ~$5-8k at the ~28-40 SOL starting range seen in
    # production logs) and climb from there. Instead: park this mint in
    # _pending_sol_entries (NOT `tokens`), request a live trade-feed
    # subscription for it so its mc_sol keeps updating in the background for
    # a short bounded window, and check ONCE, after that window, whether it
    # made it into the entry range. Still "look once, no indefinite
    # watching" -- just timed to give a real launch a fair chance instead of
    # judging it before its first trade.
    created_ts = time.time()
    _pending_sol_entries[mint] = {
        "symbol": symbol,
        "name": name,
        "created_at": created_ts,
        "mc_sol": mc_sol,
        "vsol_curve": vsol_curve,
        "initial_buy": initial_buy,
        "ws_buy_count": 0, "ws_sell_count": 0,
        "ws_buy_vol_usd": 0.0, "ws_sell_vol_usd": 0.0,
    }
    if len(_pending_sol_entries) <= SOL_PENDING_SUB_CAP and ws_sub_request_queue is not None:
        try:
            ws_sub_request_queue.put_nowait([mint])
        except asyncio.QueueFull:
            pass  # no live updates for this one during its window -- it'll just be
                   # judged on its creation-time snapshot when the delayed check fires
    asyncio.create_task(_finalize_sol_pending_entry(mint))

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


async def _finalize_sol_pending_entry(mint: str) -> None:
    """
    [v4.40] The one-shot, delayed entry check for a Solana pending mint.
    Fires once, SOL_ENTRY_CHECK_DELAY seconds after creation. If this mint's
    latest mc_sol (updated live by _handle_ws_trade while pending, if its
    trade-feed subscription went through) now lands inside the entry window,
    promote it to a real tracked TokenInfo -- carrying over any buy/sell
    activity that accumulated during the wait so that data isn't lost. If
    not, drop it for good; nothing revisits this mint again.
    """
    await asyncio.sleep(SOL_ENTRY_CHECK_DELAY)
    entry = _pending_sol_entries.pop(mint, None)
    if not entry:
        return  # already resolved another way (e.g. DexScreener path got there first)

    estimated_mc = entry["mc_sol"] * WS_SOL_PRICE_USD
    # [v4.27] vSolInBondingCurve is the SOL actually locked in the curve — the
    # closest thing a pre-graduation token has to "liquidity". DexScreener has
    # no pair for it at all until migration, so without this run_detections'
    # liq_ok gate would stay permanently unsatisfiable. See run_detections.
    estimated_liq = entry["vsol_curve"] * WS_SOL_PRICE_USD
    th = get_thresholds("solana")
    if not (th["mc_min"] <= estimated_mc <= th["mc_max"]):
        return

    async with tokens_lock:
        if mint not in tokens and enforce_token_cap(entry["symbol"]):
            tokens[mint] = TokenInfo(
                mint=mint,
                symbol=entry["symbol"],
                name=entry["name"],
                created_at=entry["created_at"],
                market_cap=estimated_mc,
                ws_discovered=True,
                ws_initial_buy_sol=entry["initial_buy"],
                ws_pre_enrichment=True,
                chain_id="solana",
                ws_liquidity_estimate=estimated_liq,
                ws_buy_count=entry["ws_buy_count"],
                ws_sell_count=entry["ws_sell_count"],
                ws_buy_vol_usd=entry["ws_buy_vol_usd"],
                ws_sell_vol_usd=entry["ws_sell_vol_usd"],
            )
            logger.debug(
                f"🔬 Pre-tracked ${entry['symbol']} after {SOL_ENTRY_CHECK_DELAY:.0f}s "
                f"grace check (MC≈${estimated_mc:,.0f})"
            )


async def _handle_ws_trade(data: dict):
    """
    Handle a live trade event from PumpPortal WS.
    Data fields: mint, txType (buy/sell), tokenAmount, solAmount, traderPublicKey,
                 marketCapSol, vSolInBondingCurve (updated bonding-curve state after this trade)
    """
    mint = data.get("mint")
    if not mint:
        return

    tx_type = data.get("txType", "")
    sol_amount = float(data.get("solAmount", 0) or 0)
    mc_sol = float(data.get("marketCapSol", 0) or 0)
    vsol_curve = float(data.get("vSolInBondingCurve", 0) or 0)
    usd_amount = sol_amount * WS_SOL_PRICE_USD

    # [v4.40] Still-pending mint (not yet promoted to `tokens`, or already
    # dropped) — update its pending snapshot so the one-shot delayed check in
    # _finalize_sol_pending_entry sees real, live-updated data instead of the
    # frozen creation-time estimate. Deliberately does NOT touch `tokens`.
    pending = _pending_sol_entries.get(mint)
    if pending is not None:
        pending["mc_sol"] = mc_sol
        pending["vsol_curve"] = vsol_curve
        if tx_type == "buy":
            pending["ws_buy_count"] += 1
            pending["ws_buy_vol_usd"] += usd_amount
        elif tx_type == "sell":
            pending["ws_sell_count"] += 1
            pending["ws_sell_vol_usd"] += usd_amount

    if mint not in tokens:
        return

    ws_stats["trades_received"] += 1
    ws_stats["last_message_at"] = time.time()

    async with tokens_lock:
        t = tokens.get(mint)
        if t:
            t.ws_sol_volume += sol_amount
            if tx_type == "buy":
                t.ws_buy_count += 1
                t.ws_buy_vol_usd += usd_amount
            elif tx_type == "sell":
                t.ws_sell_count += 1
                t.ws_sell_vol_usd += usd_amount

            # [v4.27] Keep market_cap/liquidity live for pre-graduation tokens.
            # Once DexScreener actually enriches the token (t.liquidity becomes
            # nonzero), defer to it as authoritative and stop overwriting from
            # the bonding-curve estimate — DexScreener reflects real AMM state,
            # the curve no longer determines price post-migration.
            if t.liquidity == 0:
                if mc_sol > 0:
                    new_mc = mc_sol * WS_SOL_PRICE_USD
                    update_mc_velocity(t, new_mc)
                    t.market_cap = new_mc
                if vsol_curve > 0:
                    t.ws_liquidity_estimate = vsol_curve * WS_SOL_PRICE_USD

            update_composite_score(t)


# ============================================================================
# [v4.29] Borsh decoding for direct on-chain pump.fun events
# ============================================================================
# Minimal hand-rolled Anchor/borsh reader — just the primitives pump.fun's
# CreateEvent/TradeEvent structs use. Each _b_read_* takes (buf, offset) and
# returns (value, new_offset) so callers can thread the offset through in
# field order without re-slicing by hand.

def _b_require(buf: bytes, off: int, n: int):
    # Plain slicing silently truncates on a short buffer instead of raising —
    # a malformed/truncated payload would otherwise decode into garbage
    # instead of failing loudly, so every reader checks bounds explicitly.
    if off + n > len(buf) or off < 0:
        raise IndexError(f"borsh read past end of buffer (need {n} bytes at {off}, have {len(buf)})")

def _b_read_u8(buf: bytes, off: int):
    _b_require(buf, off, 1)
    return buf[off], off + 1

def _b_read_u16(buf: bytes, off: int):
    _b_require(buf, off, 2)
    return int.from_bytes(buf[off:off + 2], "little"), off + 2

def _b_read_u64(buf: bytes, off: int):
    _b_require(buf, off, 8)
    return int.from_bytes(buf[off:off + 8], "little", signed=False), off + 8

def _b_read_i64(buf: bytes, off: int):
    _b_require(buf, off, 8)
    return int.from_bytes(buf[off:off + 8], "little", signed=True), off + 8

def _b_read_bool(buf: bytes, off: int):
    _b_require(buf, off, 1)
    return buf[off] != 0, off + 1

def _b_read_pubkey(buf: bytes, off: int):
    _b_require(buf, off, 32)
    raw = buf[off:off + 32]
    try:
        pk = str(PublicKey(raw))
    except Exception:
        pk = raw.hex()
    return pk, off + 32

def _b_read_u32(buf: bytes, off: int):
    _b_require(buf, off, 4)
    return int.from_bytes(buf[off:off + 4], "little"), off + 4

def _b_read_string(buf: bytes, off: int):
    n, off = _b_read_u32(buf, off)
    _b_require(buf, off, n)
    raw = buf[off:off + n]
    return raw.decode("utf-8", errors="replace"), off + n


def _decode_create_event(payload: bytes) -> Optional[dict]:
    """
    Decode a pump.fun CreateEvent (post-discriminator payload) per the field
    order in the official IDL: name, symbol, uri, mint, bonding_curve, user,
    creator, timestamp, virtual_token_reserves, virtual_sol_reserves,
    real_token_reserves, token_total_supply, token_program, is_mayhem_mode,
    is_cashback_enabled, quote_mint, virtual_quote_reserves.
    """
    try:
        off = 0
        name, off = _b_read_string(payload, off)
        symbol, off = _b_read_string(payload, off)
        _uri, off = _b_read_string(payload, off)
        mint, off = _b_read_pubkey(payload, off)
        _bonding_curve, off = _b_read_pubkey(payload, off)
        _user, off = _b_read_pubkey(payload, off)
        _creator, off = _b_read_pubkey(payload, off)
        _timestamp, off = _b_read_i64(payload, off)
        virtual_token_reserves, off = _b_read_u64(payload, off)
        virtual_sol_reserves, off = _b_read_u64(payload, off)
        _real_token_reserves, off = _b_read_u64(payload, off)
        token_total_supply, off = _b_read_u64(payload, off)
        # remaining fields (token_program, is_mayhem_mode, is_cashback_enabled,
        # quote_mint, virtual_quote_reserves) aren't needed for MC/liquidity —
        # virtual_sol_reserves is always SOL-denominated regardless of
        # quote_mint (the IDL tracks quote-currency reserves separately in
        # virtual_quote_reserves), so no quote_mint branching is needed here.
        return {
            "mint": mint,
            "symbol": symbol or name[:10],
            "name": name,
            "virtual_sol_reserves": virtual_sol_reserves,
            "virtual_token_reserves": virtual_token_reserves,
            "token_total_supply": token_total_supply,
        }
    except (IndexError, UnicodeDecodeError, ValueError):
        return None


def _decode_trade_event(payload: bytes) -> Optional[dict]:
    """
    Decode a pump.fun TradeEvent (post-discriminator payload) per the IDL
    field order: mint, sol_amount, token_amount, is_buy, user, timestamp,
    virtual_sol_reserves, virtual_token_reserves, real_sol_reserves,
    real_token_reserves, ... (rest unused here).
    """
    try:
        off = 0
        mint, off = _b_read_pubkey(payload, off)
        sol_amount, off = _b_read_u64(payload, off)
        _token_amount, off = _b_read_u64(payload, off)
        is_buy, off = _b_read_bool(payload, off)
        _user, off = _b_read_pubkey(payload, off)
        _timestamp, off = _b_read_i64(payload, off)
        virtual_sol_reserves, off = _b_read_u64(payload, off)
        virtual_token_reserves, off = _b_read_u64(payload, off)
        return {
            "mint": mint,
            "sol_amount": sol_amount,
            "is_buy": is_buy,
            "virtual_sol_reserves": virtual_sol_reserves,
            "virtual_token_reserves": virtual_token_reserves,
        }
    except (IndexError, ValueError):
        return None


def _pumpfun_mc_sol(virtual_sol_reserves: int, virtual_token_reserves: int, token_total_supply: int) -> float:
    """marketCapSol = (virtual_sol_reserves/1e9) * (token_total_supply/virtual_token_reserves).
    Validated against live logs in production — matches PumpPortal's marketCapSol
    for brand-new tokens to within rounding."""
    if virtual_token_reserves <= 0:
        return 0.0
    return (virtual_sol_reserves / 1e9) * (token_total_supply / virtual_token_reserves)


async def pumpfun_direct_listener():
    """
    [v4.29] Direct on-chain pump.fun indexing via Helius logsSubscribe — reads
    the pump.fun program's own CreateEvent/TradeEvent Anchor logs instead of
    going through the PumpPortal relay. See PUMPFUN_DIRECT_MODE docs above.
    """
    if not HELIUS_WS:
        logger.warning("⚠️ PUMPFUN_DIRECT_MODE set but HELIUS_API_KEY missing — direct listener not starting")
        return

    live = PUMPFUN_DIRECT_MODE == "live"
    delay = WS_RECONNECT_DELAY_MIN

    while True:
        try:
            logger.info(f"🔌 Connecting to Helius WS for direct pump.fun indexing (mode={PUMPFUN_DIRECT_MODE})")
            async with websockets.connect(
                HELIUS_WS,
                ping_interval=25,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:
                pumpfun_direct_stats["connected"] = True
                pumpfun_direct_stats["last_message_at"] = time.time()
                delay = WS_RECONNECT_DELAY_MIN
                logger.info("✅ Helius WS connected (direct pump.fun indexing)")

                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [PUMPFUN_PROGRAM_ID]},
                        {"commitment": "processed"},
                    ],
                }))

                async for message in ws:
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError:
                        continue

                    if "result" in data and "method" not in data:
                        # subscription-ack response, not a notification
                        logger.info(f"📡 Direct pump.fun subscription confirmed (id={data.get('result')})")
                        continue

                    if data.get("method") != "logsNotification":
                        continue

                    pumpfun_direct_stats["last_message_at"] = time.time()

                    try:
                        value = data["params"]["result"]["value"]
                        if value.get("err") is not None:
                            continue  # failed tx — nothing actually happened on-chain
                        logs = value.get("logs") or []
                    except (KeyError, TypeError):
                        continue

                    for line in logs:
                        if not line.startswith("Program data: "):
                            continue
                        try:
                            raw = base64.b64decode(line[len("Program data: "):])
                        except Exception:
                            continue
                        if len(raw) < 8:
                            continue
                        disc, payload = raw[:8], raw[8:]

                        if disc == _CREATE_EVENT_DISC:
                            ev = _decode_create_event(payload)
                            if not ev:
                                pumpfun_direct_stats["decode_errors"] += 1
                                continue
                            pumpfun_direct_stats["creates_decoded"] += 1
                            _pumpfun_supply_cache[ev["mint"]] = ev["token_total_supply"]
                            mc_sol = _pumpfun_mc_sol(
                                ev["virtual_sol_reserves"], ev["virtual_token_reserves"], ev["token_total_supply"]
                            )
                            vsol = ev["virtual_sol_reserves"] / 1e9
                            if live:
                                await _handle_ws_create({
                                    "mint": ev["mint"],
                                    "symbol": ev["symbol"],
                                    "name": ev["name"],
                                    "initialBuy": 0,
                                    "marketCapSol": mc_sol,
                                    "vSolInBondingCurve": vsol,
                                })
                            else:
                                logger.debug(
                                    f"🔬 [shadow] direct CREATE: ${ev['symbol']} ({ev['mint'][:8]}…) "
                                    f"MC≈{mc_sol:.1f} SOL"
                                )

                        elif disc == _TRADE_EVENT_DISC:
                            ev = _decode_trade_event(payload)
                            if not ev:
                                pumpfun_direct_stats["decode_errors"] += 1
                                continue
                            pumpfun_direct_stats["trades_decoded"] += 1
                            supply = _pumpfun_supply_cache.get(ev["mint"], _PUMPFUN_DEFAULT_SUPPLY)
                            mc_sol = _pumpfun_mc_sol(ev["virtual_sol_reserves"], ev["virtual_token_reserves"], supply)
                            vsol = ev["virtual_sol_reserves"] / 1e9
                            if live:
                                await _handle_ws_trade({
                                    "mint": ev["mint"],
                                    "txType": "buy" if ev["is_buy"] else "sell",
                                    "solAmount": ev["sol_amount"] / 1e9,
                                    "marketCapSol": mc_sol,
                                    "vSolInBondingCurve": vsol,
                                })
                            else:
                                logger.debug(
                                    f"🔬 [shadow] direct TRADE: {ev['mint'][:8]}… "
                                    f"{'buy' if ev['is_buy'] else 'sell'} {ev['sol_amount']/1e9:.3f} SOL"
                                )

        except asyncio.CancelledError:
            logger.info("🔌 Direct pump.fun listener cancelled")
            pumpfun_direct_stats["connected"] = False
            break
        except Exception as e:
            pumpfun_direct_stats["connected"] = False
            pumpfun_direct_stats["reconnects"] += 1
            logger.warning(f"🔌 Direct pump.fun WS disconnected: {e} — reconnecting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_RECONNECT_DELAY_MAX)


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
# [v4.25] Alchemy EVM WS Listener — BSC / Base / Ethereum
# ============================================================================

def _topic_to_address(topic_hex: str) -> str:
    """A 32-byte indexed-address topic is left-zero-padded — the address is
    the rightmost 20 bytes (40 hex chars)."""
    return "0x" + topic_hex[-40:]


def _log_data_words(log: dict) -> List[str]:
    """Split a log's non-indexed `data` field into its 32-byte (64 hex char) words."""
    data_hex = (log.get("data") or "0x")[2:]
    return [data_hex[i:i + 64] for i in range(0, len(data_hex), 64)]


# [v4.35] Minimal, dependency-free ABI decoding for four.meme's TokenManager2
# events -- every param is non-indexed (verified, see the FOURMEME_MODE
# comment block above), so everything lives in `data` as a standard Solidity
# ABI tuple encoding: one 32-byte "head" word per param in order (a dynamic
# `string` param's head word is a byte-offset pointer instead of its value),
# followed by a "tail" section holding each dynamic param's length + content.
# No new dependency (web3/eth_abi) needed for the handful of fixed shapes
# actually used here -- same philosophy as this file's existing borsh
# decoder for Solana's pump.fun Anchor events.
def _abi_word_addr(word_hex: str) -> str:
    """A 32-byte address word is left-zero-padded — value is the rightmost 20 bytes."""
    return "0x" + word_hex[-40:]


def _abi_word_uint(word_hex: str) -> int:
    return int(word_hex, 16) if word_hex else 0


def _abi_dyn_string(words: List[str], byte_offset: int) -> str:
    """Read a dynamic `string` param given its head-word byte offset (offset
    is measured from the start of `data`, so word index = offset // 32)."""
    word_idx = byte_offset // 32
    if word_idx < 0 or word_idx >= len(words):
        return ""
    length = _abi_word_uint(words[word_idx])
    n_words = (length + 31) // 32
    content_hex = "".join(words[word_idx + 1: word_idx + 1 + n_words])
    try:
        return bytes.fromhex(content_hex)[:length].decode("utf-8", errors="replace")
    except Exception:
        return ""


def _decode_fourmeme_create(log: dict) -> Optional[dict]:
    """
    TokenCreate(address creator, address token, uint256 requestId,
                string name, string symbol, uint256 totalSupply,
                uint256 launchTime, uint256 launchFee)
    8 head words; name/symbol are dynamic (offset pointers in their head slot).
    """
    words = _log_data_words(log)
    if len(words) < 8:
        return None
    try:
        return {
            "creator": _abi_word_addr(words[0]),
            "token": _abi_word_addr(words[1]),
            "request_id": _abi_word_uint(words[2]),
            "name": _abi_dyn_string(words, _abi_word_uint(words[3])),
            "symbol": _abi_dyn_string(words, _abi_word_uint(words[4])),
            "total_supply": _abi_word_uint(words[5]),
            "launch_time": _abi_word_uint(words[6]),
            "launch_fee": _abi_word_uint(words[7]),
        }
    except Exception:
        return None


def _decode_fourmeme_trade(log: dict) -> Optional[dict]:
    """
    TokenPurchase / TokenSale (V2, identical shape):
    (address token, address account, uint256 price, uint256 amount,
     uint256 cost, uint256 fee, uint256 offers, uint256 funds)
    All 8 params are static (no strings) -- straight positional decode.
    """
    words = _log_data_words(log)
    if len(words) < 8:
        return None
    try:
        return {
            "token": _abi_word_addr(words[0]),
            "account": _abi_word_addr(words[1]),
            "price": _abi_word_uint(words[2]),
            "amount": _abi_word_uint(words[3]),
            "cost": _abi_word_uint(words[4]),
            "fee": _abi_word_uint(words[5]),
            "offers": _abi_word_uint(words[6]),
            "funds": _abi_word_uint(words[7]),
        }
    except Exception:
        return None


_FOURMEME_WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"  # BSC WBNB — four.meme's default raise asset


async def _handle_fourmeme_log(log: dict) -> None:
    """
    Routes a four.meme TokenManager2 log to the right decoder by topic0, then
    either logs it (shadow mode) or feeds it into live tracking. See the
    FOURMEME_MODE comment block above for the off/shadow/live rollout design
    and why fourmeme_raised_usd — not market_cap — is what gets updated here.
    """
    topics = log.get("topics", [])
    if not topics:
        return
    topic0 = topics[0].lower()

    if topic0 == _FOURMEME_CREATE_TOPIC:
        info = _decode_fourmeme_create(log)
        if not info or not info.get("token"):
            fourmeme_stats["decode_errors"] += 1
            return
        fourmeme_stats["creates_decoded"] += 1
        mint = info["token"].lower()

        if FOURMEME_MODE == "shadow":
            logger.info(
                f"🌊 [shadow] four.meme CREATE ${info['symbol']} ({info['name']}) "
                f"{mint[:10]}... supply_raw={info['total_supply']}"
            )
            return

        if mint in tokens or len(tokens) >= MAX_TRACKED_TOKENS:
            return
        async with tokens_lock:
            if mint not in tokens:
                tokens[mint] = TokenInfo(
                    mint=mint, symbol=(info["symbol"] or "???")[:20],
                    name=(info["name"] or "")[:64], created_at=time.time(),
                    chain_id="bsc", ws_discovered=True, ws_pre_enrichment=True,
                )
        fourmeme_stats["tokens_tracked"] += 1
        logger.info(f"🌊 four.meme NEW: ${info['symbol']} ({mint[:10]}...)")

        # Also queue through the normal WS-discovery enrichment pipeline —
        # if/when this token migrates to Pancake (or DexScreener otherwise
        # indexes it), _finalize_ws_token upgrades this exact stub in place
        # with real dex_data, exactly like any other WS-discovered token.
        # Deliberately NOT added to `seen_mints`: that set exists to dedupe
        # the PancakeV2/V3 factory watch, and this token's eventual real
        # migration pair-creation event should still reach that path
        # normally (this queue push alone gives up after ~1 minute if the
        # curve hasn't migrated yet — bonding curves often run far longer).
        if ws_discovery_queue is not None:
            await ws_discovery_queue.put({
                "mint": mint, "symbol": info["symbol"] or "???",
                "name": info["name"] or "", "chain_id": "bsc",
                "created_at": time.time(), "ws_discovered": True,
                "ws_initial_buy_sol": 0.0, "mc_sol_at_creation": 0.0,
            })
        return

    if topic0 not in (_FOURMEME_PURCHASE_TOPIC, _FOURMEME_SALE_TOPIC):
        return

    trade = _decode_fourmeme_trade(log)
    if not trade or not trade.get("token"):
        fourmeme_stats["decode_errors"] += 1
        return
    mint = trade["token"].lower()
    is_buy = topic0 == _FOURMEME_PURCHASE_TOPIC
    fourmeme_stats["purchases_decoded" if is_buy else "sales_decoded"] += 1

    cost_usd = _evm_amount_to_usd("bsc", _FOURMEME_WBNB, trade["cost"])
    funds_usd = _evm_amount_to_usd("bsc", _FOURMEME_WBNB, trade["funds"])

    if FOURMEME_MODE == "shadow":
        logger.debug(
            f"🌊 [shadow] four.meme {'BUY' if is_buy else 'SELL'} {mint[:10]}... "
            f"cost=${cost_usd:,.0f} raised=${funds_usd:,.0f}"
        )
        return

    if mint not in tokens:
        return  # not a token we're tracking (e.g. subscription started mid-curve)
    token = tokens[mint]
    token.fourmeme_raised_usd = funds_usd
    if is_buy:
        token.ws_buy_count += 1
        token.ws_buy_vol_usd += cost_usd
    else:
        token.ws_sell_count += 1
        token.ws_sell_vol_usd += cost_usd


async def _handle_evm_swap_log(chain_id: str, log: dict) -> None:
    """
    Decode a Swap event (V2 In/Out style or V3 signed-delta style) on a pair
    we recorded metadata for at discovery time, and credit the buy/sell volume
    onto the matching tracked token — same fields the Solana WS trade handler
    populates (ws_buy_count/ws_sell_count/ws_buy_vol_usd/ws_sell_vol_usd),
    which is what activates the existing eth_vol_signal/eth_vol_accel alert
    paths for EVM chains.
    """
    pair_addr = (log.get("address") or "").lower()
    meta = evm_pair_meta[chain_id].get(pair_addr)
    if not meta:
        return
    mint = meta["mint"]
    if mint not in tokens:
        return

    topics = log.get("topics", [])
    if not topics:
        return
    topic0 = topics[0].lower()
    words = _log_data_words(log)

    try:
        if topic0 == _V2_SWAP_TOPIC and len(words) >= 4:
            amount0_in = int(words[0], 16)
            amount1_in = int(words[1], 16)
            amount0_out = int(words[2], 16)
            amount1_out = int(words[3], 16)
            if meta["base_is_token0"]:
                base_in, base_out = amount0_in, amount0_out
            else:
                base_in, base_out = amount1_in, amount1_out
            is_buy = base_in > 0
            base_raw = base_in if is_buy else base_out
        elif topic0 == _V3_SWAP_TOPIC and len(words) >= 2:
            amount0 = _hex_word_to_signed_int(words[0])
            amount1 = _hex_word_to_signed_int(words[1])
            base_delta = amount0 if meta["base_is_token0"] else amount1
            is_buy = base_delta > 0
            base_raw = abs(base_delta)
        else:
            return
    except (ValueError, IndexError):
        return

    if base_raw <= 0:
        return

    usd_amount = _evm_amount_to_usd(chain_id, meta["base_token"], base_raw)
    if usd_amount <= 0:
        return

    evm_ws_stats[chain_id]["trades_received"] += 1

    async with tokens_lock:
        t = tokens.get(mint)
        if not t:
            return
        if is_buy:
            t.ws_buy_count += 1
            t.ws_buy_vol_usd += usd_amount
        else:
            t.ws_sell_count += 1
            t.ws_sell_vol_usd += usd_amount


async def _refresh_evm_swap_subs(ws, chain_id: str) -> None:
    """
    Background loop (one per active connection): periodically rebuild the
    Swap-event subscription's address list from whichever discovered pairs
    currently have a live tracked token, capped at EVM_MAX_SWAP_SUBS. Alchemy's
    eth_subscribe has no "add address" call — refreshing means unsubscribing
    the old filter and subscribing a new one with the full address list.
    """
    while True:
        await asyncio.sleep(EVM_SWAP_RESUB_INTERVAL)
        try:
            meta = evm_pair_meta[chain_id]
            if not meta:
                continue
            candidates = [pair for pair, info in meta.items() if info["mint"] in tokens][:EVM_MAX_SWAP_SUBS]
            candidate_set = set(candidates)
            if candidate_set == evm_swap_subscribed[chain_id]:
                continue  # nothing changed — skip the resubscribe round-trip

            old_sub_id = evm_swap_sub_id.get(chain_id)
            if old_sub_id:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 8, "method": "eth_unsubscribe", "params": [old_sub_id],
                }))

            if not candidates:
                evm_swap_subscribed[chain_id] = set()
                evm_swap_sub_id[chain_id] = None
                evm_ws_stats[chain_id]["swap_subs"] = 0
                continue

            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": 2, "method": "eth_subscribe",
                "params": ["logs", {"address": candidates, "topics": [[_V2_SWAP_TOPIC, _V3_SWAP_TOPIC]]}],
            }))
            evm_swap_subscribed[chain_id] = candidate_set
            evm_ws_stats[chain_id]["swap_subs"] = len(candidate_set)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug(f"EVM swap resub error [{chain_id}]: {e}")


async def evm_ws_listener(chain_id: str):
    """
    Push-based new-pair discovery AND live trade/volume feed for an EVM chain
    via Alchemy WS, multiplexed over one connection.

    Subscription 1 (factory logs, request id 1): PairCreated (V2-style) /
    PoolCreated (V3-style) on the chain's known DEX factories. Decodes the
    newly-listed token address out of the log's indexed topics and feeds it
    into the same ws_discovery_queue the Solana WS path uses — enrichment
    (get_dex_data, threshold checks, TokenInfo creation) is already
    chain-agnostic. Also records the pair/pool address + which side is the
    base token, so subscription 2 can attribute swaps correctly.

    Subscription 2 (swap logs, request id 2, refreshed by
    _refresh_evm_swap_subs): Swap events on the pairs of currently-tracked
    tokens. Credits buy/sell volume onto ws_buy_count/ws_sell_count/
    ws_buy_vol_usd/ws_sell_vol_usd — the same TokenInfo fields the Solana WS
    trade handler populates, activating the existing eth_vol_signal /
    eth_vol_accel alert paths for EVM chains for the first time.

    Both subscriptions arrive as eth_subscription pushes on the same socket,
    distinguished by their "subscription" id — routed via evm_factory_sub_id /
    evm_swap_sub_id, captured from each subscribe call's ack.

    No-ops (logs once, returns) if ALCHEMY_API_KEY isn't set or this chain has
    no configured factories — the existing DexScreener poll loop keeps running
    as the only discovery path in that case, exactly as before this feature.
    """
    if not _alchemy_key_for(chain_id):
        logger.info(f"⏭️  Alchemy WS skipped for {chain_id} — no API key configured (polling only)")
        return

    factories = EVM_FACTORIES.get(chain_id, [])
    ws_url = ALCHEMY_WS_URLS.get(chain_id)
    if not factories or not ws_url:
        return

    addresses = [f["address"] for f in factories]
    topics = list({f["topic"] for f in factories})  # OR'd within this topic slot
    base_tokens = EVM_BASE_TOKENS.get(chain_id, set())
    base_token_decimals = EVM_BASE_TOKEN_DECIMALS.get(chain_id, {})
    # [v4.30] Purely a log label — when two factories on the same chain share a
    # topic (e.g. UniswapV3 and SushiV3 both emit the standard PoolCreated),
    # the later entry's name wins here for whichever one is logged. Detection
    # itself is unaffected: eth_subscribe filters by the full address list, so
    # every factory below is watched regardless of any topic collision.
    dex_by_topic = {f["topic"]: f["dex"] for f in factories}

    delay = WS_EVM_RECONNECT_DELAY_MIN
    stats = evm_ws_stats[chain_id]
    chain_label = get_chain(chain_id)["label"]

    while True:
        resub_task = None
        try:
            logger.info(f"🔌 Connecting to Alchemy WS [{chain_label}]")
            async with websockets.connect(
                ws_url,
                ping_interval=45,
                ping_timeout=25,
                close_timeout=5,
            ) as ws:
                stats["connected"] = True
                stats["last_message_at"] = time.time()
                delay = WS_EVM_RECONNECT_DELAY_MIN
                logger.info(f"✅ Alchemy WS connected [{chain_label}]")

                # Fresh connection = fresh subscription ids; a stale swap
                # filter from a previous connection no longer exists server-side.
                evm_swap_subscribed[chain_id] = set()
                evm_swap_sub_id[chain_id] = None
                evm_factory_sub_id[chain_id] = None
                fourmeme_sub_id[chain_id] = None

                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["logs", {"address": addresses, "topics": [topics]}],
                }))

                resub_task = asyncio.create_task(_refresh_evm_swap_subs(ws, chain_id))

                # [v4.35] four.meme -- third subscription, BSC only, off by default
                if chain_id == "bsc" and FOURMEME_MODE != "off":
                    await ws.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "eth_subscribe",
                        "params": ["logs", {
                            "address": FOURMEME_TOKEN_MANAGER2,
                            "topics": [[_FOURMEME_CREATE_TOPIC, _FOURMEME_PURCHASE_TOPIC, _FOURMEME_SALE_TOPIC]],
                        }],
                    }))
                    fourmeme_stats["connected"] = True
                    logger.info(f"🌊 four.meme WS subscribed [{FOURMEME_MODE}]")

                async for message in ws:
                    try:
                        data = json.loads(message)
                        stats["last_message_at"] = time.time()

                        # Subscription ack: {"id":N,"result":"0x..."}
                        if "result" in data and "params" not in data:
                            req_id = data.get("id")
                            if req_id == 1:
                                evm_factory_sub_id[chain_id] = data["result"]
                            elif req_id == 2:
                                evm_swap_sub_id[chain_id] = data["result"]
                            elif req_id == 3:
                                fourmeme_sub_id[chain_id] = data["result"]
                            continue

                        params = data.get("params") or {}
                        log = params.get("result")
                        if not log:
                            continue

                        # Route by subscription id — swap feed vs factory feed vs four.meme
                        sub_id = params.get("subscription")
                        if sub_id is not None and sub_id == evm_swap_sub_id.get(chain_id):
                            await _handle_evm_swap_log(chain_id, log)
                            continue
                        if sub_id is not None and sub_id == fourmeme_sub_id.get(chain_id):
                            await _handle_fourmeme_log(log)
                            continue

                        log_topics = log.get("topics", [])
                        if len(log_topics) < 3:
                            continue

                        topic0 = log_topics[0].lower()
                        dex_name = dex_by_topic.get(topic0, "?")
                        token0 = _topic_to_address(log_topics[1]).lower()
                        token1 = _topic_to_address(log_topics[2]).lower()

                        if token0 in base_tokens and token1 not in base_tokens:
                            new_token, base_token_addr = token1, token0
                        elif token1 in base_tokens and token0 not in base_tokens:
                            new_token, base_token_addr = token0, token1
                        elif token0 not in base_tokens and token1 not in base_tokens:
                            new_token, base_token_addr = token0, token1  # neither side recognized — best effort
                        else:
                            continue  # both sides are base tokens (e.g. USDC/WETH) — not a new listing

                        if new_token in seen_mints:
                            continue
                        seen_mints.add(new_token)
                        stats["pairs_discovered"] += 1
                        logger.info(f"⚡ WS NEW [{chain_label}/{dex_name}]: {new_token[:10]}...")

                        # Record pair metadata (needed by the swap feed) only when
                        # we recognize the base-token side well enough to price it.
                        pair_addr = None
                        if base_token_addr in base_token_decimals:
                            words = _log_data_words(log)
                            if topic0 == _PAIR_CREATED_TOPIC and words:
                                pair_addr = "0x" + words[0][-40:]
                            elif topic0 == _POOL_CREATED_TOPIC and len(words) >= 2:
                                pair_addr = "0x" + words[1][-40:]
                            elif topic0 == _SOLIDLY_POOL_CREATED_TOPIC and words:
                                # data = [pool, count] — same layout as PairCreated
                                pair_addr = "0x" + words[0][-40:]
                            if pair_addr:
                                evm_pair_meta[chain_id][pair_addr] = {
                                    "mint": new_token,
                                    "base_token": base_token_addr,
                                    "base_is_token0": base_token_addr == token0,
                                    "dex": dex_name,
                                }

                        created_ts = time.time()
                        creation_dt = {
                            "mint": new_token,
                            "symbol": "NEW",
                            "name": "NEW",
                            "created_at": created_ts,
                            "ws_discovered": True,
                            "ws_initial_buy_sol": 0.0,
                            "mc_sol_at_creation": 0.0,
                            "chain_id": chain_id,
                        }

                        # [v4.36->v4.39] The immediate pre-track stub that used to be
                        # created here (before any real MC data existed) is REMOVED —
                        # by explicit request, this bot no longer tracks a token at
                        # the instant it's created. A PairCreated/PoolCreated event
                        # carries no reserves, so there was never a real MC estimate
                        # possible here anyway (only the FRESH🌱 path, now dormant,
                        # existed to alert off raw trade counters with mc still at 0).
                        # Pair metadata (evm_pair_meta, above) still gets recorded
                        # regardless — that's needed for _handle_evm_swap_log to
                        # attribute swaps correctly IF this pair later becomes a
                        # tracked token via the queue below (once DexScreener returns
                        # real MC data) or via fetch_chain_new_pairs' own poll. The
                        # entry gate (mc within CHAIN_THRESHOLDS mc_min..mc_max) is
                        # enforced once, at that first-real-data moment, in
                        # _finalize_ws_token and the polling-loop creation path —
                        # see v4.39 comments there. No re-checking after that.
                        if ws_discovery_queue is not None:
                            await ws_discovery_queue.put(creation_dt)

                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        logger.debug(f"Alchemy WS [{chain_label}] message error: {e}")

        except asyncio.CancelledError:
            logger.info(f"🔌 Alchemy WS [{chain_label}] listener cancelled")
            stats["connected"] = False
            break
        except Exception as e:
            stats["connected"] = False
            stats["reconnects"] += 1
            hint = ""
            if "403" in str(e):
                hint = (f" — HTTP 403 usually means this Alchemy app doesn't have "
                        f"the {chain_label} network enabled (Alchemy apps are "
                        f"network-scoped in the dashboard); add it there, or set "
                        f"ALCHEMY_API_KEY_{chain_id.upper()} to a key from an app that has it")
            logger.warning(f"🔌 Alchemy WS [{chain_label}] disconnected: {e}{hint} — reconnecting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, WS_EVM_RECONNECT_DELAY_MAX)
        finally:
            if resub_task is not None:
                resub_task.cancel()
                try:
                    await resub_task
                except (asyncio.CancelledError, Exception):
                    pass

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

    [v4.30] The combined DEXSCREENER_NEW_PAIRS feed spans every chain
    DexScreener tracks (dozens), and Solana is by far the highest-volume new-
    pair chain on it — that stayed true even with Solana disabled here, since
    real-world Solana launch activity doesn't stop just because this bot
    isn't tracking it. The old `TOKENS_PER_POLL * len(ENABLED_CHAIN_IDS)` slice
    shrank as chains got disabled, which is backwards: fewer enabled chains
    means a SMALLER share of the combined feed's top-N is relevant, so the
    slice needs to scan deeper, not shallower, to surface enough BSC/Base/ETH
    entries once Solana's share of that same window is discarded.
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
                for profile in profiles[:NEW_PAIRS_SCAN_DEPTH]:
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


def format_gem_alert(token: TokenInfo, alert_reason: str = "GEM", distro: "DistroResult" = None, security: "SecurityResult" = None) -> str:
    age_str = format_launch_age(token.launched_at or token.created_at)
    chain = get_chain(token.chain_id)
    _dex_url = dex_url(token.mint, token.chain_id)
    safe_name   = html.escape(token.name   or "Unknown")
    safe_symbol = html.escape(token.symbol or "???")

    SIGNAL_LABELS = {
        "FAST🔥": "⚡ Fast Runner", "EARLY📈": "📈 Early Entry",
        "VOL💰": "💰 Vol Spike", "VACCEL📊": "📊 Vol Accel", "KOL🐦": "🐦 KOL", "GEM": "💎 Gem",
        "RUNNER🚀": "🚀 Runner (already running)",
        "CURVE🌊": "🌊 four.meme Curve",
        "FRESH🌱": "🌱 Fresh (pre-index buy pressure)",
    }
    signal_label = SIGNAL_LABELS.get(alert_reason, f"💎 {alert_reason}")
    # [v4.27] Same WS-derived fallback run_detections used to decide to alert —
    # without it, a bonding-curve runner's card would misleadingly show $0
    # Vol/Liq and a stale 50% buy bar even though real activity is what
    # triggered the alert in the first place.
    disp_liq, disp_vol, disp_buy_ratio = effective_liq_vol_buyratio(token)
    buy_pct = int(disp_buy_ratio * 100)
    bp_filled = buy_pct // 10
    bp_bar = "█" * bp_filled + "░" * (10 - bp_filled)

    # [v4.35] four.meme pre-migration stubs have no real market_cap (see
    # TokenInfo.fourmeme_raised_usd) — show the curve-progress number instead
    # of a misleading "MC $0".
    if alert_reason == "CURVE🌊":
        mc_s = f"{fmt_usd_short(token.fourmeme_raised_usd)} raised (of ~{FOURMEME_GRADUATION_BNB:.0f} BNB curve)"
    elif alert_reason == "FRESH🌱":
        # [v4.36] No DexScreener data yet for this stub — a literal "$0" here
        # would read as a dead/worthless token when the opposite is true.
        mc_s = "pending (not yet indexed)"
    else:
        mc_s = fmt_usd_short(token.market_cap)
    vol_s = fmt_usd_short(disp_vol)
    # [v4.36] Same reasoning as mc_s above — no liquidity proxy exists for a
    # vanilla EVM pair pre-DexScreener, so "$0" here would misleadingly read
    # as no liquidity at all rather than "not known yet."
    liq_s = "n/a (pre-index)" if alert_reason == "FRESH🌱" else fmt_usd_short(disp_liq)
    txn_str = f"{token.buys_h1}B / {token.sells_h1}S" if (token.buys_h1 or token.sells_h1) else "—"

    # Signal badges (compact)
    badges = []
    if is_high_velocity(token):          badges.append(f"🔥 +{token.mc_velocity:.0f}%")
    if is_vol_accelerating(token):       badges.append("📈 Vol↑")
    if token.ws_discovered:              badges.append("⚡ Live")
    if token.is_boosted:                 badges.append("🚀 Boosted")
    if token.chain_id in ("ethereum", "bsc", "base", "robinhood") and token.buy_volume_h1 > 0:
        badges.append(f"💰 {fmt_usd_short(token.buy_volume_h1)} buy")
    if token.price_change_h1 and abs(token.price_change_h1) > 1:
        arrow = "▲" if token.price_change_h1 > 0 else "▼"
        badges.append(f"{'🟢' if token.price_change_h1 > 0 else '🔴'} {token.price_change_h1:+.0f}% {arrow}")
    if distro and distro.data_available: badges.append(format_distro_line(distro))

    # Chain-specific links
    if alert_reason == "CURVE🌊":
        # Pre-migration four.meme token — no DexScreener pair exists yet,
        # a dexscreener.com link here would just 404.
        links = f"<a href='https://four.meme/token/{token.mint}'>four.meme</a> · <a href='https://bscscan.com/token/{token.mint}'>Scan</a>"
    elif token.chain_id == "solana":
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://pump.fun/{token.mint}'>Pump</a>"
    elif token.chain_id == "bsc":
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://dextools.io/app/en/bnb/pair-explorer/{token.mint}'>Tools</a>"
    elif token.chain_id == "base":
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://basescan.org/token/{token.mint}'>Scan</a>"
    elif token.chain_id == "ethereum":
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://etherscan.io/token/{token.mint}'>Scan</a>"
    elif token.chain_id == "robinhood":
        # [v4.37] hoodexplorer.org — Blockscout-style explorer for Robinhood
        # Chain. URL pattern not independently verified against a live page
        # (fetch blocked by its robots.txt from this environment); cosmetic
        # link only, worst case a 404, no effect on detection/alerting.
        links = f"<a href='{_dex_url}'>DEX</a> · <a href='https://www.hoodexplorer.org/token/{token.mint}'>Scan</a>"
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
    # [v4.43] GoPlus security warning — always shown in full (not subject to
    # the badges[:4] cap above) since the whole point is that the user
    # actually sees it, not that it gets silently crowded out.
    if security and security.checked and security.has_risk:
        lines.append("⚠️ <b>Security:</b> " + " · ".join(security.risk_reasons[:4]))
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
RH_MIN_BUY_VOL_H1    = 8_000     # [v4.37] Robinhood Chain — no tuning history yet, starts at BSC's level


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
    if chain not in ("ethereum", "bsc", "base", "robinhood"):
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
    elif chain == "robinhood":
        min_vol = RH_MIN_BUY_VOL_H1
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

# [v4.43] EVM scam/honeypot screening (GoPlus Token Security API), per user
# request. Chosen strictness: "honeypot + tax + owner risk" -- flags any of
# a true honeypot, an extreme buy/sell tax, or the deployer retaining
# dangerous privileges (mint more supply, blacklist/hide the owner, pause
# trading, self-destruct). Per user's explicit choice this only WARNS
# (adds a "⚠️ Security" line to the alert card, see format_gem_alert) --
# it never blocks an alert, unlike the Solana distro/bundle check above.
GOPLUS_CHAIN_IDS = {
    "ethereum": "1",
    "bsc": "56",
    "base": "8453",
    # Robinhood Chain (4663) isn't indexed by GoPlus yet -- absent from this
    # map means check_evm_token_security no-ops for it, same as any other
    # unsupported chain or a failed/timed-out lookup (fail open: no badge,
    # alert proceeds exactly as it would with no security data at all).
}
SECURITY_CHECK_TIMEOUT = 5.0
SECURITY_HIGH_TAX_PCT = float(os.getenv("SECURITY_HIGH_TAX_PCT", "25"))  # buy/sell tax %% considered a red flag


@dataclass
class SecurityResult:
    checked: bool = False          # True only if GoPlus actually returned data
    is_honeypot: bool = False
    buy_tax_pct: float = 0.0
    sell_tax_pct: float = 0.0
    high_tax: bool = False
    owner_risk: bool = False
    risk_reasons: List[str] = field(default_factory=list)

    @property
    def has_risk(self) -> bool:
        return self.is_honeypot or self.high_tax or self.owner_risk


async def check_evm_token_security(session: aiohttp.ClientSession, chain_id: str, token_address: str) -> SecurityResult:
    """
    [v4.43] Screens an EVM token through GoPlus's free Token Security API
    for honeypot / high-tax / dangerous-owner-privilege red flags. Fails
    open on anything unexpected (unsupported chain, API down/slow,
    malformed response) -- returns checked=False and the alert proceeds
    exactly as it would with no security data, since this check only ever
    adds a warning badge and must never be able to suppress a real alert.
    """
    result = SecurityResult()
    goplus_chain = GOPLUS_CHAIN_IDS.get(chain_id)
    if not goplus_chain or not token_address:
        return result
    try:
        url = f"{GOPLUS_API}/token_security/{goplus_chain}"
        async with session.get(
            url,
            params={"contract_addresses": token_address.lower()},
            timeout=aiohttp.ClientTimeout(total=SECURITY_CHECK_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return result
            data = await resp.json()
            info = (data.get("result") or {}).get(token_address.lower())
            if not info:
                return result

            result.checked = True

            def _flag(key: str) -> bool:
                return str(info.get(key, "0")) == "1"

            def _pct(key: str) -> float:
                try:
                    return float(info.get(key, 0) or 0) * 100
                except (TypeError, ValueError):
                    return 0.0

            result.is_honeypot = _flag("is_honeypot")
            result.buy_tax_pct = _pct("buy_tax")
            result.sell_tax_pct = _pct("sell_tax")
            result.high_tax = (
                result.buy_tax_pct >= SECURITY_HIGH_TAX_PCT
                or result.sell_tax_pct >= SECURITY_HIGH_TAX_PCT
            )
            result.owner_risk = (
                _flag("is_mintable") or _flag("hidden_owner")
                or _flag("can_take_back_ownership") or _flag("owner_change_balance")
                or _flag("selfdestruct") or _flag("transfer_pausable")
            )

            if result.is_honeypot:
                result.risk_reasons.append("Honeypot (can't sell)")
            if result.high_tax:
                result.risk_reasons.append(f"Tax {result.buy_tax_pct:.0f}%/{result.sell_tax_pct:.0f}% buy/sell")
            if _flag("is_mintable"):
                result.risk_reasons.append("Mintable supply")
            if _flag("hidden_owner"):
                result.risk_reasons.append("Hidden owner")
            if _flag("can_take_back_ownership"):
                result.risk_reasons.append("Ownership reclaimable")
            if _flag("owner_change_balance"):
                result.risk_reasons.append("Owner can edit balances")
            if _flag("selfdestruct"):
                result.risk_reasons.append("Selfdestruct function")
            if _flag("transfer_pausable"):
                result.risk_reasons.append("Trading pausable")

    except Exception as e:
        logger.debug(f"GoPlus security check error ({chain_id} {token_address[:10] if token_address else '?'}...): {e}")
    return result

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

        # [v4.27] Pre-graduation Solana bonding-curve fallback. DexScreener has
        # no pair at all for a pump.fun token until it migrates off the curve —
        # token.liquidity/volume_usd stay at their dataclass default (0.0)
        # forever for such tokens, which made liq_ok/vol_ok permanently
        # unsatisfiable regardless of how much the token was actually pumping
        # on-curve. market_cap itself is fixed live by the WS trade handler
        # (_handle_ws_trade); liq/vol/buy_ratio use WS-derived proxies here via
        # effective_liq_vol_buyratio, but ONLY while token.liquidity == 0 (i.e.
        # still unenriched) — once DexScreener actually reports real liquidity,
        # that's authoritative and the fallback is skipped entirely.
        liq, vol, live_buy_ratio = effective_liq_vol_buyratio(token)

        # [v4.10] Load per-chain thresholds
        th = get_thresholds(token.chain_id)
        age_min      = th["age_min"]
        age_max      = th["age_max"]
        mc_min       = th["mc_min"]
        mc_max       = th["mc_max"]
        vol_min      = th["vol_min"]
        vol_max      = th["vol_max"]
        liq_min      = th["liq_min"]
        buy_ratio_min = th["buy_ratio_min"]
        min_buys_h1  = th["min_buys_h1"]

        # ── Base gates ──────────────────────────────────────────────────────
        age_ok   = age_min <= age_s <= age_max
        mc_ok    = mc_min <= mc <= mc_max
        # [v4.39] Was a floor-only vol/mc PERCENTAGE ratio; now an absolute-
        # dollar volume BAND, independent of mc — see CHAIN_THRESHOLDS. Only
        # for the "entry window" paths (standard/fast_velocity), which already
        # share this chain's mc_min..mc_max ceiling — see vol_ok_momentum
        # below for why momentum_running (which explicitly operates ABOVE
        # mc_max) does NOT reuse this same band.
        vol_ok   = vol_min <= vol <= vol_max
        # momentum_running catches tokens already past mc_max on real,
        # CURRENT acceleration (its own velocity/accel checks below already
        # gate that) — real volume at that size routinely blows past this
        # chain's entry-tier vol_max (e.g. a $500k-mc runner easily clears
        # $50k+ volume), so capping it at the same ceiling as a $10-50k-mc
        # entry would silently make that path nearly unreachable. Floor only.
        vol_ok_momentum = vol >= vol_min
        liq_ok   = liq >= liq_min

        # ── Buy pressure gates ────────────────────────────────────────────
        # Buy ratio: must be buys-dominant (filters dumps). Uses the live
        # WS-derived ratio pre-graduation (see fallback block above) once
        # there's enough trade volume to trust it — otherwise the dataclass
        # default of 0.5 would fail this gate for every brand-new token.
        buy_ratio_ok = live_buy_ratio >= buy_ratio_min
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
            # [v4.34 fix] was a hardcoded floor of 10 regardless of chain --
            # for bsc/base/eth (min_buys_h1 now 5/5/2 after this session's
            # loosening pass) that floor was HIGHER than the chain's own
            # full standard-path threshold, silently overriding the tuning
            # above for this path specifically. Scale with the chain instead.
            and token.buys_h1 >= max(min_buys_h1 // 2, 3)
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
            token.chain_id in ("ethereum", "bsc", "base", "robinhood")
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

        # [v4.38 fix] mc_ok was missing here -- eth_vol_signal (this path's
        # sibling, right above) has always gated on it, but this one never
        # did. Confirmed live in production logs: a VACCEL📊 alert fired for
        # $NEW with MC=$6,164,227,125 (a clearly corrupted/misparsed market
        # cap -- Vol was only $21,170, i.e. a vol/mc ratio of ~0.0000034,
        # nowhere near any real gate) because nothing here checked mc at
        # all. Adding the same mc_ok bound eth_vol_signal already uses closes
        # that hole -- any mc outside the chain's normal mc_min..mc_max window
        # (garbage-low OR garbage-high) can no longer reach this path.
        eth_vol_accel = (
            token.chain_id in ("ethereum", "bsc", "base", "robinhood")
            and is_buy_vol_accelerating(token)
            and is_buy_vol_significant(token)
            and liq_ok
            and mc_ok
            and price_not_crashing
            and mc_not_collapsed
            and not_suppressed
            and age_s >= 10 * 60
            and age_s <= age_max
        )

        # ── [v4.34] Momentum path: catch tokens already running past mc_max ──
        # Every path above requires mc <= mc_max (~$50k) -- meaning a token
        # that already broke past that ceiling on real, current buy pressure
        # was previously invisible to every single alert path, no matter how
        # strong its signal. This path is intentionally disjoint from all of
        # them (mc > mc_max), so it only ever catches what they structurally
        # couldn't -- it doesn't loosen or duplicate any existing behavior.
        # Deliberately stricter than the "catch it cheap" paths on the one
        # axis that matters here (evidence of CURRENT acceleration, not just
        # a healthy ratio) since there's no age/MC cheapness gate doing any
        # of that filtering for us at this size.
        momentum_running = (
            RUNNER_ENABLED
            and mc > mc_max
            and mc <= GEM_MOMENTUM_MC_MAX
            and age_s <= age_max
            and vol_ok_momentum
            and liq_ok
            and buy_ratio_ok
            and min_buys_ok
            and (
                token.mc_velocity >= GEM_FAST_VELOCITY
                or is_vol_accelerating(token)
                or (token.chain_id in ("ethereum", "bsc", "base", "robinhood") and is_buy_vol_accelerating(token))
            )
            and price_not_crashing and mc_not_collapsed and not_suppressed
        )

        # ── [v4.35] four.meme curve-momentum path (BSC bonding-curve, pre-migration) ──
        # Uses fourmeme_raised_usd (real BNB raised, decimal-safe) instead of
        # token.market_cap, which is intentionally left at 0 for these stubs
        # (see FOURMEME_MODE comment block / TokenInfo.fourmeme_raised_usd) --
        # this can never collide with the mc_min/mc_max-gated paths above.
        fourmeme_curve_running = False
        if (token.chain_id == "bsc" and token.ws_pre_enrichment
                and token.fourmeme_raised_usd > 0):
            ws_total_fm = token.ws_buy_count + token.ws_sell_count
            fourmeme_curve_running = (
                FOURMEME_MIN_RAISED_USD <= token.fourmeme_raised_usd <= FOURMEME_MAX_RAISED_USD
                and ws_total_fm >= 5
                and token.ws_buy_count / max(token.ws_sell_count, 1) >= GEM_WS_BUY_PRESSURE
                and not_suppressed
            )

        # ── [v4.36] FRESH🌱 path: vanilla EVM pair, real buy pressure, no
        # DexScreener data yet ────────────────────────────────────────────
        # See the pre-track stub added in evm_ws_listener: PancakeV2/V3,
        # Uniswap, Sushi, and Aerodrome pairs are now tracked from the instant
        # they're created, same as pump.fun/four.meme, instead of only once
        # DexScreener returns data (which could take ~55s or fail entirely).
        # No liquidity/MC exists for these stubs (no reserves in a
        # PairCreated event, unlike a bonding curve's raised amount) so this
        # is gated on real dollar volume + trade count + buy dominance from
        # the raw swap counters instead — same shape as fourmeme_curve_running
        # above, deliberately kept off every mc_min/mc_max-gated path since
        # there's no verified price to gate on. Stops firing the moment
        # DexScreener enriches (liquidity becomes > 0) and the normal paths
        # take over from there.
        evm_fresh_momentum = False
        if (token.chain_id in ("ethereum", "bsc", "base", "robinhood")
                and token.ws_pre_enrichment and token.liquidity == 0):
            ws_total_fresh = token.ws_buy_count + token.ws_sell_count
            ws_usd_fresh = token.ws_buy_vol_usd + token.ws_sell_vol_usd
            evm_fresh_momentum = (
                ws_total_fresh >= EVM_FRESH_MIN_TRADES
                and ws_usd_fresh >= EVM_FRESH_MIN_USD_VOL
                and token.ws_buy_count / max(token.ws_sell_count, 1) >= GEM_WS_BUY_PRESSURE
                and not_suppressed
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
        elif momentum_running:
            alert_reason = "RUNNER🚀"
        elif fourmeme_curve_running:
            alert_reason = "CURVE🌊"
            fourmeme_stats["curve_alerts"] += 1
        elif evm_fresh_momentum:
            alert_reason = "FRESH🌱"
            evm_ws_stats[token.chain_id]["fresh_alerts"] += 1

        if alert_reason:
            # [v4.10] Distribution + bundle check (Solana only, non-blocking)
            distro = DistroResult()  # default: passed=True
            if token.chain_id == "solana" and HELIUS_RPC:
                try:
                    # [v4.31 fix] `asyncio` is already imported at module level
                    # (top of file) — a local `import asyncio` here shadowed it
                    # for this ENTIRE function (Python scoping: any name bound
                    # anywhere in a function body, even inside an `if`, makes
                    # that name local throughout the whole function). With
                    # Solana disabled this branch never runs, so `asyncio` was
                    # never locally bound, and the new alert_db logging call
                    # below — a plain `asyncio.create_task(...)` — raised
                    # "cannot access local variable 'asyncio'" on every single
                    # alert. Removed the redundant local import; the module-
                    # level one already covers this whole file.
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

            # [v4.43] EVM scam/honeypot screening (GoPlus), warn-only per user
            # choice — never blocks the alert, only adds a badge below.
            security = SecurityResult()
            if token.chain_id in GOPLUS_CHAIN_IDS:
                try:
                    security = await asyncio.wait_for(
                        check_evm_token_security(session, token.chain_id, token.mint),
                        timeout=SECURITY_CHECK_TIMEOUT + 1
                    )
                except Exception as e:
                    logger.debug(f"Security check error for {token.mint[:10]}: {e}")
                if security.checked and security.has_risk:
                    logger.info(f"⚠️ Security flag [{token.symbol}]: {', '.join(security.risk_reasons)}")

            token.alerted = True
            token.last_alerted = now
            token.alert_mc = mc
            # [v4.26] Record this as the "winner" for its ticker so later mints
            # reusing the exact same symbol get caught by the squat-on-winner guard.
            alerted_symbol_registry[token.symbol.upper().strip()] = {
                "mint": token.mint, "chain_id": token.chain_id, "alerted_at": now,
            }
            msg_id = await _send_telegram_direct(format_gem_alert(token, alert_reason, distro, security))
            if msg_id:
                token.alert_message_id = msg_id  # store for reply-threading
            src = "⚡WS" if token.ws_discovered else "📡Poll"
            buy_info = f"BR={live_buy_ratio:.0%} B1h={token.buys_h1}"
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
            # [v4.31] Durable copy in the SQLite alert-history log — unlike
            # _analysis_ring (capped, resets on restart) this survives restarts
            # and is queryable via /db/alerts. Fire-and-forget: a slow/failed
            # disk write must never delay the alert path that triggered it.
            asyncio.create_task(alert_db.log_alert_async({
                "mint": token.mint, "symbol": token.symbol, "chain_id": token.chain_id,
                "alert_reason": alert_reason, "alerted_at": now, "market_cap": mc,
                "volume_usd": vol, "liquidity": liq, "buy_ratio": token.buy_ratio,
                "buys_h1": token.buys_h1, "ws_discovered": token.ws_discovered,
                "age_seconds": age_s,
            }))
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
                skip = f"vol ${vol:,.0f} (need ${vol_min:,.0f}-${vol_max:,.0f})"
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

_WS_ENRICH_SKIP_SYMBOLS = {"SOL", "USDC", "USDT", "BTC", "ETH", "BONK", "WIF", "JUP", "RAY", "BSC", "BASE", "BNB", "WBNB", "WETH", "WBTC", "DAI", "BUSD", "CAKE", "ETHEREUM", "UNISWAP", "UNI", "LINK", "AAVE", "MKR", "COMP", "SNX"}


async def _finalize_ws_token(mint: str, token_data: dict, chain_id: str, dex_data: Optional[Dict], sym: str, mc: float) -> None:
    """
    Shared finalize step for a WS-discovered token — used both right after a
    successful first-attempt fetch and from the background retry path below
    (on eventual success, or once retries are exhausted and we're finalizing
    with whatever's available). Upgrades an existing pre-track stub in place,
    or creates a new TokenInfo if this token was never pre-tracked (e.g. a
    KOL-resolved mint, or an EVM pair — those have no pre-track stub).
    """
    async with tokens_lock:
        real_launch = (dex_data.get("launched_at", 0.0) if dex_data else 0.0)
        final_chain = (dex_data.get("chain_id") if dex_data else None) or chain_id

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
            t.volume_h1_total = dex_data.get("volume_h1_total", 0.0) if dex_data else t.volume_h1_total
            t.buy_volume_h1 = dex_data.get("buy_volume_h1", 0.0) if dex_data else t.buy_volume_h1
            t.liquidity = dex_data["liquidity"] if dex_data else t.liquidity
            t.buy_ratio = dex_data.get("buy_ratio", t.buy_ratio) if dex_data else t.buy_ratio
            t.buys_h1 = dex_data.get("buys_h1", t.buys_h1) if dex_data else t.buys_h1
            t.sells_h1 = dex_data.get("sells_h1", t.sells_h1) if dex_data else t.sells_h1
            t.vol_history = [t.volume_usd] if t.volume_usd else t.vol_history
            t.chain_id = final_chain
            t.is_boosted = mint in boosted_mints
            t.ws_pre_enrichment = False  # fully enriched (or retries exhausted — see caller)
            chain_label = get_chain(final_chain)["label"]
            if dex_data:
                logger.info(f"⚡ WS→Enriched [{chain_label}] ${sym} | MC: ${t.market_cap:,.0f} (stub upgraded)")
            else:
                logger.info(f"⚡ WS→Enriched [{chain_label}] ${sym} | MC: ${t.market_cap:,.0f} (DexScreener never indexed it — using WS estimate)")

        elif mint not in tokens:
            if not dex_data:
                return  # never had a stub and DexScreener never resolved it — nothing to track
            # [v4.39] Entry gate — this is this token's first real data point
            # (no pre-track stub existed for it, e.g. a KOL-resolved mint or
            # any EVM pair now that pre-track is off). Only start tracking it
            # if that first real MC already sits inside this chain's entry
            # window; otherwise, per the same "first observation only, no
            # re-checking later" rule as above, this mint is never revisited.
            _th = get_thresholds(final_chain)
            if not (_th["mc_min"] <= mc <= _th["mc_max"]):
                return
            if not enforce_token_cap(sym):
                return

            tokens[mint] = TokenInfo(
                mint=mint,
                symbol=dex_data["symbol"],
                name=dex_data["name"],
                created_at=real_launch if real_launch else token_data["created_at"],
                launched_at=real_launch,
                market_cap=mc,
                volume_usd=dex_data["volume_usd"],
                volume_h1_total=dex_data.get("volume_h1_total", 0.0),
                # [v4.44] Was left at its 0.0 default until the next poll
                # refresh (see the enrich loop below) populated it -- meaning
                # the buy-only vol_ok gate this now feeds would spuriously
                # fail for a token's entire first poll cycle after creation.
                buy_volume_h1=dex_data.get("buy_volume_h1", 0.0),
                liquidity=dex_data["liquidity"],
                buy_ratio=dex_data.get("buy_ratio", 0.5),
                buys_h1=dex_data.get("buys_h1", 0),
                sells_h1=dex_data.get("sells_h1", 0),
                last_mc=mc,
                is_boosted=mint in boosted_mints,
                vol_history=[dex_data["volume_usd"]] if dex_data["volume_usd"] else [],
                ws_discovered=True,
                ws_initial_buy_sol=token_data.get("ws_initial_buy_sol", 0.0),
                chain_id=final_chain,
            )
            chain_label = get_chain(final_chain)["label"]
            logger.info(f"⚡ WS→Tracked [{chain_label}] ${sym} | MC: ${mc:,.0f}")


async def _retry_ws_enrich(session: aiohttp.ClientSession, mint: str, token_data: dict, chain_id: str, sym: str) -> None:
    """
    [v4.26] Background retry for a WS-discovered token whose first DexScreener
    lookup came back empty. Runs as its own task so a slow-to-index (or dead)
    pair never blocks ws_enrich_worker from processing the rest of the queue.
    Tries again at each of WS_ENRICH_RETRY_DELAYS; on success, finalizes with
    real data. If every retry still comes up empty, finalizes anyway with
    whatever's available (the WS-estimated stub MC, for a pre-tracked token)
    rather than leaving the stub in permanent limbo.
    """
    dex_data = None
    for delay in WS_ENRICH_RETRY_DELAYS:
        await asyncio.sleep(delay)
        try:
            dex_data = await get_dex_data(session, mint, chain_id)
        except Exception as e:
            logger.debug(f"Retry enrich error for ${sym}: {e}")
            dex_data = None
        if dex_data and dex_data.get("market_cap", 0) >= 1_000:
            break
        dex_data = None  # treat "found but still ~$0" the same as a miss — keep retrying

    mc = dex_data["market_cap"] if dex_data else 0.0
    final_sym = dex_data["symbol"].upper() if dex_data else sym

    if dex_data and (final_sym in _WS_ENRICH_SKIP_SYMBOLS or mc > 5_000_000):
        # A late-arriving lookup revealed this was junk (or resolved to a
        # base/quote asset, not a real new listing) — drop the stub instead
        # of finalizing it as a trackable token.
        async with tokens_lock:
            if mint in tokens and tokens[mint].ws_pre_enrichment:
                del tokens[mint]
        return

    await _finalize_ws_token(mint, token_data, chain_id, dex_data, final_sym, mc)


async def ws_enrich_worker(session: aiohttp.ClientSession):
    """
    Drains ws_discovery_queue. For each WS-discovered token:
    1. Waits WS_ENRICH_DELAY seconds (let DexScreener index it first)
    2. Fetches enriched data from DexScreener
    3. Adds to token pool, or — if DexScreener hasn't indexed it yet —
       hands off to a background retry task (_retry_ws_enrich) instead of
       giving up, so this worker keeps draining the queue without delay.
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

            if dex_data:
                mc = dex_data["market_cap"]
                sym = dex_data["symbol"].upper()

                if sym in _WS_ENRICH_SKIP_SYMBOLS or mc > 5_000_000:
                    ws_discovery_queue.task_done()
                    continue
                if mc < 1_000:
                    ws_discovery_queue.task_done()
                    continue

                await _finalize_ws_token(mint, token_data, ws_chain_id, dex_data, sym, mc)
            else:
                # [v4.26] Don't give up after one miss — DexScreener often just
                # hasn't indexed the pair yet. Retry in the background instead
                # of blocking this worker (which needs to keep draining the
                # queue for every other in-flight discovery).
                sym = token_data.get("symbol", "???").upper()
                asyncio.create_task(_retry_ws_enrich(session, mint, token_data, ws_chain_id, sym))

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

                    # [v4.11] Dedicated per-chain discovery for ETH/BSC/Base/Robinhood
                    for cid in ["ethereum", "bsc", "base", "robinhood"]:
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
                            if sym in SKIP_SYMBOLS or mc > 5_000_000:
                                seen_mints.add(mint)
                                continue
                            # [v4.39] Was a quarter of mc_min — deliberately let a
                            # token in below the real floor so later polls (this
                            # loop re-fetches dex_data for already-tracked tokens
                            # elsewhere) could catch it climbing into range. That's
                            # exactly the "background watching" this bot no longer
                            # does: entry is now a hard mc_min..mc_max check against
                            # THIS first observation only — outside that window,
                            # the mint is skipped and never revisited.
                            if not (_th["mc_min"] <= mc <= _th["mc_max"]):
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
                                        volume_h1_total=dex_data.get("volume_h1_total", 0.0),
                                        buy_volume_h1=dex_data.get("buy_volume_h1", 0.0),
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
                                        tokens[mint].volume_h1_total = dex_data.get("volume_h1_total", 0.0)
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
                                        # [v4.27] Keep the eviction-ranking score live for
                                        # polled (post-graduation / non-WS) tokens too.
                                        update_composite_score(tokens[mint])

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
    sol_enabled = get_chain("solana")["enabled"]
    if not sol_enabled:
        logger.info("⏸️ Solana disabled (ENABLE_SOLANA=false) — PumpPortal/direct pump.fun listeners not starting; all capacity goes to BSC/Base/ETH")
    elif PUMPFUN_DIRECT_MODE == "live":
        logger.info(f"🔌 Direct pump.fun indexing (live) via Helius: {PUMPFUN_PROGRAM_ID}")
    else:
        logger.info(f"🔌 PumpPortal WS: {PUMPPORTAL_WS}" + (" (+ direct pump.fun shadow mode)" if PUMPFUN_DIRECT_MODE == "shadow" else ""))
    logger.info(f"📡 Fallback poll: {POLL_INTERVAL}s | Cap: {MAX_TRACKED_TOKENS} | Alert gap: {ALERT_RATE_LIMIT}s")

    load_state()
    alert_db.init_db(_DATA_DIR)  # [v4.31] SQLite alert-history log — additive, never blocks startup on failure
    await init_telegram()

    _alert_queue = asyncio.Queue(maxsize=50)
    ws_discovery_queue = asyncio.Queue(maxsize=500)
    global ws_sub_request_queue
    ws_sub_request_queue = asyncio.Queue(maxsize=100)  # [fix] trade sub requests

    load_kols()

    alert_task = asyncio.create_task(alert_worker())
    # [v4.29] In "live" direct-indexing mode, the direct listener replaces
    # PumpPortal entirely (it gets creates + trades for every mint, no 50-sub
    # cap) — starting both would double-count. In "off"/"shadow" mode PumpPortal
    # stays the real feed and behaves exactly as before.
    # [v4.30] None of this starts at all when Solana is disabled — no PumpPortal
    # connection, no direct pump.fun WS, no squat-guard bookkeeping. That's the
    # whole point of the config-disable: zero SOL-related work competing for
    # event-loop time/network connections with BSC/Base/ETH.
    ws_task = None
    ws_sub_task = None
    direct_task = None
    if sol_enabled:
        if PUMPFUN_DIRECT_MODE == "live":
            logger.info("🛰️ PUMPFUN_DIRECT_MODE=live — direct on-chain indexing replaces PumpPortal WS")
            direct_task = asyncio.create_task(pumpfun_direct_listener())
        else:
            ws_task = asyncio.create_task(pumpfun_ws_listener())
            ws_sub_task = asyncio.create_task(ws_trade_subscription_manager())
            if PUMPFUN_DIRECT_MODE == "shadow":
                logger.info("🛰️ PUMPFUN_DIRECT_MODE=shadow — direct on-chain indexing running read-only alongside PumpPortal")
                direct_task = asyncio.create_task(pumpfun_direct_listener())
    polling_task = asyncio.create_task(polling_loop())

    # [v4.25] One Alchemy WS listener per EVM chain that's enabled with has_ws
    # (i.e. ALCHEMY_API_KEY is set). No-ops harmlessly per-chain otherwise.
    evm_ws_tasks = [
        asyncio.create_task(evm_ws_listener(cid))
        for cid in ("bsc", "base", "ethereum", "robinhood")
        if get_chain(cid)["enabled"] and get_chain(cid)["has_ws"]
    ]

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

    all_tasks = [polling_task, alert_task, kol_task] + evm_ws_tasks
    all_tasks += [t for t in (ws_task, ws_sub_task, direct_task) if t is not None]
    for task in all_tasks:
        task.cancel()
    for task in all_tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass

    _last_snapshot_time = 0.0
    await save_state()
    logger.info("👋 Stopped")


app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def _rate_limit_middleware(request: Request, call_next):
    """[v4.24] Apply webhook_security.RateLimiter to every route. Was imported
    but never wired up — endpoints (including /webhook/telegram) had zero
    rate limiting before this."""
    client_ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_ip):
        return JSONResponse({"ok": False, "error": "rate limit exceeded"}, status_code=429)
    return await call_next(request)

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
        "pumpfun_direct": pumpfun_direct_stats if PUMPFUN_DIRECT_MODE != "off" else None,
        "fourmeme": {**fourmeme_stats, "mode": FOURMEME_MODE} if FOURMEME_MODE != "off" else None,
        "kol": {**kol_stats, "accounts": len(kol_accounts)},
        "evm_ws": {cid: s for cid, s in evm_ws_stats.items() if get_chain(cid)["has_ws"]},
        "squat_guard": {
            "locked_symbols": len(ws_symbol_lockout),
            "guarded_winners": len(alerted_symbol_registry),
        },
        "alert_queue": _alert_queue.qsize() if _alert_queue else 0,
        "mode": "websocket+polling",
        "runner_detection": _RUNNER_DETECTOR_AVAILABLE,
    }

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return build_dashboard_html()


@app.get("/db/alerts")
async def db_alerts(limit: int = 50, chain: str = "", symbol: str = ""):
    """
    [v4.31] Durable alert history from the SQLite log — unlike /analysis this
    survives restarts and isn't capped at 500. Query params:
      - limit: how many rows to return (default 50, max 500)
      - chain: filter by chain_id (e.g. 'bsc', 'ethereum')
      - symbol: filter by exact ticker (case-insensitive)
    """
    rows = await alert_db.query_alerts_async(limit=limit, chain_id=chain or None, symbol=symbol or None)
    return {"count": len(rows), "alerts": rows}


@app.get("/db/stats")
async def db_stats():
    """[v4.31] Aggregate counts from the alert-history DB: total alerts, broken
    down by chain and by alert path (GEM/FAST/EARLY/VOL/VACCEL)."""
    return await alert_db.get_stats_async()


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
            elif key.startswith("vol "): key = "vol"
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