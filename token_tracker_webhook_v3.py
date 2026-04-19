#!/usr/bin/env python3
"""
Solana Token Tracker v3.1 - Smart Volume Detection
====================================================
Enhanced tracker with smart volume detection algorithm.
NO milestone alerts - only smart detection!

FEATURES:
- ✅ Smart volume detection (10+ buys from 5+ wallets in 3 mins)
- ✅ Buy tracking from Helius webhooks  
- ✅ Professional alert format
- ✅ Real DexScreener volume + MC tracking
- ✅ Telegram notifications
- ✅ FastAPI for health checks

DEPLOYMENT:
- Platform: Render.com
- Runtime: Python 3
- Start: python token_tracker_webhook_v3.py
"""
import sys
import os

# UTF-8 fix
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import asyncio
import gzip
import json
import time
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple, List
from contextlib import asynccontextmanager

import aiohttp
import aiosqlite
from dotenv import load_dotenv
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
import uvicorn

# Telegram
from telegram import Bot, Update, BotCommand
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

# Solana
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey as PublicKey

# Load environment
load_dotenv()

# Enhanced modules
from config import get_config
from webhook_security import WebhookValidator, RateLimiter, verify_webhook_request
from circuit_breaker import get_circuit_breaker, CircuitBreakerConfig, CircuitBreakerOpenError, get_all_circuit_states
from holder_analysis import analyze_token_holders, HolderMetrics
from defi_detector import detect_defi_deployment, format_defi_alert
import metrics as metrics_module
import database

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
# Load centralized configuration
config = get_config()

# Legacy compatibility
HELIUS_API_KEY = config.api.helius_api_key
TELEGRAM_BOT_TOKEN = config.telegram.bot_token
TELEGRAM_CHAT_ID = config.telegram.chat_id
PORT = config.port
RPC_URL = config.api.rpc_https
FORCE_RPC_FALLBACK = os.getenv("FORCE_RPC", "0") == "1"
SKIP_STARTUP_MESSAGE = os.getenv("SKIP_STARTUP_MESSAGE", "0") == "1"
TELEGRAM_MIN_INTERVAL = float(os.getenv("TELEGRAM_MIN_INTERVAL", "1.2"))  # seconds between sends
DISABLE_BASELINE_ALERTS = os.getenv("DISABLE_BASELINE_ALERTS", "1") == "1"  # default: disable helius baseline alerts
DISABLE_RPC_BASIC_ALERTS = os.getenv("DISABLE_RPC_BASIC_ALERTS", "1") == "1"  # default: suppress RPC fallback spam
TELEGRAM_QUEUE_MAX_SIZE = int(os.getenv("TELEGRAM_QUEUE_MAX_SIZE", "500"))

# Multi-worker queue scaling (handles 25+ req/sec)
QUEUE_WORKER_COUNT = int(os.getenv("QUEUE_WORKER_COUNT", "3"))  # 3 workers = 2x throughput

# Smart Volume Threshold Tuning
SMART_VOLUME_THRESHOLD_SOL = float(os.getenv("SMART_VOLUME_THRESHOLD_SOL", "0.5"))  # Lower from 1.0 for more alerts
SMART_MIN_MC_FILTER_USD = float(os.getenv("SMART_MIN_MC_FILTER_USD", "10000"))  # MC filter to avoid rugs

# Heavy Volume Detection (Smart Algorithm)
HEAVY_VOLUME_ENABLED = os.getenv("HEAVY_VOLUME_ENABLED", "1") == "1"
HEAVY_VOLUME_SCORE_THRESHOLD = float(os.getenv("HEAVY_VOLUME_SCORE_THRESHOLD", "7.0"))  # 0-10 scale
HEAVY_VOLUME_MIN_VOLUME_SOL = float(os.getenv("HEAVY_VOLUME_MIN_VOLUME_SOL", "5.0"))  # Min vol to consider
HEAVY_VOLUME_MAX_AGE_HOURS = float(os.getenv("HEAVY_VOLUME_MAX_AGE_HOURS", "2.0"))  # Only recent tokens
HEAVY_VOLUME_VELOCITY_PEAK = float(os.getenv("HEAVY_VOLUME_VELOCITY_PEAK", "50.0"))  # SOL/hr for normalization
HEAVY_VOLUME_REQUIRE_SMART = os.getenv("HEAVY_VOLUME_REQUIRE_SMART", "0") == "1"  # Require smart money

# Settings
MAX_CONCURRENT_REQUESTS = config.max_concurrent_requests
MONITOR_HOURS_OLD = config.monitor_hours_old

# Smart Volume Thresholds
MIN_BUYS_3MIN = config.detection.min_buys_3min
MIN_UNIQUE_WALLETS = config.detection.min_unique_wallets
MIN_SOL_VOLUME = config.detection.min_sol_volume
MIN_MARKET_CAP_USD = config.detection.min_market_cap_usd
SMART_SCORE_THRESHOLD = config.detection.smart_score_threshold
SMART_COOLDOWN_SECONDS = config.detection.smart_cooldown_seconds
SMART_GLOBAL_CAP_PER_MIN = config.detection.smart_global_cap_per_min
SMART_WINDOW_SECONDS = config.detection.smart_window_seconds
SMART_CLUSTER_WINDOW_SECONDS = config.detection.smart_cluster_window_seconds
SMART_MC_DOWNWEIGHT_FACTOR = config.detection.smart_mc_downweight_factor

# Resurgence Mode - detect established tokens regaining volume
RESURGENCE_MODE = config.detection.resurgence_mode
MIN_TOKEN_AGE_SECONDS = config.detection.min_token_age_seconds
REQUIRE_EXISTING_MC = config.detection.require_existing_mc
MIN_EXISTING_MC_USD = config.detection.min_existing_mc_usd

# ============================================================================
# Data Structures
# ============================================================================
@dataclass
class BuyEvent:
    """Individual buy transaction."""
    wallet: str
    amount_sol: float
    timestamp: float

@dataclass
class TokenInfo:
    mint: PublicKey
    launchpad: str
    created_at: float
    chain: str = "solana"  # solana, ethereum, bsc
    volume_sol: float = 0.0
    market_cap: float = 0.0
    name: str = "Unknown"
    symbol: str = "UNK"
    # Smart volume tracking
    recent_buys: deque = field(default_factory=lambda: deque(maxlen=100))
    first_alert_sent: bool = False
    basic_alert_sent: bool = False  # baseline alert already sent
    # Holder analysis
    holder_metrics: Optional[HolderMetrics] = None
    holder_analysis_done: bool = False
    last_alert_ts: float = 0.0
    # Heavy Volume Detection (velocity tracking)
    velocity_history: List[tuple] = field(default_factory=list)  # [(timestamp, volume_sol), ...]
    heavy_volume_notified: bool = False
    heavy_volume_score: float = 0.0  # Last calculated score

# Chain configuration for multi-chain support
CHAIN_CONFIG = {
    "solana": {
        "name": "Solana",
        "native_symbol": "SOL",
        "native_price_usd": 180.0,
        "dexscreener_chain": "solana",
        "explorer": "https://solscan.io/token/{address}",
        "dex_link": "https://dexscreener.com/solana/{address}",
    },
    "ethereum": {
        "name": "Ethereum",
        "native_symbol": "ETH",
        "native_price_usd": 3500.0,
        "dexscreener_chain": "ethereum",
        "explorer": "https://etherscan.io/token/{address}",
        "dex_link": "https://dexscreener.com/ethereum/{address}",
    },
    "bsc": {
        "name": "BNB Chain",
        "native_symbol": "BNB",
        "native_price_usd": 600.0,
        "dexscreener_chain": "bsc",
        "explorer": "https://bscscan.com/token/{address}",
        "dex_link": "https://dexscreener.com/bsc/{address}",
    },
}

# ============================================================================
# Global State
# ============================================================================
tokens: Dict[str, TokenInfo] = {}
telegram_bot: Optional[Bot] = None
telegram_app = None
rate_limiter: Optional[asyncio.Semaphore] = None
tokens_lock = asyncio.Lock()
start_time = time.time()
processed_signatures: Set[str] = set()
telegram_queue: Optional[asyncio.Queue] = None
recent_webhook_sigs: Set[str] = set()
global_alert_times: deque = deque(maxlen=200)  # sliding window of recent smart alerts timestamps

# Async processing queue for webhook payloads (handles burst traffic)
process_queue: Optional[asyncio.Queue] = None
PROCESS_QUEUE_MAX_SIZE = 1000  # Buffer up to 1000 pending payloads

# Security & monitoring
webhook_validator: Optional[WebhookValidator] = None
request_rate_limiter: Optional[RateLimiter] = None

# Pump.fun program
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Launchpad programs to filter for (QuickNode filter)
LAUNCHPAD_PROGRAMS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "Pump.fun",
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG": "Moonshot",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "Meteora Pools",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
}

# Enable/disable QuickNode launchpad filtering (set False to receive ALL transactions)
QUICKNODE_FILTER_ENABLED = os.getenv("QUICKNODE_FILTER_ENABLED", "1") == "1"

# System tokens to ignore (not actual launches)
IGNORED_MINTS = {
    "So11111111111111111111111111111111111111112",  # Wrapped SOL (WSOL)
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # mSOL
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",  # stSOL
}

# ============================================================================
# FastAPI Lifespan
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown."""
    global telegram_bot, telegram_app, rate_limiter, webhook_validator, request_rate_limiter
    
    logger.info("🚀 Token Tracker v3.1 - Enhanced with Smart Detection")
    
    # Initialize database
    await database.init_db()
    
    # Initialize security
    webhook_validator = WebhookValidator(config.webhook.secret_key)
    if config.webhook.enable_signature_verification:
        logger.info("✅ Webhook signature verification enabled")
    
    request_rate_limiter = RateLimiter(
        max_requests=config.webhook.rate_limit_per_ip,
        window_seconds=60
    )
    logger.info(f"✅ Rate limiting: {config.webhook.rate_limit_per_ip} req/min per IP")
    
    rate_limiter = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    if TELEGRAM_BOT_TOKEN:
        try:
            telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
            telegram_app.add_handler(CommandHandler("start", start_command))
            telegram_app.add_handler(CommandHandler("status", status_command))

            # Initialize internal components required before processing updates
            await telegram_app.initialize()

            await telegram_app.bot.set_my_commands([
                BotCommand("start", "Welcome"),
                BotCommand("status", "Stats"),
            ])

            webhook_url = os.getenv("RENDER_EXTERNAL_URL")
            if webhook_url:
                await telegram_app.bot.set_webhook(f"{webhook_url}/webhook/telegram")
                logger.info("✅ Telegram webhook set")

            # Start application (job queues, persistence, etc.)
            await telegram_app.start()

            telegram_bot = telegram_app.bot
            logger.info("✅ Telegram bot initialized")
            # Initialize queue + sender worker for rate limiting
            global telegram_queue
            telegram_queue = asyncio.Queue()
            asyncio.create_task(telegram_sender_worker())

            if TELEGRAM_CHAT_ID and not SKIP_STARTUP_MESSAGE:
                await send_telegram_message(
                    "🚀 <b>Smart Volume Tracker v3.0</b>\n\n"
                    "Criteria: 10+ buys, 5+ wallets, 5 SOL in 3 mins\n"
                    "Waiting for smart tokens..."
                )
        except Exception as e:
            logger.error(f"Telegram error: {e}")
    
    # Initialize webhook processing queue for burst handling
    global process_queue
    process_queue = asyncio.Queue(maxsize=PROCESS_QUEUE_MAX_SIZE)
    
    # Multi-worker scaling: spawn multiple queue workers for 2x+ throughput
    for i in range(QUEUE_WORKER_COUNT):
        asyncio.create_task(process_queue_worker(worker_id=i))
    logger.info(f"✅ Webhook processing queue initialized (max {PROCESS_QUEUE_MAX_SIZE}, {QUEUE_WORKER_COUNT} workers)")
    
    # Start background tasks
    asyncio.create_task(sync_volumes_and_detect())
    asyncio.create_task(cleanup_old_tokens())
    asyncio.create_task(cleanup_old_signatures_task())  # New: periodic signature cleanup
    asyncio.create_task(check_heavy_volume())  # Smart heavy volume detection
    if FORCE_RPC_FALLBACK or not HELIUS_API_KEY:
        logger.info("🧰 Starting RPC fallback poller (no Helius credits)")
        asyncio.create_task(rpc_poll_pumpfun_new_tokens())
    
    yield
    
    # Shutdown
    if telegram_app:
        try:
            await telegram_app.stop()
            await telegram_app.shutdown()
        except Exception as e:
            logger.debug(f"Telegram shutdown error: {e}")
    logger.info("👋 Tracker stopped")

app = FastAPI(title="Smart Volume Tracker", version="3.0", lifespan=lifespan)


# ============================================================================
# Async Queue Worker for Webhook Processing (handles bursts)
# ============================================================================
async def process_queue_worker(worker_id: int = 0):
    """Background worker that processes queued webhook payloads.
    
    Multi-worker scaling: multiple instances run concurrently for 2x+ throughput.
    This decouples ingestion (fast) from processing (slower), allowing us
    to handle 25+ webhooks/sec without dropping requests.
    """
    global process_queue
    logger.info(f"🔄 Queue worker #{worker_id} started")
    processed_count = 0
    
    while True:
        try:
            # Wait for next item (blocks until available)
            signature, payload = await process_queue.get()
            
            try:
                await process_queued_payload(signature, payload)
                processed_count += 1
                
                # Log progress periodically (stagger by worker_id to reduce log spam)
                if processed_count % 100 == 0:
                    queue_size = process_queue.qsize()
                    logger.info(f"📊 Worker #{worker_id}: {processed_count} processed, {queue_size} pending")
                
                # Adaptive backpressure: slow down during extreme bursts
                queue_size = process_queue.qsize()
                if queue_size > 500:
                    # Heavy backlog: brief pause (reduced with multi-workers)
                    backoff = min(0.05 * (queue_size / 100), 0.5)  # Halved with multi-workers
                    if worker_id == 0:  # Only log from worker 0 to avoid spam
                        logger.warning(f"⚠️ Queue backpressure: {queue_size} pending, backoff {backoff:.2f}s")
                    await asyncio.sleep(backoff)
                elif queue_size > 300:
                    # Moderate backlog: tiny pause
                    await asyncio.sleep(0.02)
                    
            except Exception as e:
                logger.error(f"Worker #{worker_id} error processing {signature[:16] if signature else 'no-sig'}...: {e}")
            finally:
                process_queue.task_done()
                
        except asyncio.CancelledError:
            logger.info(f"🛑 Queue worker #{worker_id} cancelled")
            break
        except Exception as e:
            logger.error(f"Worker #{worker_id} loop error: {e}")
            await asyncio.sleep(0.1)  # Brief pause on error


async def process_queued_payload(signature: Optional[str], payload: dict):
    """Process a single queued webhook payload (extracted from helius_webhook for async processing)."""
    
    # DeFi Detection (if enabled)
    if config.detection.enable_defi_alerts:
        try:
            async with aiohttp.ClientSession() as session:
                cb = get_circuit_breaker("defi_detection", CircuitBreakerConfig(failure_threshold=5))
                defi_protocol = await cb.call(
                    detect_defi_deployment,
                    session,
                    payload,
                    5.0  # min liquidity SOL
                )
                
                if defi_protocol and defi_protocol.is_high_value:
                    defi_message = format_defi_alert(defi_protocol)
                    await send_telegram_message(defi_message, defi_protocol.program_id, "defi_launch")
                    metrics_module.defi_deployments_detected_total.inc()
                    await database.add_seen_program(defi_protocol.program_id)
                    
        except CircuitBreakerOpenError:
            logger.debug("DeFi detection circuit open")
        except Exception as e:
            logger.debug(f"DeFi detection error: {e}")

    # Extract mint
    mint_str = None
    
    if "mint" in payload:
        mint_str = payload["mint"]
    elif "tokenTransfers" in payload and payload["tokenTransfers"]:
        for transfer in payload["tokenTransfers"]:
            if transfer.get("mint"):
                mint_str = transfer["mint"]
                break
    
    if not mint_str:
        logger.debug(f"No mint found in queued payload")
        if signature:
            await database.add_seen_signature(signature, None)
        return
    
    # Filter out known system tokens (WSOL, USDC, etc.)
    if mint_str in IGNORED_MINTS:
        webhook_stats["helius_system_tokens_skipped"] = webhook_stats.get("helius_system_tokens_skipped", 0) + 1
        if signature:
            await database.add_seen_signature(signature, mint_str)
        return
    
    # Track successful mint extraction
    webhook_stats["helius_tokens_found"] += 1
    logger.info(f"🎯 [Queue] Mint extracted: {mint_str[:16]}...")
    
    # Persist signature with associated mint
    if signature:
        await database.add_seen_signature(signature, mint_str)
    
    # Sanitize mint
    mint_str = webhook_validator.sanitize_mint_address(mint_str) if webhook_validator else mint_str
    if not mint_str:
        return
    
    # Validate mint
    try:
        mint_pubkey = PublicKey.from_string(str(mint_str))
    except:
        return
    
    # Ensure token exists
    async with tokens_lock:
        if mint_str not in tokens:
            tokens[mint_str] = TokenInfo(
                mint=mint_pubkey,
                launchpad="Pump.fun",
                created_at=time.time(),
            )
            # Schedule baseline new token alert
            async def baseline_alert(mint_token: str):
                try:
                    async with tokens_lock:
                        t = tokens.get(mint_token)
                        if not t or t.basic_alert_sent:
                            return
                        t.basic_alert_sent = True
                    await notify_new_token_basic(mint_token)
                except Exception as e:
                    logger.debug(f"Baseline alert error for {mint_token}: {e}")
            
            if not DISABLE_BASELINE_ALERTS:
                asyncio.create_task(baseline_alert(mint_str))

    # Record buy-like transfers
    transfers = payload.get("tokenTransfers", []) or []
    if transfers:
        async with tokens_lock:
            for transfer in transfers:
                to_wallet = transfer.get("toUserAccount") or transfer.get("toUserAccountOwner") or ""
                amount = float(transfer.get("tokenAmount", 0) or 0)
                if to_wallet and amount > 0:
                    sol_amount = amount * 0.00001
                    tokens[mint_str].recent_buys.append(BuyEvent(
                        wallet=to_wallet,
                        amount_sol=sol_amount,
                        timestamp=time.time()
                    ))

    # Process token (DexScreener lookup, smart volume check)
    await process_token_data(mint_str)


async def process_token_data(mint: str):
    """Fetch DexScreener data and check smart volume criteria."""
    try:
        cb = get_circuit_breaker("dexscreener", CircuitBreakerConfig(
            failure_threshold=config.api.api_failure_threshold,
            recovery_timeout=config.api.api_recovery_timeout
        ))
        
        async with aiohttp.ClientSession() as session:
            metrics_module.api_calls_total.inc(service="dexscreener")
            vol, mc = await cb.call(get_dexscreener_data, session, mint)
            
        async with tokens_lock:
            t = tokens.get(mint)
            if not t:
                return
            t.volume_sol = vol if vol is not None else 0.0
            t.market_cap = mc if mc is not None else 0.0
            passed, metrics = check_smart_volume(t)
            
            # Debug logging for smart volume check
            if metrics.get("buys", 0) > 0:
                logger.info(f"📊 Smart check {mint[:12]}: buys={metrics.get('buys')} wallets={metrics.get('wallets')} sol={metrics.get('sol')} score={metrics.get('weighted_score')} passed={passed}")
                if not passed and metrics.get("rejected_reason"):
                    logger.info(f"   ❌ Rejected: {metrics.get('rejected_reason')}")
            
            if passed:
                t.first_alert_sent = True
                t.last_alert_ts = time.time()
                t.basic_alert_sent = True
                metrics_module.smart_volume_detections_total.inc()
                logger.info(f"✅ Smart volume PASSED for {mint[:12]} - sending alert!")
        
        # Send alert outside lock
        async with tokens_lock:
            t2 = tokens.get(mint)
        if t2 and t2.first_alert_sent:
            global_alert_times.append(time.time())
            await notify_smart_token(t2, metrics)
            
    except CircuitBreakerOpenError:
        logger.warning(f"DexScreener circuit open for {mint[:8]}")
        metrics_module.api_errors_total.inc(service="dexscreener", reason="circuit_open")
    except Exception as e:
        logger.error(f"process_token_data error for {mint}: {e}")
        metrics_module.api_errors_total.inc(service="processing")

# ============================================================================
# Helper Functions
# ============================================================================
async def get_dexscreener_data(session: aiohttp.ClientSession, mint: str, chain: str = "solana") -> Tuple[float, float]:
    """Get volume and market cap from DexScreener for any supported chain."""
    try:
        # DexScreener API works with token address directly
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return 0.0, 0.0
            
            data = await resp.json()
            pairs = data.get("pairs", [])
            if not pairs:
                return 0.0, 0.0
            
            # Filter pairs by chain if specified
            chain_id = CHAIN_CONFIG.get(chain, {}).get("dexscreener_chain", chain)
            chain_pairs = [p for p in pairs if p.get("chainId", "").lower() == chain_id.lower()]
            if not chain_pairs:
                chain_pairs = pairs  # fallback to all pairs
            
            sorted_pairs = sorted(chain_pairs, key=lambda x: float(x.get("volume", {}).get("h24", 0)), reverse=True)
            top_pair = sorted_pairs[0]
            
            volume_usd = float(top_pair.get("volume", {}).get("h24", 0))
            native_price = CHAIN_CONFIG.get(chain, {}).get("native_price_usd", 180.0)
            volume_native = volume_usd / native_price
            market_cap_usd = float(top_pair.get("fdv", 0)) if top_pair.get("fdv") else 0.0
            
            return volume_native, market_cap_usd
    except Exception as e:
        logger.debug(f"DexScreener error for {chain}: {e}")
        return 0.0, 0.0

async def get_token_metadata(mint: str) -> Tuple[str, str]:
    """Fetch token name and symbol."""
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        payload = {
            "jsonrpc": "2.0",
            "id": "metadata",
            "method": "getAsset",
            "params": {"id": mint}
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "result" in data and "content" in data["result"]:
                        metadata = data["result"]["content"].get("metadata", {})
                        return metadata.get("name", "Unknown"), metadata.get("symbol", "UNK")
    except:
        pass
    return "Unknown", "UNK"

def check_smart_volume(token: TokenInfo) -> Tuple[bool, dict]:
    """
    Intelligent Detection Algorithm (v4) - Resurgence Mode
    Detects ESTABLISHED tokens regaining smart volume, not new launches.
    
    Features:
    - Weighted time decay (recent buys matter more)
    - Buy clustering (tight bursts within 10s windows)
    - Momentum (buys per minute acceleration)
    - Repeat buyer conviction (multi-buy wallets)
    - Composite scoring with MC sanity down-weight
    - RESURGENCE MODE: filters out brand new tokens
    
    Preserves: global alert cap & 30m per-token cooldown anti-spam layers
    """
    now = time.time()
    
    # RESURGENCE MODE: Skip brand new tokens
    if RESURGENCE_MODE:
        token_age = now - token.created_at
        # Must be old enough
        if token_age < MIN_TOKEN_AGE_SECONDS:
            return False, {
                "buys": 0, "wallets": 0, "sol": 0.0, "weighted_score": 0.0,
                "rejected_reason": f"too_new (age: {int(token_age)}s < {MIN_TOKEN_AGE_SECONDS}s)"
            }
        # Must have existing market cap from DexScreener
        if REQUIRE_EXISTING_MC and token.market_cap < MIN_EXISTING_MC_USD:
            return False, {
                "buys": 0, "wallets": 0, "sol": 0.0, "weighted_score": 0.0,
                "rejected_reason": f"no_mc (mc: ${token.market_cap:.0f} < ${MIN_EXISTING_MC_USD:.0f})"
            }
    
    cutoff = now - SMART_WINDOW_SECONDS  # evaluation window
    recent = [b for b in token.recent_buys if b.timestamp > cutoff]
    if not recent:
        return False, {"buys": 0, "wallets": 0, "sol": 0.0, "weighted_score": 0.0}

    # 1) Raw metrics
    buy_count = len(recent)
    unique_wallets = len({b.wallet for b in recent})
    total_sol = sum(b.amount_sol for b in recent)

    # 2) Time decay weighting (linear)
    time_scores = []
    for b in recent:
        age = now - b.timestamp
        weight = max(0.1, 1 - (age / SMART_WINDOW_SECONDS))  # within window: newer -> closer to 1
        time_scores.append(weight)
    time_density_score = sum(time_scores)

    # 3) Clustering (adjacent buys within N seconds)
    if buy_count > 1:
        timestamps = sorted(b.timestamp for b in recent)
        cluster_hits = sum(1 for i in range(len(timestamps) - 1) if timestamps[i+1] - timestamps[i] <= SMART_CLUSTER_WINDOW_SECONDS)
    else:
        cluster_hits = 0
    cluster_score = cluster_hits * 0.75

    # 4) Momentum (buys per minute)
    oldest_ts = min(b.timestamp for b in recent)
    window_minutes = max(0.1, (now - oldest_ts) / 60)
    buys_per_min = buy_count / window_minutes
    momentum_score = buys_per_min * 0.5

    # 5) Repeat buyer conviction
    wallet_counts = defaultdict(int)
    for b in recent:
        wallet_counts[b.wallet] += 1
    repeat_buyers = sum(1 for c in wallet_counts.values() if c >= 2)
    quality_score = repeat_buyers * 1.2

    # 6) Composite score (base weights tuned empirically)
    score = (
        buy_count * 0.8 +
        unique_wallets * 1.4 +
        (total_sol * 0.6) +
        time_density_score * 1.0 +
        cluster_score +
        momentum_score +
        quality_score
    )
    # Down-weight low MC noisy pumps
    if token.market_cap < MIN_MARKET_CAP_USD:
        score *= SMART_MC_DOWNWEIGHT_FACTOR

    # Anti-spam: global cap & per-token cooldown (30 min)
    while global_alert_times and now - global_alert_times[0] > 60:
        global_alert_times.popleft()
    global_cap_ok = len(global_alert_times) < SMART_GLOBAL_CAP_PER_MIN
    cooldown_ok = (now - token.last_alert_ts) >= SMART_COOLDOWN_SECONDS

    passed = score >= SMART_SCORE_THRESHOLD and global_cap_ok and cooldown_ok

    metrics = {
        "buys": buy_count,
        "wallets": unique_wallets,
        "sol": round(total_sol, 2),
        "weighted_score": round(score, 2),
        "clusters": cluster_hits,
        "momentum_bpm": round(buys_per_min, 2),
        "repeat_wallets": repeat_buyers,
        "global_cap_ok": global_cap_ok,
        "cooldown_ok": cooldown_ok,
        "token_age_seconds": int(now - token.created_at),
        "resurgence_mode": RESURGENCE_MODE,
    }
    return passed, metrics

def explain_smart_volume(token: TokenInfo) -> dict:
    """Detailed breakdown including composite scoring components."""
    passed, metrics = check_smart_volume(token)
    chain_cfg = CHAIN_CONFIG.get(token.chain, CHAIN_CONFIG["solana"])
    return {
        "address": str(token.mint),
        "chain": token.chain,
        "chain_name": chain_cfg["name"],
        "native_symbol": chain_cfg["native_symbol"],
        "name": token.name,
        "symbol": token.symbol,
        "age_seconds": int(time.time() - token.created_at),
        "market_cap": token.market_cap,
        "metrics": metrics,
        "passed": passed,
        "alert_sent": token.first_alert_sent,
        "thresholds": {
            "score_min": SMART_SCORE_THRESHOLD,
            "cooldown_seconds": SMART_COOLDOWN_SECONDS,
            "global_cap_per_min": SMART_GLOBAL_CAP_PER_MIN,
            "mc_downweight_below": MIN_MARKET_CAP_USD,
            "mc_downweight_factor": SMART_MC_DOWNWEIGHT_FACTOR,
            "window_seconds": SMART_WINDOW_SECONDS,
            "cluster_window_seconds": SMART_CLUSTER_WINDOW_SECONDS,
        },
        "resurgence_mode": {
            "enabled": RESURGENCE_MODE,
            "min_age_seconds": MIN_TOKEN_AGE_SECONDS,
            "require_mc": REQUIRE_EXISTING_MC,
            "min_mc_usd": MIN_EXISTING_MC_USD,
        }
    }

# ============================================================================
# Telegram Functions
# ============================================================================
async def send_telegram_message(message: str, mint: str = "", alert_type: str = "general"):
    """Send Telegram message with metrics tracking."""
    if not telegram_bot or not TELEGRAM_CHAT_ID:
        return
    # Enqueue message for rate-limited sending
    if telegram_queue:
        # Backpressure: drop oldest if queue too large
        try:
            if telegram_queue.qsize() >= TELEGRAM_QUEUE_MAX_SIZE:
                # drain one oldest item to keep memory bounded
                _ = telegram_queue.get_nowait()
                telegram_queue.task_done()
                logger.warning("telegram_queue overflow: dropping oldest message")
            await telegram_queue.put((message, mint, alert_type))
        except Exception as e:
            logger.error(f"telegram_queue enqueue error: {e}")
    else:
        await _telegram_send_immediate(message, mint, alert_type)

async def _telegram_send_immediate(message: str, mint: str, alert_type: str):
    try:
        await telegram_bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
        metrics_module.alerts_sent_total.inc(type=alert_type)
        if mint:
            await database.log_alert(mint, alert_type, message[:200])
    except Exception as e:
        logger.error(f"Telegram send error: {e}")
        metrics_module.api_errors_total.inc(service="telegram")

async def telegram_sender_worker():
    """Background worker applying rate limit and flood-wait backoff."""
    last_sent = 0.0
    while True:
        try:
            payload = await telegram_queue.get()
            message, mint, alert_type = payload
            # Rate limit spacing
            elapsed = time.time() - last_sent
            if elapsed < TELEGRAM_MIN_INTERVAL:
                await asyncio.sleep(TELEGRAM_MIN_INTERVAL - elapsed)
            try:
                await _telegram_send_immediate(message, mint, alert_type)
                last_sent = time.time()
            except Exception as e:
                # Parse flood control retry hints
                retry_delay = None
                text = str(e)
                import re
                m = re.search(r"Retry in (\d+) seconds", text)
                if m:
                    retry_delay = int(m.group(1)) + 1
                if hasattr(e, 'retry_after'):
                    retry_delay = int(getattr(e, 'retry_after')) + 1
                if retry_delay:
                    logger.warning(f"Telegram flood wait detected; retry in {retry_delay}s")
                    await asyncio.sleep(retry_delay)
                    # Requeue message at tail
                    await telegram_queue.put((message, mint, alert_type))
                else:
                    logger.error(f"Telegram send failed (no retry hint): {e}")
            finally:
                telegram_queue.task_done()
        except Exception as e:
            logger.error(f"telegram_sender_worker loop error: {e}")
            await asyncio.sleep(2)

async def notify_smart_token(token: TokenInfo, metrics: dict):
    """Send smart volume alert in enhanced professional format with multi-chain support."""
    # Fetch metadata if missing (only for Solana via Helius)
    if token.name == "Unknown" and token.chain == "solana":
        token.name, token.symbol = await get_token_metadata(str(token.mint))
    
    # Perform holder analysis if enabled and not done yet (Solana only for now)
    if config.detection.enable_holder_analysis and not token.holder_analysis_done and token.chain == "solana":
        try:
            async with aiohttp.ClientSession() as session:
                cb = get_circuit_breaker("holder_analysis", CircuitBreakerConfig(failure_threshold=3))
                token.holder_metrics = await cb.call(
                    analyze_token_holders,
                    session,
                    str(token.mint),
                    HELIUS_API_KEY
                )
                token.holder_analysis_done = True
        except CircuitBreakerOpenError:
            logger.warning(f"Holder analysis circuit open for {str(token.mint)[:8]}")
        except Exception as e:
            logger.debug(f"Holder analysis error: {e}")
    
    # Get chain configuration
    chain_cfg = CHAIN_CONFIG.get(token.chain, CHAIN_CONFIG["solana"])
    native_symbol = chain_cfg["native_symbol"]
    chain_name = chain_cfg["name"]
    
    # Calculate age
    age_seconds = int(time.time() - token.created_at)
    age_minutes = age_seconds // 60
    age_secs = age_seconds % 60
    age_str = f"{age_minutes}m:{age_secs}s"
    
    # Chain emoji
    chain_emoji = {"solana": "☀️", "ethereum": "🔷", "bsc": "🟡"}.get(token.chain, "🔗")
    
    # Build message in enhanced format
    message = "🚨🚨🚨🚨🚨🚨🚨\n\n"
    message += f"{chain_emoji} <b>{chain_name}</b>\n"
    message += f"Name: {token.name} ({token.symbol})\n"
    message += f"MC: ${token.market_cap:,.0f}\n"
    message += f"Address: <code>{str(token.mint)}</code>\n"
    message += f"🔎 by AlphaDegen\n\n"
    message += f"Dex: {token.launchpad} | Age: {age_str}\n"
    message += f"Last {int(SMART_WINDOW_SECONDS/60)} mins: {metrics['sol']:.0f} {native_symbol} in {metrics['buys']} buys\n"
    message += f"Unique wallets: {metrics['wallets']}\n"
    if 'weighted_score' in metrics:
        message += f"Score: {metrics['weighted_score']} | Clusters: {metrics.get('clusters', 0)} | Momentum: {metrics.get('momentum_bpm', 0)} bpm\n\n"
    else:
        message += "\n"
    
    # Add holder information
    if token.holder_metrics and token.holder_metrics.total_holders > 0:
        message += f"Top 10 Holder: {token.holder_metrics.top10_percentage:.1f}%\n"
        if token.holder_metrics.is_risky:
            message += "⚠️ HIGH RUG RISK - Concentrated holdings\n"
    else:
        message += "Top 10 Holder: N/A\n"
    
    # Chain-specific links
    address = str(token.mint)
    dex_link = chain_cfg["dex_link"].format(address=address)
    explorer_link = chain_cfg["explorer"].format(address=address)
    
    message += f"\n🔗 <a href='{dex_link}'>DexScreener</a> | "
    message += f"<a href='{explorer_link}'>Explorer</a>"
    
    # Add Pump.fun link only for Solana
    if token.chain == "solana":
        message += f" | <a href='https://pump.fun/{address}'>Pump.fun</a>"
    
    await send_telegram_message(message, str(token.mint), "smart_volume")

async def notify_new_token_basic(mint: str):
    """Basic new token notification (RPC fallback)."""
    msg = (
        "🚀 <b>NEW TOKEN (RPC Fallback)</b>\n\n"
        f"Mint: <code>{mint}</code>\n"
        "Source: Pump.fun\n\n"
        f"<a href='https://dexscreener.com/solana/{mint}'>DexScreener</a> | "
        f"<a href='https://pump.fun/{mint}'>Pump.fun</a> | "
        f"<a href='https://solscan.io/token/{mint}'>Solscan</a>"
    )
    await send_telegram_message(msg)


async def send_burst_alert(rate: float):
    """Send Telegram alert when webhook burst rate exceeds threshold."""
    msg = (
        f"⚠️ <b>BURST ALERT</b>\n\n"
        f"📊 <b>Rate:</b> {rate:.1f} req/sec\n"
        f"⏰ <b>Time:</b> {time.strftime('%H:%M:%S UTC', time.gmtime())}\n\n"
        f"🔥 High webhook volume detected!\n"
        f"Possible meme wave or Solana congestion.\n\n"
        f"📈 Check: <a href='https://solscan.io/analytics'>Solana TPS</a>"
    )
    await send_telegram_message(msg, alert_type="burst_alert")


async def reset_burst_alert_flag():
    """Reset burst alert flag after 60 seconds to allow new alerts."""
    await asyncio.sleep(60)
    webhook_stats["_burst_alert_sent_recently"] = False

async def sync_volumes_and_detect():
    """Periodically refresh DexScreener data and trigger smart alerts.
    Runs in the background independent of incoming webhooks.
    """
    global rate_limiter
    session_timeout = aiohttp.ClientTimeout(total=8)
    # Reasonable cadence; fast enough to catch action without hammering APIs
    SLEEP_SECONDS = 30
    while True:
        try:
            # Snapshot mints to avoid holding the lock during network I/O
            async with tokens_lock:
                mints_with_chain = [(m, t.chain) for m, t in tokens.items()]

            if not mints_with_chain:
                await asyncio.sleep(10)
                continue

            async with aiohttp.ClientSession(timeout=session_timeout) as session:
                async def process_mint(mint: str, chain: str):
                    try:
                        # Throttle external calls
                        sem = rate_limiter or asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
                        async with sem:
                            vol, mc = await get_dexscreener_data(session, mint, chain)

                        # Update token state and evaluate
                        async with tokens_lock:
                            t = tokens.get(mint)
                            if not t:
                                return
                            t.volume_sol = vol if vol is not None else 0.0
                            t.market_cap = mc if mc is not None else 0.0
                            passed, metrics = check_smart_volume(t)
                            if passed:
                                t.first_alert_sent = True
                                token_snapshot = t
                            else:
                                token_snapshot = None

                        # Notify outside the lock
                        if token_snapshot and token_snapshot.first_alert_sent:
                            await notify_smart_token(token_snapshot, metrics)
                    except Exception as e:
                        logger.debug(f"sync process error for {mint}: {e}")

                await asyncio.gather(*(process_mint(m, c) for m, c in mints_with_chain))
        except Exception as e:
            logger.error(f"sync_volumes_and_detect error: {e}")
        await asyncio.sleep(SLEEP_SECONDS)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 <b>Smart Volume Tracker v3.0</b>\\n\\n"
        "Detects tokens with real buying pressure\\n"
        "/status - Stats",
        parse_mode='HTML'
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int(time.time() - start_time)
    hours, remainder = divmod(uptime, 3600)
    minutes, _ = divmod(remainder, 60)
    
    await update.message.reply_text(
        f"🤖 <b>Status</b>\\n\\n"
        f"Tokens tracked: {len(tokens)}\\n"
        f"Uptime: {hours}h {minutes}m\\n"
        f"Mode: Smart Detection",
        parse_mode='HTML'
    )

async def cleanup_old_tokens():
    """Remove old tokens."""
    while True:
        try:
            cutoff = time.time() - (MONITOR_HOURS_OLD * 3600)
            
            async with tokens_lock:
                to_remove = [m for m, t in tokens.items() if t.created_at < cutoff]
                if to_remove:
                    logger.info(f"🧹 Cleaning {len(to_remove)} old tokens")
                    for mint in to_remove:
                        del tokens[mint]
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        
        await asyncio.sleep(300)


# ============================================================================
# Heavy Volume Detection (Smart Algorithm)
# ============================================================================
async def check_heavy_volume():
    """Smart heavy volume detection using velocity, acceleration, and peer ranking.
    
    Score = (Velocity Factor × 0.6) + (Acceleration Factor × 0.3) + (Peer Rank Bonus × 0.1)
    - Velocity: SOL/hour (age-normalized)
    - Acceleration: % change in velocity last 5 min
    - Peer Rank: Token's volume rank vs all tracked (bonus if top 10)
    """
    if not HEAVY_VOLUME_ENABLED:
        logger.info("🌋 Heavy volume detection disabled")
        return
    
    logger.info("🌋 Starting Heavy Volume Detection...")
    
    while True:
        try:
            current_time = time.time()
            
            async with tokens_lock:
                # Get active tokens: recent + minimum volume
                all_tokens = list(tokens.values())
                active_tokens = [
                    info for info in all_tokens 
                    if info.volume_sol >= HEAVY_VOLUME_MIN_VOLUME_SOL 
                    and (current_time - info.created_at) < (HEAVY_VOLUME_MAX_AGE_HOURS * 3600)
                ]
            
            # Debug logging every check
            logger.info(f"🌋 Heavy volume check: {len(all_tokens)} total tokens, {len(active_tokens)} active (≥{HEAVY_VOLUME_MIN_VOLUME_SOL} SOL, <{HEAVY_VOLUME_MAX_AGE_HOURS}h)")
            
            # Need enough tokens for peer comparison (lowered to 1 for testing)
            if len(active_tokens) < 1:
                await asyncio.sleep(60)
                continue
            
            # Update velocity history for all active tokens
            for token in active_tokens:
                token.velocity_history.append((current_time, token.volume_sol))
                # Keep last 12 snapshots (~1 hour at 5-min intervals)
                if len(token.velocity_history) > 12:
                    token.velocity_history.pop(0)
            
            # Calculate scores and find heavy volume tokens
            scored_tokens = []
            for token in active_tokens:
                if len(token.velocity_history) < 2:
                    continue
                
                # Calculate velocity (SOL/hour)
                dt = token.velocity_history[-1][0] - token.velocity_history[0][0]
                dv = token.velocity_history[-1][1] - token.velocity_history[0][1]
                velocity = dv / max(dt / 3600, 0.1)  # SOL/hour
                
                # Calculate acceleration (% change in velocity)
                accel = 0.0
                if len(token.velocity_history) >= 3:
                    # Compare current velocity to previous velocity
                    prev_dt = token.velocity_history[-2][0] - token.velocity_history[-3][0]
                    prev_dv = token.velocity_history[-2][1] - token.velocity_history[-3][1]
                    prev_velocity = prev_dv / max(prev_dt / 3600, 0.1) if prev_dt > 0 else 0
                    if prev_velocity > 0:
                        accel = (velocity - prev_velocity) / prev_velocity
                
                scored_tokens.append((token, velocity, accel))
            
            if not scored_tokens:
                await asyncio.sleep(60)
                continue
            
            # Calculate peer rankings (by total volume)
            sorted_by_volume = sorted(scored_tokens, key=lambda x: x[0].volume_sol, reverse=True)
            volume_ranks = {id(t[0]): i for i, t in enumerate(sorted_by_volume)}
            
            # Calculate final scores
            for token, velocity, accel in scored_tokens:
                # Velocity factor (normalized to peak expectation)
                velocity_factor = min(velocity / HEAVY_VOLUME_VELOCITY_PEAK, 1.0) * 10  # 0-10
                
                # Acceleration factor (positive acceleration = good)
                accel_factor = min(max(accel, 0) * 10, 10)  # 0-10, cap at 10
                
                # Peer rank bonus (top tokens get bonus)
                rank = volume_ranks[id(token)]
                total = len(scored_tokens)
                rank_percentile = 1 - (rank / total)  # 1.0 = top, 0.0 = bottom
                rank_factor = rank_percentile * 10  # 0-10
                
                # Combined score with weights
                score = (velocity_factor * 0.6) + (accel_factor * 0.3) + (rank_factor * 0.1)
                token.heavy_volume_score = score
                
                # Debug: log top scoring tokens
                if score >= 5.0:
                    logger.info(f"🌋 {token.symbol}: score={score:.1f} vel={velocity:.1f} SOL/hr accel={accel:.1%} rank=#{rank+1}/{total}")
                
                # Check if qualifies for alert
                if score >= HEAVY_VOLUME_SCORE_THRESHOLD and not token.heavy_volume_notified:
                    logger.info(f"🌋 ALERT TRIGGERED: {token.symbol} score={score:.1f} >= threshold {HEAVY_VOLUME_SCORE_THRESHOLD}")
                    # Optional: require smart money presence
                    smart_vol = sum(b.amount_sol for b in token.recent_buys)
                    if HEAVY_VOLUME_REQUIRE_SMART and smart_vol < 0.5:
                        logger.info(f"🌋 Skipped {token.symbol}: smart_vol={smart_vol:.2f} < 0.5 (REQUIRE_SMART=1)")
                        continue
                    
                    token.heavy_volume_notified = True
                    asyncio.create_task(notify_heavy_volume(token, score, velocity, accel, rank + 1, total, smart_vol))
            
        except Exception as e:
            logger.error(f"Heavy volume check error: {e}")
        
        await asyncio.sleep(60)  # Check every minute


async def notify_heavy_volume(token: TokenInfo, score: float, velocity: float, accel: float, rank: int, total: int, smart_vol: float):
    """Send heavy volume alert with detailed metrics."""
    age_min = (time.time() - token.created_at) / 60
    smart_pct = (smart_vol / token.volume_sol * 100) if token.volume_sol > 0 else 0
    
    # Determine intensity emoji based on score
    if score >= 9.0:
        intensity = "🌋🌋🌋"
        status = "EXTREME"
    elif score >= 8.0:
        intensity = "🌋🌋"
        status = "VERY HIGH"
    else:
        intensity = "🌋"
        status = "HIGH"
    
    message = f"{intensity} <b>HEAVY VOLUME DETECTED</b> {intensity}\n\n"
    message += f"🏷 <b>{token.name or 'Unknown'}</b> (${token.symbol or 'UNK'})\n"
    message += f"🎯 <b>Launchpad:</b> {token.launchpad}\n\n"
    
    message += f"📊 <b>Heavy Score:</b> {score:.1f}/10 ({status})\n"
    message += f"🚀 <b>Velocity:</b> {velocity:.1f} SOL/hour\n"
    if accel > 0:
        message += f"📈 <b>Acceleration:</b> +{accel*100:.0f}%\n"
    message += f"🏆 <b>Peer Rank:</b> #{rank} of {total} active\n\n"
    
    message += f"💰 <b>Total Vol:</b> {token.volume_sol:.2f} SOL\n"
    if smart_vol > 0:
        message += f"🧠 <b>Smart Vol:</b> {smart_vol:.2f} SOL ({smart_pct:.0f}%)\n"
    if token.market_cap > 0:
        message += f"📈 <b>Market Cap:</b> ${token.market_cap:,.0f}\n"
    message += f"⏰ <b>Age:</b> {age_min:.0f} min\n\n"
    
    message += f"<code>{str(token.mint)}</code>\n\n"
    
    chain_cfg = CHAIN_CONFIG.get(token.chain, CHAIN_CONFIG["solana"])
    message += f"🔗 <a href='{chain_cfg['dex_link'].format(address=str(token.mint))}'>DexScreener</a> | "
    message += f"<a href='{chain_cfg['explorer'].format(address=str(token.mint))}'>Explorer</a>\n\n"
    message += f"⚡ <i>Outpacing peers—potential moonshot!</i>"
    
    await send_telegram_message(message, alert_type="heavy_volume")
    logger.info(f"🌋 Heavy volume alert sent: {token.symbol} score={score:.1f} vel={velocity:.1f} SOL/hr")


async def cleanup_old_signatures_task():
    """Periodically clean up old webhook signatures from the database."""
    while True:
        try:
            # Wait 1 hour between cleanups
            await asyncio.sleep(3600)
            
            # Clean signatures older than 24 hours
            deleted = await database.cleanup_old_signatures(max_age_hours=24)
            if deleted and deleted > 0:
                logger.info(f"🧹 Cleaned {deleted} old webhook signatures from DB")
            
            # Also clean up old data
            await database.cleanup_old_data(days=7)
            
        except Exception as e:
            logger.error(f"Signature cleanup error: {e}")

async def rpc_poll_pumpfun_new_tokens():
    """Fallback: poll Pump.fun program txs via RPC and detect new token mints.
    This does NOT compute smart buys; it only sends a 'new token' alert.
    """
    global processed_signatures
    session_timeout = aiohttp.ClientTimeout(total=8)
    while True:
        try:
            # 1) Fetch recent signatures for Pump.fun program
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [PUMPFUN_PROGRAM, {"limit": 50}]
            }
            async with aiohttp.ClientSession(timeout=session_timeout) as s:
                async with s.post(RPC_URL, json=payload) as resp:
                    data = await resp.json()
            sigs = data.get("result", []) or []
            new_sigs = [x["signature"] if isinstance(x, dict) else x for x in sigs]
            # process newest first
            for sig in reversed(new_sigs):
                if sig in processed_signatures:
                    continue
                processed_signatures.add(sig)

                # 2) Fetch transaction and try to extract mint(s)
                tx_payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
                }
                try:
                    async with aiohttp.ClientSession(timeout=session_timeout) as s:
                        async with s.post(RPC_URL, json=tx_payload) as resp:
                            tx_data = await resp.json()
                    result = tx_data.get("result") or {}
                    meta = result.get("meta") or {}
                    post_bal = meta.get("postTokenBalances") or []
                    mints = list({b.get("mint") for b in post_bal if b.get("mint")})
                    for mint in mints:
                        # Track if not already
                        async with tokens_lock:
                            if mint not in tokens:
                                try:
                                    tokens[mint] = TokenInfo(
                                        mint=PublicKey.from_string(mint),
                                        launchpad="Pump.fun",
                                        created_at=time.time(),
                                    )
                                except Exception:
                                    continue
                        # Optional RPC basic alert (suppressed when DISABLE_RPC_BASIC_ALERTS=1)
                        if not DISABLE_RPC_BASIC_ALERTS:
                            await notify_new_token_basic(mint)
                except Exception as e:
                    # Keep polling even if a tx fails to decode
                    logger.debug(f"RPC tx decode error for {sig}: {e}")
        except Exception as e:
            logger.error(f"RPC fallback poll error: {e}")
        await asyncio.sleep(5)

# ============================================================================
# FastAPI Endpoints
# ============================================================================
@app.get("/")
async def root():
    return {
        "name": "Smart Volume Tracker",
        "version": "3.0",
        "status": "online",
        "criteria": {
            "min_buys": MIN_BUYS_3MIN,
            "min_wallets": MIN_UNIQUE_WALLETS,
            "min_sol": MIN_SOL_VOLUME,
            "min_mc": MIN_MARKET_CAP_USD
        }
    }

@app.head("/")
async def root_head():
    return {"status": "ok"}

@app.get("/health")
async def health():
    mode = "rpc-fallback" if (FORCE_RPC_FALLBACK or not HELIUS_API_KEY) else "helius"
    circuit_states = get_all_circuit_states()
    
    # Determine overall health
    all_healthy = all(
        state["state"] != "open" 
        for state in circuit_states.values()
    )
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "mode": mode,
        "tokens": len(tokens),
        "uptime": int(time.time() - start_time),
        "circuit_breakers": circuit_states,
        "features": {
            "holder_analysis": config.detection.enable_holder_analysis,
            "defi_detection": config.detection.enable_defi_alerts,
            "webhook_security": config.webhook.enable_signature_verification,
        }
    }

@app.get("/stats")
async def stats():
    return {
        "tokens_tracked": len(tokens),
        "uptime_seconds": int(time.time() - start_time),
        "criteria": {
            "buys_3min": MIN_BUYS_3MIN,
            "unique_wallets": MIN_UNIQUE_WALLETS,
            "sol_volume": MIN_SOL_VOLUME
        },
        "circuit_breakers": get_all_circuit_states()
    }


@app.get("/queue/status")
async def queue_status():
    """Monitor webhook processing queue status."""
    now = time.time()
    recent_cutoff = now - 10
    recent_count = sum(1 for t in webhook_burst_times if t > recent_cutoff)
    burst_rate = recent_count / 10.0
    
    return {
        "queue_size": process_queue.qsize() if process_queue else 0,
        "queue_max_size": PROCESS_QUEUE_MAX_SIZE,
        "queue_utilization_pct": round((process_queue.qsize() / PROCESS_QUEUE_MAX_SIZE) * 100, 1) if process_queue else 0,
        "burst_rate_per_sec": round(burst_rate, 2),
        "burst_alert_active": webhook_stats.get("_burst_alert_sent_recently", False),
        "webhook_stats": {
            "helius_requests": webhook_stats.get("helius_requests", 0),
            "quicknode_requests": webhook_stats.get("quicknode_requests", 0),
            "total_requests": webhook_stats.get("helius_requests", 0) + webhook_stats.get("quicknode_requests", 0),
            "tokens_found": webhook_stats.get("helius_tokens_found", 0),
            "duplicates_skipped": webhook_stats.get("helius_duplicates_skipped", 0) + webhook_stats.get("quicknode_duplicates_skipped", 0),
            "system_tokens_skipped": webhook_stats.get("helius_system_tokens_skipped", 0),
        },
        "dedup_memory_size": len(recent_webhook_sigs),
    }


@app.get("/metrics")
async def get_metrics():
    """Quick health metrics for monitoring dashboards."""
    now = time.time()
    uptime = int(now - start_time)
    recent_cutoff = now - 10
    burst_rate = sum(1 for t in webhook_burst_times if t > recent_cutoff) / 10.0
    
    # Find tokens with smart volume activity
    smart_tokens = []
    async with tokens_lock:
        for mint, token in list(tokens.items())[:50]:  # Limit for performance
            # Use configurable threshold, also check MC filter
            if token.volume_sol >= SMART_VOLUME_THRESHOLD_SOL:
                # Apply MC filter to avoid rugs (if MC data available)
                if token.market_cap > 0 and token.market_cap < SMART_MIN_MC_FILTER_USD:
                    continue  # Skip low MC tokens (likely rugs)
                smart_tokens.append({
                    "mint": mint[:16] + "...",
                    "volume_sol": round(token.volume_sol, 2),
                    "market_cap": round(token.market_cap, 0),
                    "buys": len(token.recent_buys),
                    "age_min": round((now - token.created_at) / 60, 1),
                    "alerted": token.first_alert_sent,
                })
    
    # Sort by volume descending
    smart_tokens.sort(key=lambda x: x["volume_sol"], reverse=True)
    
    return {
        "status": "healthy",
        "uptime_seconds": uptime,
        "uptime_human": f"{uptime // 3600}h {(uptime % 3600) // 60}m",
        "queue": {
            "size": process_queue.qsize() if process_queue else 0,
            "utilization_pct": round((process_queue.qsize() / PROCESS_QUEUE_MAX_SIZE) * 100, 1) if process_queue else 0,
            "worker_count": QUEUE_WORKER_COUNT,
        },
        "burst_rate_per_sec": round(burst_rate, 2),
        "tokens_tracked": len(tokens),
        "smart_volume_threshold_sol": SMART_VOLUME_THRESHOLD_SOL,
        "smart_min_mc_filter_usd": SMART_MIN_MC_FILTER_USD,
        "smart_volume_tokens": smart_tokens[:10],  # Top 10 by volume
        "alerts_sent_last_hour": sum(1 for t in global_alert_times if t > now - 3600),
        "webhook_requests_total": webhook_stats.get("helius_requests", 0),
    }


@app.get("/config")
async def get_runtime_config():
    """Expose current detection configuration and thresholds."""
    return {
        "detection": {
            "smart_score_threshold": SMART_SCORE_THRESHOLD,
            "smart_cooldown_seconds": SMART_COOLDOWN_SECONDS,
            "smart_global_cap_per_min": SMART_GLOBAL_CAP_PER_MIN,
            "smart_window_seconds": SMART_WINDOW_SECONDS,
            "smart_cluster_window_seconds": SMART_CLUSTER_WINDOW_SECONDS,
            "smart_mc_downweight_factor": SMART_MC_DOWNWEIGHT_FACTOR,
            "min_buys_3min": MIN_BUYS_3MIN,
            "min_unique_wallets": MIN_UNIQUE_WALLETS,
            "min_sol_volume": MIN_SOL_VOLUME,
            "min_market_cap_usd": MIN_MARKET_CAP_USD,
        },
        "resurgence_mode": {
            "enabled": RESURGENCE_MODE,
            "min_token_age_seconds": MIN_TOKEN_AGE_SECONDS,
            "min_token_age_human": f"{MIN_TOKEN_AGE_SECONDS // 3600}h {(MIN_TOKEN_AGE_SECONDS % 3600) // 60}m",
            "require_existing_mc": REQUIRE_EXISTING_MC,
            "min_existing_mc_usd": MIN_EXISTING_MC_USD,
            "description": "Only alert on established tokens regaining volume, not new launches"
        },
        "chains": {
            chain: {
                "name": cfg["name"],
                "native_symbol": cfg["native_symbol"],
                "webhook_endpoints": [f"/webhook/{chain}", f"/webhook/{cfg['native_symbol'].lower()}"] if chain != "solana" else ["/webhook/helius"],
            }
            for chain, cfg in CHAIN_CONFIG.items()
        },
        "features": {
            "holder_analysis": config.detection.enable_holder_analysis,
            "defi_detection": config.detection.enable_defi_alerts,
            "webhook_security": config.webhook.enable_signature_verification,
            "multi_chain": True,
        },
        "rate_limiting": {
            "telegram_min_interval": TELEGRAM_MIN_INTERVAL,
            "telegram_queue_max_size": TELEGRAM_QUEUE_MAX_SIZE,
            "max_concurrent_requests": MAX_CONCURRENT_REQUESTS,
        },
        "queue_scaling": {
            "worker_count": QUEUE_WORKER_COUNT,
            "queue_max_size": PROCESS_QUEUE_MAX_SIZE,
        },
        "smart_volume_tuning": {
            "threshold_sol": SMART_VOLUME_THRESHOLD_SOL,
            "min_mc_filter_usd": SMART_MIN_MC_FILTER_USD,
            "description": "Lower threshold (0.5 SOL) + MC filter (>10k) for quality alerts",
        },
        "suppression": {
            "baseline_alerts_disabled": DISABLE_BASELINE_ALERTS,
            "rpc_basic_alerts_disabled": DISABLE_RPC_BASIC_ALERTS,
            "skip_startup_message": SKIP_STARTUP_MESSAGE,
        }
    }

@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus-compatible metrics endpoint."""
    metrics_module.tokens_tracked_total.set(len(tokens))
    return PlainTextResponse(metrics_module.format_metrics_for_prometheus())

@app.get("/debug/tokens")
async def debug_tokens():
    """Inspect all tracked tokens and their smart volume evaluation."""
    async with tokens_lock:
        snapshot = [explain_smart_volume(t) for t in tokens.values()]
    return {
        "count": len(snapshot),
        "tokens": snapshot
    }

# Webhook stats tracking
webhook_stats = {
    "helius_requests": 0,
    "helius_tokens_found": 0,
    "helius_duplicates_skipped": 0,
    "helius_system_tokens_skipped": 0,
    "eth_requests": 0,
    "bsc_requests": 0,
    "last_helius_request": None,
    "last_eth_request": None,
    "last_bsc_request": None,
}

# Burst tracking (rolling window)
webhook_burst_times: deque = deque(maxlen=1000)  # Track last 1000 webhook timestamps

@app.get("/debug/webhook-stats")
async def debug_webhook_stats():
    """Debug endpoint to see webhook activity and deduplication stats."""
    # Get signature count from DB
    try:
        async with aiosqlite.connect(database.DB_NAME) as db:
            async with db.execute("SELECT COUNT(*) FROM seen_signatures") as cursor:
                row = await cursor.fetchone()
                db_sig_count = row[0] if row else 0
    except:
        db_sig_count = "error"
    
    # Calculate current burst rate
    now = time.time()
    recent_cutoff = now - 60  # Last minute
    recent_count = sum(1 for t in webhook_burst_times if t > recent_cutoff)
    burst_rate_per_min = recent_count
    
    # Last 10 seconds rate
    very_recent_cutoff = now - 10
    very_recent_count = sum(1 for t in webhook_burst_times if t > very_recent_cutoff)
    burst_rate_per_sec = very_recent_count / 10.0
    
    return {
        "stats": webhook_stats,
        "tokens_tracked": len(tokens),
        "deduplication": {
            "memory_sigs_count": len(recent_webhook_sigs),
            "db_sigs_count": db_sig_count,
            "memory_limit": 10000,
            "total_duplicates_skipped": webhook_stats.get("helius_duplicates_skipped", 0),
        },
        "burst_monitoring": {
            "webhooks_last_minute": burst_rate_per_min,
            "webhooks_per_second_10s_avg": round(burst_rate_per_sec, 2),
            "burst_buffer_size": len(webhook_burst_times),
        },
        "ignored_mints": list(IGNORED_MINTS)[:3] + ["..."],  # Show a few examples
        "uptime": int(time.time() - start_time),
    }

@app.post("/simulate/buys")
async def simulate_buys(request: Request):
    """Simulate buy events for a mint to test alerting.
    Body JSON: {"mint": "<mint>", "wallets": 5, "buys_per_wallet": 2, "sol_per_buy": 0.5}
    """
    try:
        body = await request.json()
        mint = body.get("mint")
        wallets = int(body.get("wallets", 5))
        buys_per_wallet = int(body.get("buys_per_wallet", 2))
        sol_per_buy = float(body.get("sol_per_buy", 0.5))
        if not mint:
            return {"status": "error", "message": "mint required"}
        try:
            mint_key = PublicKey.from_string(mint)
        except Exception:
            return {"status": "error", "message": "invalid mint"}
        async with tokens_lock:
            if mint not in tokens:
                tokens[mint] = TokenInfo(mint=mint_key, launchpad="Simulated", created_at=time.time())
            token = tokens[mint]
            # Generate simulated buys
            now = time.time()
            for w in range(wallets):
                wallet_id = f"SIM_WALLET_{w}_{int(now)}"
                for _ in range(buys_per_wallet):
                    token.recent_buys.append(BuyEvent(wallet=wallet_id, amount_sol=sol_per_buy, timestamp=time.time()))
        # Force immediate evaluation via Dex refresh (market_cap may be 0 -> can set via env lowering MC threshold)
        async with tokens_lock:
            passed, metrics = check_smart_volume(tokens[mint])
        if passed and not tokens[mint].first_alert_sent:
            tokens[mint].first_alert_sent = True
            await notify_smart_token(tokens[mint], metrics)
        return {"status": "ok", "mint": mint, "passed": passed, "metrics": metrics}
    except Exception as e:
        logger.error(f"simulate_buys error: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/simulate/evm")
async def simulate_evm_buys(request: Request):
    """Simulate buy events for ETH/BNB tokens to test alerting.
    Body JSON: {"address": "0x...", "chain": "ethereum|bsc", "wallets": 5, "buys_per_wallet": 2, "native_per_buy": 0.1}
    """
    try:
        body = await request.json()
        address = body.get("address", "").lower()
        chain = body.get("chain", "ethereum").lower()
        wallets = int(body.get("wallets", 5))
        buys_per_wallet = int(body.get("buys_per_wallet", 2))
        native_per_buy = float(body.get("native_per_buy", 0.1))
        
        if not address:
            return {"status": "error", "message": "address required"}
        if chain not in ["ethereum", "bsc"]:
            return {"status": "error", "message": "chain must be 'ethereum' or 'bsc'"}
        
        chain_cfg = CHAIN_CONFIG.get(chain, CHAIN_CONFIG["ethereum"])
        
        async with tokens_lock:
            if address not in tokens:
                tokens[address] = TokenInfo(
                    mint=address,  # Store address as string
                    launchpad=f"Simulated {chain_cfg['name']}",
                    created_at=time.time(),
                    chain=chain,
                    name="SimToken",
                    symbol="SIM",
                )
            token = tokens[address]
            # Generate simulated buys
            now = time.time()
            for w in range(wallets):
                wallet_id = f"0xSIM_WALLET_{w}_{int(now)}"
                for _ in range(buys_per_wallet):
                    token.recent_buys.append(BuyEvent(
                        wallet=wallet_id,
                        amount_sol=native_per_buy,
                        timestamp=time.time()
                    ))
        
        async with tokens_lock:
            passed, metrics = check_smart_volume(tokens[address])
        if passed and not tokens[address].first_alert_sent:
            tokens[address].first_alert_sent = True
            await notify_smart_token(tokens[address], metrics)
        
        return {
            "status": "ok",
            "address": address,
            "chain": chain,
            "passed": passed,
            "metrics": metrics
        }
    except Exception as e:
        logger.error(f"simulate_evm_buys error: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/test-alert")
async def test_alert():
    """Test endpoint to verify Telegram alerts are working."""
    try:
        if not telegram_bot or not TELEGRAM_CHAT_ID:
            return {
                "status": "error",
                "message": "Telegram bot not configured",
                "bot_connected": False
            }
        
        # Send test notification
        test_message = "🧪 <b>ALERT TEST</b>\n\n"
        test_message += "✅ Telegram bot is connected and working!\n"
        test_message += "✅ Alert system is functional!\n\n"
        test_message += f"📊 Currently tracking: {len(tokens)} tokens\n"
        test_message += f"⏱ Uptime: {int((time.time() - start_time) / 60)} minutes\n"
        test_message += f"💬 Chat ID: {TELEGRAM_CHAT_ID}\n\n"
        test_message += "🎯 <b>Setup Status:</b>\n"
        test_message += "• Bot: ✅ Connected\n"
        test_message += "• Webhook: Ready for Helius events\n"
        test_message += "• Filters: Monitoring for alpha signals\n\n"
        test_message += "💡 Configure Helius webhook to start receiving live alerts!"
        
        await telegram_bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=test_message,
            parse_mode='HTML'
        )
        # Replace direct send with queued send for consistency
        # (Keeps the immediate result response while respecting rate limits for subsequent sends)
        await send_telegram_message("✅ Test acknowledged", alert_type="test")
        
        return {
            "status": "success",
            "message": "Test alert sent! Check your Telegram.",
            "bot_connected": True,
            "chat_id": TELEGRAM_CHAT_ID,
            "tokens_tracked": len(tokens)
        }
    except Exception as e:
        logger.error(f"Test alert error: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "bot_connected": telegram_bot is not None
        }

@app.post("/webhook/helius")
async def helius_webhook(request: Request, background_tasks: BackgroundTasks = None):
    """Enhanced Helius webhook with queue-based processing for burst handling.
    
    Fast path: deduplication, validation, enqueue
    Slow path: DeFi detection, DexScreener API calls (handled by queue worker)
    """
    start_time_req = time.time()
    client_ip = request.client.host if request.client else 'unknown'
    
    # Track burst timing
    webhook_burst_times.append(start_time_req)
    
    # Calculate current burst rate (webhooks per second in last 10s)
    recent_cutoff = start_time_req - 10
    recent_count = sum(1 for t in webhook_burst_times if t > recent_cutoff)
    burst_rate = recent_count / 10.0
    
    # Log with burst info if high velocity
    if burst_rate > 5:
        queue_size = process_queue.qsize() if process_queue else 0
        logger.info(f"📥 Helius webhook from {client_ip} | Burst: {burst_rate:.1f}/sec | Queue: {queue_size}")
        
        # Send Telegram alert if burst exceeds 10/sec (possible meme wave)
        if burst_rate > 10 and not webhook_stats.get("_burst_alert_sent_recently"):
            webhook_stats["_burst_alert_sent_recently"] = True
            asyncio.create_task(send_burst_alert(burst_rate))
            asyncio.create_task(reset_burst_alert_flag())
    else:
        logger.info(f"📥 Helius webhook received from {client_ip}")
    
    # Track webhook stats
    webhook_stats["helius_requests"] += 1
    webhook_stats["last_helius_request"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    
    try:
        metrics_module.webhook_requests_total.inc()
        
        # Read raw body and handle gzip compression
        raw_body = await request.body()
        content_encoding = request.headers.get("content-encoding", "").lower()
        
        if content_encoding == "gzip" or (len(raw_body) >= 2 and raw_body[0:2] == b'\x1f\x8b'):
            try:
                raw_body = gzip.decompress(raw_body)
                logger.debug("Decompressed gzip payload")
            except Exception as e:
                logger.error(f"Gzip decompression error: {e}")
                return JSONResponse(status_code=400, content={"status": "error", "message": "Failed to decompress gzip payload"})
        
        # Rate limiting check
        if request_rate_limiter and not request_rate_limiter.is_allowed(client_ip):
            return JSONResponse(status_code=429, content={"status": "error", "message": "Rate limited"})
        
        # Parse JSON
        try:
            data = json.loads(raw_body)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid JSON"})
        
        payloads = data if isinstance(data, list) else [data]
        enqueued = 0
        skipped_dupes = 0
        
        for payload in payloads:
            # Extract signature early for deduplication
            evt_sig = payload.get("signature") or payload.get("transactionSignature")
            
            # ==== FAST DEDUPLICATION (stays inline) ====
            if evt_sig:
                # Fast check: in-memory set first
                if evt_sig in recent_webhook_sigs:
                    skipped_dupes += 1
                    continue
                
                # Slow check: database for persistent deduplication
                if await database.is_signature_seen(evt_sig):
                    skipped_dupes += 1
                    recent_webhook_sigs.add(evt_sig)
                    continue
                
                # Mark as seen immediately (prevents race conditions)
                recent_webhook_sigs.add(evt_sig)
                if len(recent_webhook_sigs) > 10000:
                    for _ in range(2000):
                        recent_webhook_sigs.pop()
            
            # ==== ENQUEUE FOR BACKGROUND PROCESSING ====
            if process_queue:
                try:
                    # Non-blocking put - if queue is full, process inline as fallback
                    process_queue.put_nowait((evt_sig, payload))
                    enqueued += 1
                except asyncio.QueueFull:
                    # Queue full - fallback to direct processing
                    logger.warning(f"⚠️ Process queue full ({PROCESS_QUEUE_MAX_SIZE}), processing inline")
                    asyncio.create_task(process_queued_payload(evt_sig, payload))
                    enqueued += 1
            else:
                # Queue not initialized - process directly
                asyncio.create_task(process_queued_payload(evt_sig, payload))
                enqueued += 1
        
        # Track processing time (just ingestion time, not processing)
        duration = time.time() - start_time_req
        metrics_module.webhook_processing_duration_seconds.observe(duration)
        
        # Update global stats
        webhook_stats["helius_duplicates_skipped"] = webhook_stats.get("helius_duplicates_skipped", 0) + skipped_dupes
        
        if skipped_dupes > 0:
            logger.debug(f"Skipped {skipped_dupes} duplicate signatures")
        
        return {"status": "ok", "enqueued": enqueued, "skipped_duplicates": skipped_dupes}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        metrics_module.api_errors_total.inc(service="webhook")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


def is_launchpad_transaction(tx: dict) -> Tuple[bool, Optional[str]]:
    """Check if transaction involves a launchpad program (Pump.fun, Moonshot, etc).
    
    Returns (is_launchpad, launchpad_name)
    """
    meta = tx.get("meta", {})
    
    # Check log messages for program invocations
    log_messages = meta.get("logMessages", [])
    for log in log_messages:
        for program_id, name in LAUNCHPAD_PROGRAMS.items():
            if f"Program {program_id}" in log:
                return True, name
    
    # Check account keys in the transaction
    transaction = tx.get("transaction", {})
    message = transaction.get("message", {})
    account_keys = message.get("accountKeys", [])
    
    # Handle both string array and object array formats
    for key in account_keys:
        key_str = key if isinstance(key, str) else key.get("pubkey", "")
        if key_str in LAUNCHPAD_PROGRAMS:
            return True, LAUNCHPAD_PROGRAMS[key_str]
    
    # Check loaded addresses (for versioned transactions)
    loaded = meta.get("loadedAddresses", {})
    for addr in loaded.get("writable", []) + loaded.get("readonly", []):
        if addr in LAUNCHPAD_PROGRAMS:
            return True, LAUNCHPAD_PROGRAMS[addr]
    
    return False, None


@app.post("/webhook/quicknode")
async def quicknode_webhook(request: Request, background_tasks: BackgroundTasks = None):
    """QuickNode Solana webhook handler with Pump.fun/launchpad filtering.
    
    QuickNode sends different payload format than Helius:
    - matchedTransactions[] array with full transaction data
    - Each tx has signature, slot, blockTime, meta, transaction fields
    
    Filtering: Only processes transactions from launchpad programs (Pump.fun, Moonshot, etc.)
    """
    start_time_req = time.time()
    client_ip = request.client.host if request.client else 'unknown'
    
    # Track burst timing
    webhook_burst_times.append(start_time_req)
    recent_cutoff = start_time_req - 10
    recent_count = sum(1 for t in webhook_burst_times if t > recent_cutoff)
    burst_rate = recent_count / 10.0
    
    # Track webhook stats
    webhook_stats["quicknode_requests"] = webhook_stats.get("quicknode_requests", 0) + 1
    webhook_stats["last_quicknode_request"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    req_count = webhook_stats["quicknode_requests"]
    
    # Only log every 25th request to reduce noise (filter stats will show activity)
    if burst_rate > 5:
        queue_size = process_queue.qsize() if process_queue else 0
        logger.info(f"📥 QuickNode #{req_count} from {client_ip} | Burst: {burst_rate:.1f}/sec | Queue: {queue_size}")
    elif req_count % 25 == 0:
        logger.info(f"📥 QuickNode #{req_count} received from {client_ip}")
    
    try:
        metrics_module.webhook_requests_total.inc()
        
        # Read raw body
        raw_body = await request.body()
        content_encoding = request.headers.get("content-encoding", "").lower()
        
        # Handle gzip if present
        if content_encoding == "gzip" or (len(raw_body) >= 2 and raw_body[0:2] == b'\x1f\x8b'):
            try:
                raw_body = gzip.decompress(raw_body)
            except Exception as e:
                logger.error(f"Gzip decompression error: {e}")
                return JSONResponse(status_code=400, content={"status": "error", "message": "Gzip decompression failed"})
        
        # Parse JSON
        try:
            data = json.loads(raw_body)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid JSON"})
        
        # QuickNode format: matchedTransactions array or direct transaction list
        transactions = []
        if isinstance(data, list):
            transactions = data
        elif isinstance(data, dict):
            transactions = data.get("matchedTransactions", []) or data.get("transactions", []) or [data]
        
        enqueued = 0
        skipped_dupes = 0
        skipped_non_launchpad = 0
        
        for tx in transactions:
            # Extract signature from QuickNode format
            evt_sig = None
            if isinstance(tx, dict):
                evt_sig = tx.get("signature") or tx.get("transaction", {}).get("signatures", [None])[0]
            
            # 🎯 PUMP.FUN FILTER: Skip non-launchpad transactions
            if QUICKNODE_FILTER_ENABLED:
                is_launchpad, launchpad_name = is_launchpad_transaction(tx)
                if not is_launchpad:
                    skipped_non_launchpad += 1
                    continue
                # Tag the launchpad for later use
                tx["_detected_launchpad"] = launchpad_name
            
            # Deduplication
            if evt_sig:
                if evt_sig in recent_webhook_sigs:
                    skipped_dupes += 1
                    continue
                if await database.is_signature_seen(evt_sig):
                    skipped_dupes += 1
                    recent_webhook_sigs.add(evt_sig)
                    continue
                recent_webhook_sigs.add(evt_sig)
                if len(recent_webhook_sigs) > 10000:
                    for _ in range(2000):
                        recent_webhook_sigs.pop()
            
            # Convert QuickNode format to normalized payload for queue worker
            normalized_payload = normalize_quicknode_payload(tx)
            
            # Enqueue for processing
            if process_queue:
                try:
                    process_queue.put_nowait((evt_sig, normalized_payload))
                    enqueued += 1
                except asyncio.QueueFull:
                    logger.warning(f"⚠️ Process queue full, processing inline")
                    asyncio.create_task(process_queued_payload(evt_sig, normalized_payload))
                    enqueued += 1
            else:
                asyncio.create_task(process_queued_payload(evt_sig, normalized_payload))
                enqueued += 1
        
        duration = time.time() - start_time_req
        metrics_module.webhook_processing_duration_seconds.observe(duration)
        
        webhook_stats["quicknode_duplicates_skipped"] = webhook_stats.get("quicknode_duplicates_skipped", 0) + skipped_dupes
        webhook_stats["quicknode_non_launchpad_skipped"] = webhook_stats.get("quicknode_non_launchpad_skipped", 0) + skipped_non_launchpad
        webhook_stats["quicknode_enqueued"] = webhook_stats.get("quicknode_enqueued", 0) + enqueued
        
        # Only log when we actually find a launchpad tx (important!) or every 100th filtered
        total_filtered = webhook_stats.get("quicknode_non_launchpad_skipped", 0)
        total_enqueued = webhook_stats.get("quicknode_enqueued", 0)
        if enqueued > 0:
            logger.info(f"🎯 QuickNode FOUND: {enqueued} launchpad tx QUEUED! Total: {total_enqueued} queued / {total_filtered} filtered")
        elif total_filtered % 100 == 0 and total_filtered > 0:
            logger.info(f"🎯 QuickNode filter: {total_filtered} non-launchpad filtered, {total_enqueued} launchpad found")
        
        return {
            "status": "ok", 
            "enqueued": enqueued, 
            "skipped_duplicates": skipped_dupes, 
            "skipped_non_launchpad": skipped_non_launchpad,
            "filter_enabled": QUICKNODE_FILTER_ENABLED,
            "source": "quicknode"
        }
    
    except Exception as e:
        logger.error(f"QuickNode webhook error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


def normalize_quicknode_payload(tx: dict) -> dict:
    """Convert QuickNode transaction format to normalized format matching Helius structure.
    
    QuickNode sends raw Solana transaction data, we normalize it to match
    what our process_queued_payload() expects.
    """
    normalized = {
        "signature": tx.get("signature") or tx.get("transaction", {}).get("signatures", [None])[0],
        "slot": tx.get("slot"),
        "blockTime": tx.get("blockTime"),
        "tokenTransfers": [],
        "mint": None,
    }
    
    # Extract token transfers from QuickNode meta
    meta = tx.get("meta", {})
    
    # Pre/Post token balances method
    pre_balances = {b.get("accountIndex"): b for b in meta.get("preTokenBalances", [])}
    post_balances = {b.get("accountIndex"): b for b in meta.get("postTokenBalances", [])}
    
    for idx, post in post_balances.items():
        pre = pre_balances.get(idx, {})
        pre_amount = float(pre.get("uiTokenAmount", {}).get("uiAmount") or 0)
        post_amount = float(post.get("uiTokenAmount", {}).get("uiAmount") or 0)
        
        if post_amount > pre_amount:  # This is a receive/buy
            mint = post.get("mint")
            if mint and mint not in IGNORED_MINTS:
                normalized["tokenTransfers"].append({
                    "mint": mint,
                    "tokenAmount": post_amount - pre_amount,
                    "toUserAccount": post.get("owner"),
                })
                # Set primary mint
                if not normalized["mint"]:
                    normalized["mint"] = mint
    
    # Fallback: check inner instructions for token program calls
    if not normalized["mint"]:
        inner_instructions = meta.get("innerInstructions", [])
        for inner in inner_instructions:
            for ix in inner.get("instructions", []):
                # Look for token mint in parsed data
                parsed = ix.get("parsed", {})
                if isinstance(parsed, dict):
                    info = parsed.get("info", {})
                    mint = info.get("mint")
                    if mint and mint not in IGNORED_MINTS:
                        normalized["mint"] = mint
                        break
    
    # Use pre-detected launchpad from filter, or detect from logs
    if tx.get("_detected_launchpad"):
        normalized["launchpad"] = tx["_detected_launchpad"]
    else:
        # Fallback: Check logs for Pump.fun create events
        log_messages = meta.get("logMessages", [])
        for log in log_messages:
            for program_id, name in LAUNCHPAD_PROGRAMS.items():
                if f"Program {program_id}" in log:
                    normalized["launchpad"] = name
                    break
            if normalized.get("launchpad"):
                break
    
    return normalized


async def process_evm_webhook(request: Request, chain: str, background_tasks: BackgroundTasks = None):
    """Generic EVM chain webhook handler for ETH/BNB tokens with persistent deduplication."""
    start_time_req = time.time()
    client_ip = request.client.host if request.client else 'unknown'
    logger.info(f"📥 {chain.upper()} webhook received from {client_ip}")
    
    # Update stats
    webhook_stats[f"{chain}_requests"] = webhook_stats.get(f"{chain}_requests", 0) + 1
    webhook_stats[f"last_{chain}_request"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    
    try:
        metrics_module.webhook_requests_total.inc()
        
        # Read raw body and handle gzip compression
        raw_body = await request.body()
        content_encoding = request.headers.get("content-encoding", "").lower()
        
        # Decompress if gzip encoded (check header or magic bytes)
        if content_encoding == "gzip" or (len(raw_body) >= 2 and raw_body[0:2] == b'\x1f\x8b'):
            try:
                raw_body = gzip.decompress(raw_body)
                logger.debug(f"Decompressed gzip payload for {chain}")
            except Exception as e:
                logger.error(f"Gzip decompression error for {chain}: {e}")
                return JSONResponse(status_code=400, content={"status": "error", "message": "Failed to decompress gzip payload"})
        
        data = json.loads(raw_body)
        payloads = data if isinstance(data, list) else [data]
        processed = 0
        skipped_dupes = 0
        
        chain_cfg = CHAIN_CONFIG.get(chain, CHAIN_CONFIG["ethereum"])
        native_symbol = chain_cfg["native_symbol"]
        
        for payload in payloads:
            # Dedupe by transaction hash with persistent storage
            tx_hash = payload.get("transactionHash") or payload.get("hash") or payload.get("txHash")
            if tx_hash:
                # Fast check: in-memory
                if tx_hash in recent_webhook_sigs:
                    skipped_dupes += 1
                    continue
                
                # Slow check: database
                if await database.is_signature_seen(tx_hash):
                    skipped_dupes += 1
                    recent_webhook_sigs.add(tx_hash)
                    continue
                
                # Mark as seen
                recent_webhook_sigs.add(tx_hash)
                if len(recent_webhook_sigs) > 10000:
                    for _ in range(2000):
                        recent_webhook_sigs.pop()
            
            # Extract token address - support multiple formats
            token_address = (
                payload.get("tokenAddress") or 
                payload.get("contractAddress") or 
                payload.get("token") or
                payload.get("address")
            )
            
            if not token_address:
                # Try to extract from logs/events
                logs = payload.get("logs", [])
                for log in logs:
                    if log.get("address"):
                        token_address = log["address"]
                        break
            
            if not token_address:
                if tx_hash:
                    await database.add_seen_signature(tx_hash, None)
                continue
            
            # Normalize address (lowercase for EVM)
            token_address = token_address.lower()
            
            # Persist signature with address
            if tx_hash:
                await database.add_seen_signature(tx_hash, token_address)
            
            # Extract buy info
            buyer_wallet = (
                payload.get("from") or 
                payload.get("buyer") or 
                payload.get("sender") or
                payload.get("fromAddress")
            )
            
            # Try to extract value/amount
            value_native = 0.0
            if payload.get("value"):
                try:
                    # Wei to native token
                    value_native = float(payload["value"]) / 1e18
                except:
                    pass
            elif payload.get("amount"):
                try:
                    value_native = float(payload["amount"])
                except:
                    pass
            
            # Get token name/symbol from payload if available
            token_name = payload.get("tokenName") or payload.get("name") or "Unknown"
            token_symbol = payload.get("tokenSymbol") or payload.get("symbol") or "UNK"
            
            # Ensure token exists
            async with tokens_lock:
                if token_address not in tokens:
                    # Create a placeholder PublicKey-like object (just store as string for EVM)
                    tokens[token_address] = TokenInfo(
                        mint=PublicKey.default(),  # Placeholder for EVM
                        launchpad=f"{chain_cfg['name']} DEX",
                        created_at=time.time(),
                        chain=chain,
                        name=token_name,
                        symbol=token_symbol,
                    )
                    # Store actual address in a way we can retrieve
                    tokens[token_address].mint = token_address  # Override with string
                
                # Record buy event
                if buyer_wallet and value_native > 0:
                    tokens[token_address].recent_buys.append(BuyEvent(
                        wallet=buyer_wallet.lower(),
                        amount_sol=value_native,  # Using native token amount
                        timestamp=time.time()
                    ))
            
            # Background: refresh Dex data and possibly alert
            async def process_evm_token(addr: str, chain_id: str):
                try:
                    cb = get_circuit_breaker("dexscreener", CircuitBreakerConfig(
                        failure_threshold=config.api.api_failure_threshold,
                        recovery_timeout=config.api.api_recovery_timeout
                    ))
                    
                    async with aiohttp.ClientSession() as session:
                        metrics_module.api_calls_total.inc(service="dexscreener")
                        vol, mc = await cb.call(get_dexscreener_data, session, addr, chain_id)
                    
                    async with tokens_lock:
                        t = tokens.get(addr)
                        if not t:
                            return
                        t.volume_sol = vol if vol is not None else 0.0
                        t.market_cap = mc if mc is not None else 0.0
                        passed, metrics = check_smart_volume(t)
                        if passed and not t.first_alert_sent:
                            t.first_alert_sent = True
                            t.last_alert_ts = time.time()
                            metrics_module.smart_volume_detections_total.inc()
                    
                    async with tokens_lock:
                        t2 = tokens.get(addr)
                    if t2 and t2.first_alert_sent:
                        global_alert_times.append(time.time())
                        await notify_smart_token(t2, metrics)
                        
                except CircuitBreakerOpenError:
                    logger.warning(f"DexScreener circuit open for {addr[:8]}")
                except Exception as e:
                    logger.error(f"process_evm_token error for {addr}: {e}")
            
            try:
                if background_tasks:
                    background_tasks.add_task(process_evm_token, token_address, chain)
                else:
                    asyncio.create_task(process_evm_token(token_address, chain))
            except Exception:
                asyncio.create_task(process_evm_token(token_address, chain))
            
            processed += 1
        
        duration = time.time() - start_time_req
        metrics_module.webhook_processing_duration_seconds.observe(duration)
        
        if skipped_dupes > 0:
            logger.debug(f"Skipped {skipped_dupes} duplicate {chain} signatures")
        
        return {"status": "ok", "processed": processed, "chain": chain, "skipped_duplicates": skipped_dupes}
    
    except Exception as e:
        logger.error(f"{chain} webhook error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@app.post("/webhook/ethereum")
@app.post("/webhook/eth")
async def ethereum_webhook(request: Request, background_tasks: BackgroundTasks = None):
    """Webhook endpoint for Ethereum token events."""
    return await process_evm_webhook(request, "ethereum", background_tasks)


@app.post("/webhook/bsc")
@app.post("/webhook/bnb")
async def bsc_webhook(request: Request, background_tasks: BackgroundTasks = None):
    """Webhook endpoint for BNB Chain token events."""
    return await process_evm_webhook(request, "bsc", background_tasks)


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """Telegram webhook endpoint."""
    try:
        if not telegram_app:
            return {"status": "not configured"}
        
        payload = await request.json()
        update = Update.de_json(payload, telegram_app.bot)
        try:
            if update and update.message and update.message.text:
                logger.info(f"TG update: {update.message.from_user.username or update.message.from_user.id} -> {update.message.text}")
        except Exception:
            pass
        await telegram_app.process_update(update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        return {"status": "error"}

@app.get("/debug/telegram")
async def debug_telegram():
    """Return Telegram bot and webhook status for diagnostics."""
    if not telegram_app:
        return {"configured": False}
    try:
        me = await telegram_app.bot.get_me()
        info = await telegram_app.bot.get_webhook_info()
        cmds = await telegram_app.bot.get_my_commands()
        return {
            "configured": True,
            "bot": {
                "id": me.id,
                "username": me.username,
                "name": f"{me.first_name or ''} {me.last_name or ''}".strip(),
            },
            "webhook": {
                "url": info.url,
                "has_custom_certificate": info.has_custom_certificate,
                "pending_update_count": info.pending_update_count,
                "ip_address": info.ip_address,
                "max_connections": info.max_connections,
                "last_error_date": info.last_error_date,
                "last_error_message": info.last_error_message,
                "allowed_updates": info.allowed_updates,
            },
            "commands": [
                {"command": c.command, "description": c.description} for c in cmds
            ],
            "chat_id_configured": bool(TELEGRAM_CHAT_ID),
        }
    except Exception as e:
        logger.error(f"debug_telegram error: {e}")
        return {"configured": True, "error": str(e)}

# ============================================================================
# Main
# ============================================================================
if __name__ == "__main__":
    logger.info(f"Starting Smart Volume Tracker on port {PORT}...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info"
    )
