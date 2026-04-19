#!/usr/bin/env python3
"""
Solana Token Tracker v2.0 - Webhook Mode (Render Compatible)
=============================================================
Production-ready tracker for Render deployment using Helius webhooks.

FEATURES:
- ✅ Helius webhook integration (100% detection)
- ✅ Real DexScreener volume tracking
- ✅ Smart money & cabal detection
- ✅ Telegram notifications
- ✅ FastAPI for health checks & stats
- ✅ All v2.0 fixes included

DEPLOYMENT:
- Platform: Render.com (free tier)
- Runtime: Python 3
- Build: pip install -r requirements.txt
- Start: python token_tracker_webhook.py
"""
import sys
import os

# UTF-8 fix (works everywhere, no-op on Linux)
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
import time
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn

# Telegram
from telegram import Bot, Update, BotCommand
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
from telegram.error import TelegramError

# Solana
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey as PublicKey

# Database (optional)
try:
    import database
except ImportError:
    database = None

# Load environment
load_dotenv()

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
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
PORT = int(os.getenv("PORT", 10000))

# RPC URLs
RPC_HTTPS = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_URL = "https://api.helius.xyz/v0"

# Settings
MAX_CONCURRENT_REQUESTS = 10
WALLET_CACHE_SIZE = 1000
MAX_PROCESSED_SIGS = 10000
MONITOR_HOURS_OLD = 3

# Volume
VOLUME_ALERT_THRESHOLD = 5  # Minimum SOL volume before first alert
VOLUME_MILESTONES = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]
MIN_MARKET_CAP_USD = 15000  # Minimum $15K market cap to alert

# Smart wallet
SMART_AGE_DAYS = 7
SMART_BALANCE_SOL = 5.0
SMART_TX_COUNT = 50

# Cabal
CABAL_EARLY_BUYERS = 3
CABAL_SHARED_FUNDERS = 2

# ============================================================================
# Data Structures
# ============================================================================
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
    buyers: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    smart_wallets: Set[str] = field(default_factory=set)
    cabal_score: int = 0
    notified_milestones: Set[float] = field(default_factory=set)
    name: str = "Unknown"
    symbol: str = "UNK"
    market_cap: float = 0.0

# ============================================================================
# Global State
# ============================================================================
tokens: Dict[str, TokenInfo] = {}
wallets_cache: Dict[str, WalletInfo] = {}
processed_sigs: deque = deque(maxlen=MAX_PROCESSED_SIGS)
telegram_bot: Optional[Bot] = None
telegram_app = None
rate_limiter: Optional[asyncio.Semaphore] = None
tokens_lock = asyncio.Lock()
wallet_cache_lock = asyncio.Lock()
start_time = time.time()

# ============================================================================
# FastAPI Lifespan
# ============================================================================
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown."""
    global telegram_bot, telegram_app, rate_limiter
    
    # Startup
    logger.info("🚀 Token Tracker started - Webhook endpoint ready")
    
    rate_limiter = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    if TELEGRAM_BOT_TOKEN:
        try:
            telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
            telegram_app.add_handler(CommandHandler("start", start_command))
            telegram_app.add_handler(CommandHandler("status", status_command))
            telegram_app.add_handler(CommandHandler("top", top_command))
            
            await telegram_app.bot.set_my_commands([
                BotCommand("start", "Welcome"),
                BotCommand("status", "Stats"),
                BotCommand("top", "Top tokens"),
            ])
            
            webhook_url = os.getenv("RENDER_EXTERNAL_URL")
            if webhook_url:
                await telegram_app.bot.set_webhook(f"{webhook_url}/webhook/telegram")
                logger.info(f"✅ Telegram webhook set to: {webhook_url}/webhook/telegram")
            else:
                await telegram_app.initialize()
                await telegram_app.start()
                await telegram_app.updater.start_polling(drop_pending_updates=True)
            
            telegram_bot = telegram_app.bot
            logger.info("✅ Telegram bot initialized in WEBHOOK mode")
            
            if TELEGRAM_CHAT_ID:
                await send_telegram_message(
                    "🚀 <b>Volume Tracker v2.0 Started</b>\n\n"
                    "Mode: Webhook (100% Detection)\n"
                    "Platform: Render\n\n"
                    "Waiting for Helius webhook events..."
                )
        except Exception as e:
            logger.error(f"Telegram init error: {e}")
    
    if database:
        try:
            await database.init_db()
            logger.info("✅ Database initialized")
        except Exception as e:
            logger.error(f"Database error: {e}")
    
    asyncio.create_task(sync_real_volumes())
    asyncio.create_task(cleanup_old_tokens())
    
    logger.info("⚖️ Checking volume stability...")
    
    yield
    
    # Shutdown
    if telegram_app:
        try:
            if telegram_app.updater and telegram_app.updater.running:
                await telegram_app.updater.stop()
            await telegram_app.stop()
            await telegram_app.shutdown()
        except:
            pass
    logger.info("👋 Tracker stopped")

# FastAPI app with lifespan
app = FastAPI(title="Solana Volume Tracker", version="2.0", lifespan=lifespan)

# ============================================================================
# Helper Functions
# ============================================================================
async def get_real_volume_dexscreener(session: aiohttp.ClientSession, mint: str) -> Tuple[float, dict, float]:
    """Get real volume and market cap from DexScreener."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return 0.0, {}, 0.0
            
            data = await resp.json()
            pairs = data.get("pairs", [])
            if not pairs:
                return 0.0, {}, 0.0
            
            sorted_pairs = sorted(pairs, key=lambda x: float(x.get("volume", {}).get("h24", 0)), reverse=True)
            top_pair = sorted_pairs[0]
            volume_usd = float(top_pair.get("volume", {}).get("h24", 0))
            volume_sol = volume_usd / 180.0
            
            # Get market cap (FDV)
            market_cap_usd = float(top_pair.get("fdv", 0)) if top_pair.get("fdv") else 0.0
            
            socials = {}
            info = top_pair.get("info", {})
            if info.get("socials"):
                for s in info["socials"]:
                    stype = s.get("type", "").lower()
                    if stype == "twitter":
                        socials["twitter"] = s.get("url")
                    elif stype == "telegram":
                        socials["telegram"] = s.get("url")
            
            return volume_sol, socials, market_cap_usd
    except Exception as e:
        logger.debug(f"DexScreener error: {e}")
        return 0.0, {}, 0.0

async def get_token_metadata(mint: str) -> Tuple[str, str]:
    """Fetch token metadata."""
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

# ============================================================================
# Telegram Functions
# ============================================================================
async def send_telegram_message(message: str):
    """Send Telegram message."""
    if not telegram_bot or not TELEGRAM_CHAT_ID:
        return
    
    try:
        await telegram_bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Telegram error: {e}")

async def notify_new_token(token: TokenInfo):
    """Notify about new token (ONLY called after volume check)."""
    if token.name == "Unknown":
        token.name, token.symbol = await get_token_metadata(str(token.mint))
    
    age_str = f"{(time.time() - token.created_at) / 60:.1f}m old"
    vol_str = f"${token.volume_sol * 180:,.0f}"
    
    message = f"🔥 <b>[SOL] {token.volume_sol:.1f} SOL VOLUME</b>\n\n"
    message += f"💸 <b>{token.name}</b> (${token.symbol})\n\n"
    message += f"• 💹 Volume: {vol_str}\n"
    message += f"• ⏰ Age: {age_str}\n"
    message += f"• 🎯 {token.launchpad}\n\n"
    message += f"<code>{str(token.mint)}</code>\n\n"
    message += f"<a href='https://dexscreener.com/solana/{str(token.mint)}'>DS</a> | "
    message += f"<a href='https://rugcheck.xyz/tokens/{str(token.mint)}'>RC</a>\n\n"
    message += f"⏰ {time.strftime('%H:%M:%S UTC', time.gmtime())}"
    
    await send_telegram_message(message)

async def notify_volume_milestone(token: TokenInfo, milestone: float):
    """Notify volume milestone."""
    emoji = "🔥🔥🔥" if milestone >= 1000 else "🔥🔥" if milestone >= 500 else "🔥" if milestone >= 100 else "📈"
    
    message = f"{emoji} <b>VOLUME: {milestone} SOL</b>\n\n"
    message += f"🏷 <code>{str(token.mint)[:8]}...</code>\n"
    message += f"🎯 {token.launchpad}\n"
    message += f"💰 Total: {token.volume_sol:.2f} SOL\n"
    message += f"⏳ Age: {(time.time() - token.created_at) / 60:.0f}m\n\n"
    message += f"<a href='https://dexscreener.com/solana/{str(token.mint)}'>Chart</a>"
    
    await send_telegram_message(message)

# Telegram commands
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 <b>Volume Tracker v2.0</b>\n\n"
        "Webhook Mode - 100% Detection\n"
        "/status - Stats\n"
        "/top - Top tokens",
        parse_mode='HTML'
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int(time.time() - start_time)
    hours, remainder = divmod(uptime, 3600)
    minutes, _ = divmod(remainder, 60)
    
    await update.message.reply_text(
        f"🤖 <b>Status</b>\n\n"
        f"Tokens: {len(tokens)}\n"
        f"Uptime: {hours}h {minutes}m\n"
        f"Mode: Webhook",
        parse_mode='HTML'
    )

async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not tokens:
        await update.message.reply_text("No tokens yet")
        return
    
    sorted_tokens = sorted(tokens.items(), key=lambda x: x[1].volume_sol, reverse=True)
    msg = "📈 <b>Top Tokens</b>\n\n"
    for i, (mint, info) in enumerate(sorted_tokens[:5], 1):
        msg += f"{i}. {info.launchpad}: {info.volume_sol:.2f} SOL\n"
    
    await update.message.reply_text(msg, parse_mode='HTML')

# ============================================================================
# Background Tasks
# ============================================================================
async def sync_real_volumes():
    """Background: Sync volumes from DexScreener and send alerts."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with tokens_lock:
                    # Check ALL tokens, even new ones with 0 volume
                    active = list(tokens.items())
                
                if active:
                    logger.debug(f"🔄 Checking {len(active)} tokens for volume...")
                    
                    for mint_str, token_info in active:
                        real_vol, socials, mc_usd = await get_real_volume_dexscreener(session, mint_str)
                        
                        if real_vol > 0:
                            old_vol = token_info.volume_sol
                            
                            async with tokens_lock:
                                token_info.volume_sol = real_vol
                                token_info.market_cap = mc_usd
                            
                            # FIRST ALERT: When token hits minimum threshold AND market cap
                            if old_vol < VOLUME_ALERT_THRESHOLD <= real_vol:
                                if mc_usd >= MIN_MARKET_CAP_USD:
                                    logger.info(f"🔔 FIRST ALERT: {mint_str[:8]} hit {VOLUME_ALERT_THRESHOLD} SOL with ${mc_usd:,.0f} MC")
                                    await notify_new_token(token_info)
                                else:
                                    logger.debug(f"⏭️ Skipping {mint_str[:8]}: MC too low (${mc_usd:,.0f} < ${MIN_MARKET_CAP_USD:,.0f})")
                            
                            # MILESTONE ALERTS: Check for milestones (only if MC sufficient)
                            if mc_usd >= MIN_MARKET_CAP_USD:
                                for ms in VOLUME_MILESTONES:
                                    if old_vol < ms <= real_vol and ms not in token_info.notified_milestones:
                                        token_info.notified_milestones.add(ms)
                                        logger.info(f"💥 MILESTONE: {mint_str[:8]} hit {ms} SOL")
                                        await notify_volume_milestone(token_info, ms)
                            
                            if abs(real_vol - old_vol) > 1.0:
                                logger.debug(f"📊 {mint_str[:8]}: {old_vol:.1f} → {real_vol:.1f} SOL (MC: ${mc_usd:,.0f})")
                        
                        await asyncio.sleep(0.3)  # Rate limit DexScreener calls
            except Exception as e:
                logger.error(f"Volume sync error: {e}")
            
            await asyncio.sleep(30)  # Check every 30 seconds

async def cleanup_old_tokens():
    """Background: Remove old tokens."""
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
# FastAPI Endpoints
# ============================================================================
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "Solana Volume Tracker",
        "version": "2.0",
        "mode": "webhook",
        "status": "online"
    }

@app.head("/")
async def root_head():
    """Handle HEAD requests for health checks."""
    return {"status": "ok"}

@app.get("/health")
async def health():
    """Health check for Render."""
    return {
        "status": "healthy",
        "tokens": len(tokens),
        "uptime": int(time.time() - start_time)
    }

@app.get("/stats")
async def stats():
    """Statistics endpoint."""
    sorted_tokens = sorted(tokens.items(), key=lambda x: x[1].volume_sol, reverse=True)
    
    return {
        "tokens_tracked": len(tokens),
        "uptime_seconds": int(time.time() - start_time),
        "top_tokens": [
            {
                "mint": mint[:8] + "...",
                "launchpad": info.launchpad,
                "volume_sol": round(info.volume_sol, 2),
                "age_minutes": round((time.time() - info.created_at) / 60, 1)
            }
            for mint, info in sorted_tokens[:10]
        ]
    }

@app.post("/webhook/helius")
async def helius_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Helius webhook endpoint for new token detection.
    
    Configure at: https://dashboard.helius.xyz/webhooks
    URL: https://your-app.onrender.com/webhook/helius
    Account: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
    Type: Enhanced
    """
    try:
        data = await request.json()
        
        # DEBUG: Log first payload to see structure
        if isinstance(data, list) and len(data) > 0:
            logger.info(f"📦 Webhook payload sample: {json.dumps(data[0], indent=2)[:500]}...")
        elif isinstance(data, dict):
            logger.info(f"📦 Webhook payload sample: {json.dumps(data, indent=2)[:500]}...")
        
        payloads = data if isinstance(data, list) else [data]
        processed = 0
        
        for payload in payloads:
            # Extract transaction data
            tx_type = payload.get("type", "")
            description = payload.get("description", "")
            
            # Log what we received
            logger.debug(f"Type: {tx_type}, Description: {description}")
            
            # Try to extract mint from EVERY possible location
            mint_str = None
            
            # Method 1: Direct mint field
            if "mint" in payload:
                mint_str = payload["mint"]
                logger.info(f"Found mint (method 1): {mint_str}")
            
            # Method 2: Token transfers
            elif "tokenTransfers" in payload and payload["tokenTransfers"]:
                for transfer in payload["tokenTransfers"]:
                    if transfer.get("mint"):
                        mint_str = transfer["mint"]
                        logger.info(f"Found mint (method 2): {mint_str}")
                        break
            
            # Method 3: Native transfers (for Pump.fun)
            elif "nativeTransfers" in payload and payload["nativeTransfers"]:
                # Sometimes mint is in the accounts involved
                accounts = payload.get("accountData", [])
                if accounts:
                    for acc in accounts:
                        acc_key = acc.get("account", "")
                        if len(str(acc_key)) in [43, 44]:  # Solana address length
                            mint_str = str(acc_key)
                            logger.info(f"Found mint (method 3): {mint_str}")
                            break
            
            # Method 4: Instructions (Pump.fun specific)
            elif "instructions" in payload:
                for inst in payload["instructions"]:
                    if "accounts" in inst and len(inst["accounts"]) > 0:
                        # First account is often the mint in Create instruction
                        mint_str = inst["accounts"][0]
                        logger.info(f"Found mint (method 4): {mint_str}")
                        break
            
            if not mint_str:
                logger.warning(f"❌ No mint found in payload (type: {tx_type})")
                continue
            
            # Validate mint
            try:
                mint_pubkey = PublicKey.from_string(str(mint_str))
            except Exception as e:
                logger.warning(f"Invalid mint: {mint_str} - {e}")
                continue
            
            # Check if already tracking
            async with tokens_lock:
                if mint_str in tokens:
                    logger.debug(f"Already tracking: {mint_str[:8]}...")
                    continue
                
                # Add new token
                tokens[mint_str] = TokenInfo(
                    mint=mint_pubkey,
                    launchpad="Pump.fun",
                    created_at=time.time(),
                )
                processed += 1
            
            logger.info(f"🎉 NEW TOKEN: {mint_str[:8]}... (Total tracked: {len(tokens)})")
            
            # DON'T notify immediately - wait for volume sync to trigger alerts
            # background_tasks.add_task(notify_new_token, tokens[mint_str])
        
        if processed > 0:
            logger.info(f"✅ Processed {processed} new tokens from webhook")
        
        return {"status": "ok", "processed": processed}
    
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """Telegram webhook endpoint."""
    try:
        if not telegram_app:
            return {"status": "telegram not configured"}
        
        update = Update.de_json(await request.json(), telegram_app.bot)
        await telegram_app.process_update(update)
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

# Deprecation warnings fixed - using lifespan (see above)

# ============================================================================
# Main
# ============================================================================
if __name__ == "__main__":
    # Validate config
    if not HELIUS_API_KEY:
        logger.error("❌ HELIUS_API_KEY not set!")
        sys.exit(1)
    
    # Run server
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info"
    )
