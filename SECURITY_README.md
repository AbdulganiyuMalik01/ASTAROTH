# Solana Token Tracker - Security & Setup Guide

## 🚨 CRITICAL SECURITY ALERT

**Your API keys were exposed in the code!** Take immediate action:

### 1. Revoke Exposed Credentials

#### Helius API Key
1. Go to https://dashboard.helius.xyz/settings/api-keys
2. Find key: `843f36f3-65a5-4c7f-b95f-f51d75b15ca4`
3. Click "Revoke" or "Delete"
4. Create a new API key

#### Telegram Bot Token
1. Open Telegram and message @BotFather
2. Send `/mybots`
3. Select your bot
4. Click "Revoke Token"
5. Create a new bot or get a new token

### 2. Setup Environment Variables

```bash
# Copy example file
cp .env.example .env

# Edit .env with your NEW credentials
# Never commit .env to git!
```

### 3. Install Dependencies

```bash
pip install python-dotenv
```

## 📋 What Was Fixed

### Critical Fixes:
✅ **Security**: Moved API keys to `.env` file
✅ **Memory Leak**: Changed `processed_sigs` from set to deque with 10K limit
✅ **Blocking I/O**: Replaced `requests` with `aiohttp` in `get_wallet_info()`
✅ **Race Conditions**: Added `asyncio.Lock()` for shared state
✅ **Rate Limiting**: Added semaphore for max 10 concurrent requests
✅ **Cabal Logic**: Fixed inverted condition (was checking `< 2`, now checks ratio)
✅ **Launchpads**: Removed dead Moonshot program (merged with Pump.fun)
✅ **Volume Spike**: Fixed timing logic (was impossible to trigger)
✅ **Smart Wallet**: Made criteria more flexible (OR vs AND)
✅ **Mint Extraction**: Improved for pump.fun compatibility
✅ **Logging**: Replaced print() with proper logging

### Architecture Improvements:
- Added LRU cache management for wallets (max 1000 entries)
- Better error handling with logging levels
- Configuration validation on startup
- Proper async/await patterns throughout

## 🔧 Known Limitations

### Still Need Manual Fixing:

1. **WebSocket Launch Detection** - Currently broken for pump.fun
   - Subscribes to program account (won't catch new mints)
   - Need to use Helius webhooks or logs_subscribe()

2. **Volume Calculation** - Inaccurate
   - Only tracks buyer SOL spent, not actual DEX volume
   - Recommend: Poll DexScreener API every 30s

3. **Smart Money Detection** - Basic criteria only
   - Doesn't check win rate or known wallet clusters
   - Consider: Integrate with GMGN.ai or Rugcheck

4. **Cabal Detection** - Still limited
   - Doesn't analyze Jito bundles properly
   - Missing timing cluster analysis (<500ms)

## 🚀 Recommended Next Steps

### 1. Switch to Helius Webhooks (More Reliable)
```python
# Instead of WebSocket subscription, use:
# https://docs.helius.dev/webhooks-and-websockets/webhooks
```

### 2. Add DexScreener Volume
```python
async def get_real_volume(mint: str) -> float:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    async with session.get(url) as r:
        data = await r.json()
        if data.get("pairs"):
            return float(data["pairs"][0]["volume"]["h24"])
```

### 3. Run the Fixed Version
```bash
python token_tracker.py
```

## 📊 Performance Improvements

- **Before**: Blocking I/O, unlimited memory, no rate limits
- **After**: Full async, 10K sig limit, 10 concurrent requests max

## ⚠️ Important Notes

- The `.env` file is ignored by git (see `.gitignore`)
- Never share your `.env` file or commit it to version control
- Rotate your API keys every 3-6 months
- Monitor your Helius credit usage

## 🛠️ Troubleshooting

**Issue**: `ModuleNotFoundError: No module named 'dotenv'`
```bash
pip install python-dotenv
```

**Issue**: `ValueError: HELIUS_API_KEY not set`
- Make sure `.env` file exists in project root
- Check that `HELIUS_API_KEY=your_key` is set (no quotes)

**Issue**: No Telegram notifications
- Verify `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env`
- Check bot has permission to message you
