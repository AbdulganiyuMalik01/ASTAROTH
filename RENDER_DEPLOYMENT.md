# 🚀 Deploy to Render.com - Quick Guide

## Overview

This deploys your token tracker to Render.com with:
- ✅ Automatic HTTPS webhook endpoint
- ✅ 24/7 uptime (free tier)
- ✅ No credit card required for free tier
- ✅ Auto-restarts on crashes
- ✅ Environment variable management

---

## Step 1: Prepare Your Repository

### 1.1 Initialize Git (if not already)
```powershell
git init
git add .
git commit -m "Initial commit - Token Tracker v2.0"
```

### 1.2 Push to GitHub
```powershell
# Create repo at https://github.com/new
# Then:
git remote add origin https://github.com/YOUR_USERNAME/solana-tracker.git
git branch -M main
git push -u origin main
```

**Important:** Make sure `.env` is in `.gitignore` (already configured)

---

## Step 2: Create Render Account

1. Go to https://render.com/
2. Sign up with GitHub (easiest)
3. Authorize Render to access your repos

---

## Step 3: Deploy Web Service

### 3.1 Create New Web Service
1. Click "New +" → "Web Service"
2. Connect your GitHub repository
3. Select the `solana-tracker` repo

### 3.2 Configure Service

**Basic Settings:**
- **Name:** `solana-token-tracker` (or your choice)
- **Region:** Oregon (US West) - lowest latency for Solana
- **Branch:** `main`
- **Root Directory:** Leave empty
- **Runtime:** Python 3
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `python token_tracker_webhook.py`

**Instance Type:**
- Select: **Free** (512MB RAM, shared CPU)
- Good for: ~1000 tokens/day monitoring
- Upgrade to $7/mo "Starter" if you need more

### 3.3 Add Environment Variables

Click "Environment" → "Add Environment Variable"

Add these:
```
HELIUS_API_KEY=your_helius_api_key_here
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_telegram_chat_id_here
PORT=10000
```

**To get these:**
- **HELIUS_API_KEY:** https://dashboard.helius.xyz/
- **TELEGRAM_BOT_TOKEN:** Message @BotFather on Telegram → /newbot
- **TELEGRAM_CHAT_ID:** Message @userinfobot on Telegram
- **PORT:** Leave as 10000 (Render default)

### 3.4 Deploy
1. Click "Create Web Service"
2. Wait 2-3 minutes for deployment
3. You'll get a URL like: `https://solana-token-tracker.onrender.com`

---

## Step 4: Configure Helius Webhook

### 4.1 Get Your Webhook URL
After deployment, your webhook endpoint will be:
```
https://YOUR_APP_NAME.onrender.com/webhook/helius
```

### 4.2 Add to Helius Dashboard
1. Go to https://dashboard.helius.xyz/webhooks
2. Click "Create New Webhook"
3. **Webhook URL:** Paste your Render URL + `/webhook/helius`
4. **Webhook Type:** Enhanced or Raw
5. **Account Addresses:** 
   ```
   6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
   ```
   (This is Pump.fun program address)
6. **Transaction Types:** Select "All" or "Token Creation"
7. Click "Create"

### 4.3 Test Webhook
Helius provides a "Test" button. Click it to send test payload.

Check your Render logs (click "Logs" tab) - you should see:
```
INFO - 🎉 Webhook: New token abcd1234...
```

---

## Step 5: Verify Deployment

### 5.1 Check Health Endpoint
Visit in browser:
```
https://YOUR_APP.onrender.com/health
```

Should return:
```json
{
  "status": "healthy",
  "tokens": 0,
  "uptime": 123.45,
  "timestamp": 1700000000
}
```

### 5.2 Check Telegram
You should receive startup message:
```
🤖 Token Tracker v2.0 Started (Webhook Mode)

✅ Helius webhook endpoint active
✅ Real volume from DexScreener
✅ Smart wallet detection enabled
✅ Deployed on Render

Tracking: Pump.fun, Raydium, Orca, Meteora
```

### 5.3 Monitor Logs
In Render dashboard:
1. Click on your service
2. Click "Logs" tab
3. Should see:
   ```
   INFO - 🚀 Token Tracker started - Webhook endpoint ready
   INFO - ✅ Telegram bot initialized
   INFO - 🔄 Syncing volumes for 0 tokens...
   ```

---

## Step 6: Test with Real Launch

### Wait for New Pump.fun Launch
When a new token launches on pump.fun:

1. **Helius sends webhook** → Your Render app
2. **Render app processes** → Adds token to tracking
3. **Telegram notification** → You get alert
4. **DexScreener sync** → Real volume every 60s

**Check Render logs:**
```
INFO - 🎉 Webhook: New token Ekp1qT7E...
INFO - 📊 Ekp1qT7E: 0.0 → 12.3 SOL
```

**Check Telegram:**
```
🚀 NEW TOKEN DETECTED

🏷 Mint: Ekp1qT7E...
🎯 Launchpad: Pump.fun

🔗 Links:
• DexScreener
• Pump.fun
• Solscan
```

---

## Monitoring & Maintenance

### Check Stats Endpoint
```
https://YOUR_APP.onrender.com/stats
```

Returns:
```json
{
  "total_tokens": 42,
  "high_volume_tokens": 8,
  "top_3": [
    {
      "mint": "Ekp1qT7E...",
      "launchpad": "Pump.fun",
      "volume": 123.45,
      "smart_wallets": 5,
      "cabal_score": 3,
      "age_minutes": 15.2
    }
  ]
}
```

### View Logs
In Render dashboard → Logs tab → Real-time logs

### Restart Service
If needed: Render dashboard → "Manual Deploy" → "Clear build cache & deploy"

---

## Free Tier Limits

**Render Free Tier:**
- 750 hours/month (enough for 24/7 single service)
- 512MB RAM
- Shared CPU
- Auto-sleeps after 15 min inactivity (webhook wakes it)
- HTTPS included

**Helius Free Tier:**
- 100,000 webhook events/month
- ~3,000 launches/day = 90K/month (within limit)

**DexScreener:**
- 300 requests/minute
- Unlimited (no API key needed)

**Telegram:**
- Unlimited messages (rate limit: 30/sec)

**Cost: $0/month** ✅

---

## Upgrading to Paid (Optional)

### When to Upgrade Render:

**Stick with Free if:**
- ✅ <1000 launches/day
- ✅ Don't mind 15s wake-up delay
- ✅ Personal use only

**Upgrade to Starter ($7/mo) if:**
- ✅ Need instant response (no sleep)
- ✅ >1000 launches/day
- ✅ Multiple users
- ✅ Want 1GB RAM

### How to Upgrade:
1. Render dashboard → Your service
2. "Instance Type" → Change to "Starter"
3. Add payment method
4. Done

---

## Troubleshooting

### Issue: "Application failed to start"
**Solution:** Check Render logs for Python errors. Usually:
- Missing environment variable
- Typo in `requirements.txt`
- Port conflict (should be 10000)

### Issue: "No tokens detected"
**Solutions:**
1. Check Helius webhook is configured correctly
2. Verify program address: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`
3. Test webhook from Helius dashboard
4. Check Render logs for incoming requests

### Issue: "Telegram not sending"
**Solutions:**
1. Verify `TELEGRAM_BOT_TOKEN` in environment
2. Verify `TELEGRAM_CHAT_ID` (get from @userinfobot)
3. Send `/start` to your bot on Telegram
4. Check bot has permission to send messages

### Issue: "Out of memory"
**Solution:** Upgrade to Starter ($7/mo) for 1GB RAM

### Issue: "Service sleeping"
**Solution:** 
- Free tier sleeps after 15 min inactivity
- Wakes on incoming webhook (15s delay)
- Upgrade to Starter for always-on

---

## Architecture on Render

```
Helius Webhook → Render HTTPS Endpoint
                       ↓
            Token Tracker (Python)
                ↙      ↓      ↘
        Telegram   DexScreener   Solana RPC
        Alerts     Volume Sync   Security Check
```

**Flow:**
1. New token on pump.fun
2. Helius detects → sends webhook
3. Render app receives → processes
4. Telegram alert sent
5. DexScreener polls every 60s
6. Volume milestones trigger alerts

---

## Next Steps After Deployment

### Week 1:
- ✅ Monitor logs daily
- ✅ Verify all launches detected
- ✅ Check Telegram alerts arriving
- ✅ Compare volume to DexScreener website

### Week 2:
- ✅ Tune notification thresholds
- ✅ Adjust volume milestones
- ✅ Fine-tune smart wallet criteria

### Week 3:
- ✅ Consider upgrading to paid tier
- ✅ Add more launchpad webhooks
- ✅ Implement additional features

---

## Cost Summary

| Service | Free Tier | Paid Option | Recommendation |
|---------|-----------|-------------|----------------|
| Render | 750 hrs/mo | $7/mo (Starter) | Start free |
| Helius | 100K webhooks/mo | $100/mo (5M) | Free is enough |
| DexScreener | Unlimited | N/A | Always free |
| Telegram | Unlimited | N/A | Always free |
| **Total** | **$0/mo** | $7-100/mo | **Free works great** |

---

## Security Checklist

- [x] `.env` in `.gitignore` (keys not in repo)
- [x] Environment variables in Render (not hardcoded)
- [x] HTTPS endpoint (Render provides)
- [x] No API keys in logs
- [x] Telegram bot restricted to your chat ID

**Your deployment is secure.** ✅

---

## Success Metrics

**After 24 hours, you should see:**
- ✅ 100+ tokens detected
- ✅ Volume syncs every 60s
- ✅ Telegram alerts for new launches
- ✅ Milestone notifications (5, 10, 25+ SOL)
- ✅ No crashes or errors in logs

**If all above = SUCCESS** 🎯

---

## Support

### Render Issues:
- Docs: https://render.com/docs
- Community: https://community.render.com/

### Helius Issues:
- Docs: https://docs.helius.dev/
- Discord: https://discord.gg/helius

### App Issues:
- Check Render logs first
- Verify environment variables
- Test webhook from Helius dashboard

---

**You're now live with a production-grade webhook-based tracker.** 🚀

**Webhook URL:** `https://YOUR_APP.onrender.com/webhook/helius`

**Next:** Configure Helius webhook and watch the launches roll in!
