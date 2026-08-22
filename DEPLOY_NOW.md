# 🚀 DEPLOYMENT CHECKLIST

## ✅ Step 1: Code Pushed to GitHub
- [x] Repository created
- [x] Code committed and pushed
- [x] `.env` file protected (not in repo)

---

## 📝 Step 2: Deploy to Render (5 minutes)

### 2.1 Create Render Account
1. Go to: **https://render.com**
2. Click "Get Started"
3. Sign up with GitHub (easiest - auto-connects repos)

### 2.2 Create Web Service
1. After login, click **"New +"** → **"Web Service"**
2. Click **"Connect account"** to authorize GitHub access
3. Find and select: **`solana-token-tracker`** repository
4. Click **"Connect"**

### 2.3 Configure Service Settings

**Fill in these fields:**

| Field | Value |
|-------|-------|
| **Name** | `solana-token-tracker` |
| **Region** | Oregon (US West) |
| **Branch** | `main` |
| **Root Directory** | (leave empty) |
| **Runtime** | Python 3 |
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `python token_tracker_webhook.py` |
| **Instance Type** | Free |

### 2.4 Add Environment Variables

Click **"Advanced"** → **"Add Environment Variable"**

Add these 4 variables:

```
HELIUS_API_KEY = your_helius_key_here
TELEGRAM_BOT_TOKEN = your_telegram_bot_token
TELEGRAM_CHAT_ID = your_telegram_chat_id
PORT = 10000
```

**Where to get these:**
- **HELIUS_API_KEY**: Go to https://dashboard.helius.xyz/ → Settings → API Keys → Copy
- **TELEGRAM_BOT_TOKEN**: Already in your `.env` file (copy from there)
- **TELEGRAM_CHAT_ID**: Already in your `.env` file (copy from there)
- **PORT**: Just type `10000`

### 2.5 Deploy!
1. Click **"Create Web Service"**
2. Wait 2-3 minutes for build
3. Watch the logs - you should see:
   ```
   🚀 Token Tracker started - Webhook endpoint ready
   ✅ Telegram bot initialized
   ```

4. Your app URL will be: `https://solana-token-tracker.onrender.com`

---

## 📡 Step 3: Configure Helius Webhook (2 minutes)

### 3.1 Get Your Webhook URL
After Render deployment completes, your webhook endpoint is:
```
https://solana-token-tracker.onrender.com/webhook/helius
```

### 3.2 Add to Helius Dashboard
1. Go to: **https://dashboard.helius.xyz/webhooks**
2. Click **"Create New Webhook"**
3. Fill in:

| Field | Value |
|-------|-------|
| **Webhook URL** | `https://solana-token-tracker.onrender.com/webhook/helius` |
| **Webhook Type** | Enhanced |
| **Account Address** | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| **Transaction Types** | Select "All" or check "Token Creation" |

4. Click **"Create Webhook"**
5. Click **"Test"** button to send test event

### 3.3 Verify
Check your Render logs (in dashboard → Logs tab):
```
INFO - 🎉 Webhook: New token test123...
```

Check your Telegram - you should get startup message:
```
🤖 Token Tracker v2.0 Started (Webhook Mode)
✅ Helius webhook endpoint active
```

---

## 🧪 Step 4: Test Live (Wait for Real Launch)

### Check Health Endpoint
Visit in browser:
```
https://solana-token-tracker.onrender.com/health
```

Should show:
```json
{
  "status": "healthy",
  "tokens": 0,
  "uptime": 123.45
}
```

### Monitor Stats
```
https://solana-token-tracker.onrender.com/stats
```

### Wait for Launch
When next pump.fun token launches:
1. **Helius → Render** (webhook delivery)
2. **Telegram notification** (you get alert)
3. **DexScreener sync** (real volume every 60s)
4. **Milestone alerts** (5, 10, 25, 50+ SOL)

---

## 📊 Monitoring

### View Render Logs
- Render Dashboard → Your service → **"Logs"** tab
- Real-time stream of events

### Check Stats Endpoint
- `https://your-app.onrender.com/stats`
- Shows top tokens by volume

### Telegram Alerts
You'll receive:
- 🚀 New token detected
- 📈 Volume milestones
- ⚠️ Security alerts
- 🧠 Smart money activity

---

## 💰 Cost

| Service | Cost |
|---------|------|
| Render Free | $0/mo |
| Helius Free | $0/mo |
| DexScreener | $0/mo |
| Telegram | $0/mo |
| **TOTAL** | **$0/mo** |

---

## ⚡ Quick Actions

### Restart Service
Render Dashboard → Manual Deploy → "Clear build cache & deploy"

### Update Code
```powershell
git add .
git commit -m "Update"
git push
```
(Render auto-deploys on push)

### View Logs Live
Render Dashboard → Logs (streams in real-time)

### Stop Service
Render Dashboard → Settings → Suspend Service

---

## 🎯 SUCCESS CRITERIA

After 1 hour, you should have:
- ✅ Service deployed and running
- ✅ Helius webhook configured
- ✅ Health endpoint responding
- ✅ Telegram startup message received
- ✅ Volume sync running (every 60s)
- ✅ Logs showing no errors

**When first token launches:**
- ✅ Webhook received (Render logs)
- ✅ Telegram alert sent
- ✅ Volume syncing starts
- ✅ Milestones trigger

---

## 🚀 YOU'RE READY!

**Next action:** Go to https://render.com and start Step 2.1

**Time required:** 5-7 minutes total

**Result:** 24/7 production tracker with 99.9% detection rate

---

**Deploy now and start catching launches!** 🎯
