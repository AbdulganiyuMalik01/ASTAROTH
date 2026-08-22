# AlphaDegen — Full System Reference

Solana memecoin tracker + quant signal intelligence layer.  
Built across 4 generations of iteration. All files documented below.

---

## Codebase Map

```
project root/
├── token_tracker_webhook.py          ← Render entrypoint (start command)
├── token_tracker_webhook_v3.py       ← LIVE core tracker (FastAPI app)
├── webhook_security.py               ← HMAC validation + rate limiter
├── database.py                       ← SQLite: signatures, alerts, history
├── config.py                         ← Centralised config (loads .env)
├── circuit_breaker.py                ← Prevents cascade failures on API errors
├── holder_analysis.py                ← Helius: top-10 holder concentration
├── defi_detector.py                  ← Detects DeFi protocol deployments
├── metrics.py                        ← Prometheus-style internal metrics
├── requirements.txt                  ← All deps (v3 + quant agent)
├── .env                              ← Your secrets (never commit)
│
└── quant_agent/
    ├── utils/
    │   ├── kol_accounts.py           ← 388 KOLs across 16 categories, weights 1-10
    │   └── keywords.py               ← Animal→ticker map, hashtags, RSS feeds, scorer
    ├── signals/
    │   ├── monitor.py                ← SignalAggregator: Nitter + News + Trends
    │   ├── narrative.py              ← NarrativeDetector: 11 templates, ticker prediction
    │   └── quant_integration.py      ← THE BRIDGE: connects quant agent into v3
    ├── tracker/
    │   └── pumpfun.py                ← DexScreener search, risk scoring, watchlist
    └── agent/
        └── bot.py                    ← Standalone Telegram bot (optional)
```

---

## File Lineage (What Was Built and When)

| File | Generation | Key Capability Added |
|---|---|---|
| `token_tracker.py` | v1 — Origin | WebSocket launch detection, wallet analysis, cabal scoring |
| `volume_spike_tracker_v2_1.py` | v2 — Alpha | DexScreener real volume, numpy trend detection, smart money |
| `token_tracker_webhook_OLD.py` | v2.5 | FastAPI + Helius webhook, first database integration |
| `token_tracker_webhook_v3.py` | v3 — Production | Smart volume algorithm, heavy volume scoring, queue workers, circuit breakers, multi-chain |
| `quant_agent/` | v4 — Intelligence | 388 KOLs, narrative detection, ticker prediction, DexScreener matching |

---

## How to Deploy (Render)

**Start Command:**
```
python token_tracker_webhook.py
```

**Required env vars:**
```
HELIUS_API_KEY=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
PORT=10000
```

**Optional env vars:**
```
# Quant Agent
QUANT_AGENT_ENABLED=1
QUANT_SIGNAL_INTERVAL=300
QUANT_MIN_SIGNAL_SCORE=7
QUANT_AUTO_INJECT_TOKENS=1
QUANT_MIN_INJECT_LIQ=5000

# Tuning
SMART_SCORE_THRESHOLD=25
HEAVY_VOLUME_SCORE_THRESHOLD=7.0
DISABLE_BASELINE_ALERTS=1
```

---

## Integrating the Quant Agent into v3

Add two things to `token_tracker_webhook_v3.py`:

**At the top, with other imports:**
```python
from quant_agent.signals.quant_integration import start_quant_agent
```

**At the bottom of the `lifespan()` startup block:**
```python
asyncio.create_task(start_quant_agent(
    tokens,
    tokens_lock,
    send_telegram_message,
    TokenInfo_cls=TokenInfo,
))
```

That's it. The quant agent runs as a parallel background task. It does not touch any existing v3 logic.

---

## How v3 Works (Core Loop)

```
Helius/QuickNode Webhook POST
        ↓
  Gzip decompress
  Rate limit check
  Signature dedup (memory + SQLite)
        ↓
  process_queue.put_nowait()   ← returns 200 immediately
        ↓
  [Queue Worker x3] picks up payload
  ├── DeFi detection (optional)
  ├── Extract mint address
  ├── Filter system mints (WSOL, USDC...)
  ├── TokenInfo created / BuyEvent appended
  └── process_token_data()
          ↓
      DexScreener API → volume + MC
      check_smart_volume()
          ↓
      Score ≥ threshold → notify_smart_token() → Telegram
```

**Background tasks running in parallel:**
- `sync_volumes_and_detect()` — re-checks DexScreener every 30s for all tracked tokens
- `check_heavy_volume()` — velocity + acceleration + peer-rank scoring every 60s
- `cleanup_old_tokens()` — removes tokens older than `MONITOR_HOURS_OLD`
- `telegram_sender_worker()` — rate-limited queue (1.2s spacing, flood-wait handling)
- `start_quant_agent()` — signal collection + narrative detection every 5 min *(new)*

---

## Smart Volume Algorithm (v4 composite score)

```
score =
  buy_count        × 0.8
  unique_wallets   × 1.4
  total_sol        × 0.6
  time_density     × 1.0   ← decay weighting, recent buys worth more
  cluster_score            ← bursts within 10s windows
  momentum_score           ← buys per minute
  quality_score            ← repeat buyers (2+ buys same wallet)

if market_cap < MIN_MARKET_CAP_USD:
    score *= SMART_MC_DOWNWEIGHT_FACTOR   ← penalise micro-cap noise

Anti-spam:
  - Global cap: 5 alerts/min max
  - Per-token cooldown: 30 min
```

---

## Heavy Volume Algorithm

```
velocity  = SOL gained / hour (from velocity_history snapshots)
accel     = % change in velocity vs prior snapshot
rank      = this token's volume rank vs all active tokens (0-1)

score = (velocity_factor × 0.6) + (accel_factor × 0.3) + (rank × 0.1)

Alert fires at score ≥ 7.0, one-shot per token.
```

---

## Quant Agent Signal Flow

```
Every 5 minutes:

1. NitterMonitor
   ├── 32 priority KOLs (weight 9-10) polled every cycle
   └── 356 remaining KOLs rotated in batches of 20

2. NewsMonitor
   └── 15 RSS feeds: CoinTelegraph, Decrypt, BBC Wildlife, WWF, The Dodo...

3. GoogleTrendsMonitor
   └── Top 20 US real-time trends

4. NarrativeDetector runs on every signal
   └── 11 narrative templates: animal_viral, political, sports, tech_ai,
       celebrity, meme, space, conservation, crypto_narrative, geopolitical, food_viral

5. Signals scoring ≥ QUANT_MIN_SIGNAL_SCORE (default 7):
   └── DexScreener search for matching Solana tokens
   └── Risk score calculated (liquidity + volume + MC)
   └── Tokens with liq ≥ $5000 injected into v3's tokens{} dict
   └── v3's smart volume engine immediately starts monitoring them
   └── Alert sent via v3's Telegram queue
```

---

## Alert Types You'll Receive

| Alert | Trigger | Source |
|---|---|---|
| 🚨 Smart Volume | Buy clustering score threshold | v3 |
| 🌋 Heavy Volume | Velocity + acceleration + peer rank | v3 |
| ⚡ Quant Signal | KOL or news hit score ≥ 7 | Quant Agent |
| 🧠 Narrative | 3+ sources confirm same narrative | Quant Agent |
| 📡 Trend Digest | Every 15 min summary | Quant Agent |
| ⚠️ Burst Alert | Webhook rate > 10/sec | v3 |

---

## KOL Categories (388 accounts)

| Category | Count | Purpose |
|---|---|---|
| Mainstream Mega | 29 | Elon, Kanye, MrBeast — single tweet = memecoin |
| Crypto KOL | 50 | AnsemSol, Murad, blknoiz06, ZachXBT — direct alpha |
| Solana Ecosystem | 26 | toly, rajgokal, mert — on-chain Solana signal |
| Memecoin Specialist | 22 | PumpFunAlpha, degenalpha — live callers |
| DeFi / On-Chain | 25 | lookonchain, WhaleAlert — whale movement |
| VC / Founders | 25 | VitalikButerin, saylor, cz_binance — market-moving |
| Wildlife / Animals | 32 | WWF, BBCEarth, BronxZoo — $PNUT/$MOODENG source |
| Meme / Viral | 23 | daquan, PopCrave — memes become coins |
| Politics | 23 | Trump, AOC, Lummis — political events = narratives |
| News / Media | 21 | CoinDesk, Cointelegraph, WuBlockchain |
| Ecosystem | 20 | pumpdotfun, JupiterExchange, DexScreener |
| Alpha Channels | 25 | GemHunterSOL, OnChainAlerts |
| Tech / AI | 19 | OpenAI, sama, SpaceX, XAI |
| Global KOL | 20 | coin_bureau, altcoindaily |
| Sports / Entertainment | 28 | UFC, KSI, Logan Paul, Snoop |

---

## Webhook Endpoints

| Path | Source | What It Does |
|---|---|---|
| `POST /webhook/helius` | Helius | Primary Solana token event stream |
| `POST /webhook/quicknode` | QuickNode | Alternative with Pump.fun filter |
| `POST /webhook/ethereum` | Any | ETH token events |
| `POST /webhook/bsc` | Any | BNB Chain token events |
| `POST /webhook/telegram` | Telegram | Bot command handler |
| `GET /health` | Browser | Service health + circuit breaker states |
| `GET /stats` | Browser | Token counts + detection criteria |
| `GET /metrics` | Browser | Prometheus-style metrics |
| `GET /config` | Browser | Runtime detection configuration |
| `GET /debug/tokens` | Browser | Full smart volume breakdown per token |
| `GET /debug/webhook-stats` | Browser | Dedup stats, burst rate |
| `POST /simulate/buys` | Dev | Test smart volume alerts without real data |
| `POST /test-alert` | Dev | Fire a test Telegram message |

---

## Next Steps

1. **Add quant_agent/ to your GitHub repo** and push
2. **Add to requirements.txt**: `feedparser>=6.0.10`, `pytrends>=4.9.2`, `beautifulsoup4>=4.12.3`
3. **Add two lines** to v3 lifespan() (see Integration section above)
4. **Add 6 env vars** to Render: `QUANT_AGENT_ENABLED=1`, etc.
5. **Deploy** — watch first signal cycle fire in Render logs within 5 minutes

**After that:**
- Jupiter API execution layer (auto-buy when narrative fires)
- Twitter API v2 ($100/mo) for more reliable KOL polling
- Community Alpha League Bot (gamified leaderboard, monetizable)
