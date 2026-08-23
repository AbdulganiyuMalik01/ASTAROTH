# ⚠️ `main.py` / `agent/` / `signals/` / `tracker/` / `utils/` are NOT deployed

**TL;DR: the live bot is `token_tracker_polling.py`, started via the
Procfile/`railway.toml`/`nixpacks.toml` (`python token_tracker_polling.py`).
Everything under `main.py`, `agent/`, `signals/`, `tracker/`, `utils/` is a
separate, older, standalone Telegram bot prototype (`QuantAgentBot`) that
Railway never runs and that has been dead code in production since before
the deploy stack was rebuilt around `token_tracker_polling.py`.**

## Why this file exists

Commit `6db5e6c` ("restore docs/standalone bot, remove dead code + unsafe
deploy scripts") explains what happened: an earlier force-push had
accidentally wiped this repo's history down to a sparser working copy that
never had `main.py`/`agent/`/`signals/`/`utils/` in it. That commit restored
them from the repo's object store purely to avoid losing the files — it was
a data-recovery action, not a decision to run two bots. No deploy config was
ever pointed at `main.py`, so this stack has never actually run on Railway.

## The concrete duplication this caused

Both stacks independently implement KOL/influencer tracking:

- **Live path** (`token_tracker_polling.py`): `kol_polling_loop()` polls
  Nitter (falling back to a RapidAPI Twitter proxy) for a dynamically-managed
  account list persisted to `kol_list.json`, extracts `$TICKER` mentions, and
  cross-references them against currently tracked tokens.
- **Dead path** (`agent/bot.py` + `utils/kol_accounts.py`): a hardcoded
  `KOL_ACCOUNTS` seed list (Elon Musk, MrBeast, etc.) feeding a
  `SignalAggregator`/`NarrativeDetector` combo, exposed through its own
  Telegram command set (`/signals`, `/trends`, `/scan`, `/movers`, `/kols`,
  ...) via `python-telegram-bot`'s polling `Application` — a completely
  different bot framework usage than the live service's `telegram.Bot` +
  FastAPI setup.

Because the dead path never runs, there's no runtime conflict (no double
alerts, no double Telegram polling collision) — the duplication is a repo
clarity problem, not a live bug. The risk it creates is future: someone
extending KOL logic in the wrong stack, or assuming `/signals`-style commands
work against the live bot when they don't.

## What to do with it

Nothing has been deleted. Given the history above, deleting this code isn't
a call to make unilaterally — if you want it gone, delete `main.py`,
`agent/`, `signals/`, `tracker/`, `utils/`, and this file in one commit; if
you'd rather keep it as reference/a source of ideas for a future rewrite,
leave it as is now that it's clearly labeled.
