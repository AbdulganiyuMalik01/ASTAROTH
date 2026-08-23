"""
⚠️ NOT PART OF THE DEPLOYED SERVICE.

This is the entrypoint for an older, standalone Telegram bot prototype
(QuantAgentBot in agent/bot.py). Railway runs token_tracker_polling.py
directly (see Procfile / railway.toml / nixpacks.toml) — nothing invokes
this file in production, and it has not been deployed since the deploy
stack was rebuilt around token_tracker_polling.py.

It also duplicates KOL/influencer tracking: this stack uses a hardcoded
KOL_ACCOUNTS list (utils/kol_accounts.py) feeding SignalAggregator/
NarrativeDetector, completely separate from token_tracker_polling.py's
live kol_polling_loop() (Nitter/RapidAPI + kol_list.json). See
LEGACY_STANDALONE_BOT.md at the repo root for the full history and what
to do about it.
"""
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def main():
    from agent.bot import QuantAgentBot

    # Validate env
    required = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"❌ Missing required env vars: {', '.join(missing)}")
        logger.error("Copy .env.example to .env and fill in your values.")
        return

    bot = QuantAgentBot()
    bot.run()


if __name__ == "__main__":
    main()
