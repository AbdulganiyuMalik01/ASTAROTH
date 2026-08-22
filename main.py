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
