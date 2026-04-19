"""
Centralized Configuration Management
Handles environment-based configuration with validation and defaults.
"""
import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class WebhookConfig:
    """Webhook-specific configuration."""
    secret_key: Optional[str] = None
    enable_signature_verification: bool = False
    max_payload_size: int = 1_000_000  # 1MB
    rate_limit_per_ip: int = 100  # requests per minute


@dataclass
class DetectionConfig:
    """Detection algorithm configuration."""
    # Smart volume thresholds
    # Raised defaults for strict smart-volume mode
    # Only high-signal tokens pass without env overrides
    min_buys_3min: int = 30
    min_unique_wallets: int = 15
    min_sol_volume: float = 20.0
    min_market_cap_usd: float = 35000

    # Feature flags
    enable_holder_analysis: bool = True
    enable_defi_alerts: bool = True
    enable_social_verification: bool = True

    # Holder analysis thresholds
    max_top10_holder_pct: float = 50.0  # Rug risk if exceeded
    min_holder_count: int = 10

    # Smart detection v4 parameters
    smart_score_threshold: float = 35.0
    smart_cooldown_seconds: int = 1800  # 30 minutes per-token cooldown
    smart_global_cap_per_min: int = 10  # max smart alerts per minute globally
    smart_window_seconds: int = 180     # evaluation window (3 minutes)
    smart_cluster_window_seconds: int = 10  # cluster buys within this window
    smart_mc_downweight_factor: float = 0.6  # score multiplier if MC below threshold

    # Established token detection (resurgence mode)
    min_token_age_seconds: int = 3600       # 1 hour minimum age (ignore brand new tokens)
    require_existing_mc: bool = True         # require DexScreener MC > 0 before alerting
    min_existing_mc_usd: float = 5000.0      # minimum MC to consider "established"
    resurgence_mode: bool = True             # enable resurgence detection mode


@dataclass
class APIConfig:
    """External API configuration."""
    helius_api_key: str
    helius_url: str = "https://api.helius.xyz/v0"
    rpc_https: str = ""
    rpc_wss: str = ""

    # Circuit breaker settings
    api_failure_threshold: int = 5
    api_recovery_timeout: int = 60  # seconds
    api_timeout: int = 5  # seconds

    # Retry settings
    max_retries: int = 3
    retry_backoff_base: float = 2.0

    def __post_init__(self):
        if not self.rpc_https:
            self.rpc_https = f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
        if not self.rpc_wss:
            self.rpc_wss = f"wss://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"


@dataclass
class TelegramConfig:
    """Telegram bot configuration."""
    bot_token: str
    chat_id: str
    parse_mode: str = "HTML"
    disable_web_preview: bool = True


@dataclass
class DatabaseConfig:
    """Database configuration."""
    sqlite_path: str = "tracker_data.db"
    postgres_url: Optional[str] = None
    use_postgres: bool = False

    def __post_init__(self):
        if self.postgres_url:
            self.use_postgres = True


@dataclass
class CacheConfig:
    """Caching configuration."""
    enable_caching: bool = True
    redis_url: Optional[str] = None
    dexscreener_ttl: int = 30  # seconds
    metadata_ttl: int = 300  # 5 minutes
    wallet_info_ttl: int = 600  # 10 minutes


@dataclass
class MonitoringConfig:
    """Monitoring and metrics configuration."""
    enable_metrics: bool = True
    metrics_port: int = 9090
    log_level: str = "INFO"
    structured_logging: bool = True


@dataclass
class Config:
    """Main configuration class."""
    webhook: WebhookConfig
    detection: DetectionConfig
    api: APIConfig
    telegram: TelegramConfig
    database: DatabaseConfig
    cache: CacheConfig
    monitoring: MonitoringConfig

    # Server settings
    port: int = 10000
    host: str = "0.0.0.0"

    # General settings
    monitor_hours_old: int = 3
    max_concurrent_requests: int = 10


def load_config() -> Config:
    """Load configuration from environment variables with validation."""

    # Required variables
    helius_api_key = os.getenv("HELIUS_API_KEY", "")
    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    if not helius_api_key:
        raise ValueError("HELIUS_API_KEY environment variable is required")
    if not telegram_bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is required")
    if not telegram_chat_id:
        raise ValueError("TELEGRAM_CHAT_ID environment variable is required")

    # Webhook config
    webhook = WebhookConfig(
        secret_key=os.getenv("WEBHOOK_SECRET_KEY"),
        enable_signature_verification=os.getenv("ENABLE_WEBHOOK_VERIFICATION", "false").lower() == "true",
        max_payload_size=int(os.getenv("MAX_PAYLOAD_SIZE", "1000000")),
        rate_limit_per_ip=int(os.getenv("RATE_LIMIT_PER_IP", "100")),
    )

    # Detection config
    detection = DetectionConfig(
        min_buys_3min=int(os.getenv("MIN_BUYS_3MIN", "30")),
        min_unique_wallets=int(os.getenv("MIN_UNIQUE_WALLETS", "15")),
        min_sol_volume=float(os.getenv("MIN_SOL_VOLUME", "20.0")),
        min_market_cap_usd=float(os.getenv("MIN_MARKET_CAP_USD", "35000")),
        enable_holder_analysis=os.getenv("ENABLE_HOLDER_ANALYSIS", "true").lower() == "true",
        enable_defi_alerts=os.getenv("ENABLE_DEFI_ALERTS", "true").lower() == "true",
        enable_social_verification=os.getenv("ENABLE_SOCIAL_VERIFICATION", "true").lower() == "true",
        max_top10_holder_pct=float(os.getenv("MAX_TOP10_HOLDER_PCT", "50.0")),
        min_holder_count=int(os.getenv("MIN_HOLDER_COUNT", "10")),
        smart_score_threshold=float(os.getenv("SMART_SCORE_THRESHOLD", "35.0")),
        smart_cooldown_seconds=int(os.getenv("SMART_ALERT_COOLDOWN_SECONDS", "1800")),
        smart_global_cap_per_min=int(os.getenv("SMART_GLOBAL_CAP_PER_MIN", "10")),
        smart_window_seconds=int(os.getenv("SMART_WINDOW_SECONDS", "180")),
        smart_cluster_window_seconds=int(os.getenv("SMART_CLUSTER_WINDOW_SECONDS", "10")),
        smart_mc_downweight_factor=float(os.getenv("SMART_MC_DOWNWEIGHT_FACTOR", "0.6")),
        # Resurgence mode settings
        min_token_age_seconds=int(os.getenv("MIN_TOKEN_AGE_SECONDS", "3600")),
        require_existing_mc=os.getenv("REQUIRE_EXISTING_MC", "true").lower() == "true",
        min_existing_mc_usd=float(os.getenv("MIN_EXISTING_MC_USD", "5000.0")),
        resurgence_mode=os.getenv("RESURGENCE_MODE", "true").lower() == "true",
    )

    # API config
    api = APIConfig(
        helius_api_key=helius_api_key,
        api_failure_threshold=int(os.getenv("API_FAILURE_THRESHOLD", "5")),
        api_recovery_timeout=int(os.getenv("API_RECOVERY_TIMEOUT", "60")),
        api_timeout=int(os.getenv("API_TIMEOUT", "5")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        retry_backoff_base=float(os.getenv("RETRY_BACKOFF_BASE", "2.0")),
    )

    # Telegram config
    telegram = TelegramConfig(
        bot_token=telegram_bot_token,
        chat_id=telegram_chat_id,
    )

    # Database config
    database = DatabaseConfig(
        sqlite_path=os.getenv("SQLITE_PATH", "tracker_data.db"),
        postgres_url=os.getenv("DATABASE_URL"),
    )

    # Cache config
    cache = CacheConfig(
        enable_caching=os.getenv("ENABLE_CACHING", "true").lower() == "true",
        redis_url=os.getenv("REDIS_URL"),
        dexscreener_ttl=int(os.getenv("DEXSCREENER_TTL", "30")),
        metadata_ttl=int(os.getenv("METADATA_TTL", "300")),
        wallet_info_ttl=int(os.getenv("WALLET_INFO_TTL", "600")),
    )

    # Monitoring config
    monitoring = MonitoringConfig(
        enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
        metrics_port=int(os.getenv("METRICS_PORT", "9090")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        structured_logging=os.getenv("STRUCTURED_LOGGING", "true").lower() == "true",
    )

    return Config(
        webhook=webhook,
        detection=detection,
        api=api,
        telegram=telegram,
        database=database,
        cache=cache,
        monitoring=monitoring,
        port=int(os.getenv("PORT", "10000")),
        host=os.getenv("HOST", "0.0.0.0"),
        monitor_hours_old=int(os.getenv("MONITOR_HOURS_OLD", "3")),
        max_concurrent_requests=int(os.getenv("MAX_CONCURRENT_REQUESTS", "10")),
    )


# Global config instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reload_config() -> Config:
    """Reload configuration from environment."""
    global _config
    _config = load_config()
    return _config
