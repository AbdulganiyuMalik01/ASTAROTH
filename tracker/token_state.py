from dataclasses import dataclass, field
from typing import Dict, Set
from collections import deque
import time

# Minimal token state used by tracker adapters.
# This is intentionally lightweight and avoids heavy external imports.

@dataclass
class TokenInfo:
    mint: str
    launchpad: str = "Unknown"
    created_at: float = field(default_factory=lambda: time.time())
    volume_sol: float = 0.0
    smart_volume_sol: float = 0.0
    buyers: Dict[str, float] = field(default_factory=dict)
    smart_wallets: Set[str] = field(default_factory=set)
    cabal_score: int = 5
    name: str = "Unknown"
    symbol: str = "UNK"
    market_cap: float = 0.0


@dataclass
class WalletInfo:
    age_days: float = 0.0
    balance_sol: float = 0.0
    tx_count: int = 0
    is_smart: bool = False


# Global in-memory stores
tokens: Dict[str, TokenInfo] = {}
wallets_cache: Dict[str, WalletInfo] = {}
processed_sigs = deque(maxlen=10000)

# Basic thresholds exported for other modules
VOLUME_ALERT_THRESHOLD = 5
WALLET_CACHE_SIZE = 1000
