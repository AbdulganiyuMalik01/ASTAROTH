"""
Circuit Breaker Pattern Implementation
Provides resilience for external API calls with automatic recovery.

Improvement #7: All thresholds are now env-overridable so you can tune
failure tolerance without a redeploy.

Env vars:
  CB_FAILURE_THRESHOLD   (default 5)  — failures before opening
  CB_RECOVERY_TIMEOUT    (default 60) — seconds before half-open attempt
  CB_SUCCESS_THRESHOLD   (default 2)  — successes in half-open before closing
  CB_HALF_OPEN_MAX_CALLS (default 1)  — concurrent calls allowed in half-open
"""
import os
import time
import logging
from enum import Enum
from typing import Optional, Callable, Any, Dict
from dataclasses import dataclass, field
import asyncio

logger = logging.getLogger(__name__)

# ── Global env defaults ────────────────────────────────────────────────────────
_DEFAULT_FAILURE_THRESHOLD  = int(os.getenv("CB_FAILURE_THRESHOLD",  "5"))
_DEFAULT_RECOVERY_TIMEOUT   = int(os.getenv("CB_RECOVERY_TIMEOUT",   "60"))
_DEFAULT_SUCCESS_THRESHOLD  = int(os.getenv("CB_SUCCESS_THRESHOLD",  "2"))
_DEFAULT_HALF_OPEN_MAX      = int(os.getenv("CB_HALF_OPEN_MAX_CALLS","1"))


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED    = "closed"     # Normal operation
    OPEN      = "open"       # Failing — reject requests fast
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """
    Circuit breaker configuration.

    All fields default to the process-level env vars so you can override a
    single instance without touching others:

        CircuitBreakerConfig(failure_threshold=3)  # only this one is stricter
    """
    failure_threshold:  int = field(default_factory=lambda: _DEFAULT_FAILURE_THRESHOLD)
    recovery_timeout:   int = field(default_factory=lambda: _DEFAULT_RECOVERY_TIMEOUT)
    success_threshold:  int = field(default_factory=lambda: _DEFAULT_SUCCESS_THRESHOLD)
    half_open_max_calls:int = field(default_factory=lambda: _DEFAULT_HALF_OPEN_MAX)


class CircuitBreaker:
    """
    Circuit breaker for external API calls.
    Prevents cascading failures by failing fast when a service is down.

    States:
      CLOSED    → normal operation
      OPEN      → service is down; calls rejected immediately
      HALF_OPEN → probing whether service recovered; limited calls allowed
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name   = name
        self.config = config or CircuitBreakerConfig()

        self.state             = CircuitState.CLOSED
        self.failure_count     = 0
        self.success_count     = 0
        self.half_open_calls   = 0   # concurrent calls in-flight during HALF_OPEN
        self.last_failure_time: Optional[float] = None
        self.state_changed_at  = time.time()
        self._lock             = asyncio.Lock()

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _should_attempt_reset(self) -> bool:
        if self.state != CircuitState.OPEN:
            return False
        if self.last_failure_time is None:
            return True
        return (time.time() - self.last_failure_time) >= self.config.recovery_timeout

    def _on_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self._close_circuit()
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0

    def _on_failure(self):
        self.last_failure_time = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self._open_circuit()
        elif self.state == CircuitState.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.config.failure_threshold:
                self._open_circuit()

    def _open_circuit(self):
        if self.state != CircuitState.OPEN:
            logger.warning(
                f"[CB] '{self.name}' OPENED after {self.failure_count} failures "
                f"(threshold={self.config.failure_threshold})"
            )
            self.state            = CircuitState.OPEN
            self.state_changed_at = time.time()
            self.failure_count    = 0
            self.success_count    = 0
            self.half_open_calls  = 0

    def _close_circuit(self):
        if self.state != CircuitState.CLOSED:
            logger.info(f"[CB] '{self.name}' CLOSED (recovered)")
            self.state            = CircuitState.CLOSED
            self.state_changed_at = time.time()
            self.failure_count    = 0
            self.success_count    = 0
            self.half_open_calls  = 0

    def _attempt_reset(self):
        logger.info(f"[CB] '{self.name}' → HALF_OPEN (probing recovery)")
        self.state            = CircuitState.HALF_OPEN
        self.state_changed_at = time.time()
        self.success_count    = 0
        self.half_open_calls  = 0

    # ── Public API ─────────────────────────────────────────────────────────────

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute *func* with circuit-breaker protection.

        Raises:
            CircuitBreakerOpenError  — if circuit is open (fail fast)
            Exception                — original exception from *func*
        """
        async with self._lock:
            if self._should_attempt_reset():
                self._attempt_reset()

            if self.state == CircuitState.OPEN:
                raise CircuitBreakerOpenError(
                    f"[CB] '{self.name}' is OPEN. "
                    f"Retry in ~{self.config.recovery_timeout}s."
                )

            # In HALF_OPEN, limit concurrent probe calls
            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.config.half_open_max_calls:
                    raise CircuitBreakerOpenError(
                        f"[CB] '{self.name}' HALF_OPEN: max probe calls in-flight."
                    )
                self.half_open_calls += 1

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            async with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.half_open_calls = max(0, self.half_open_calls - 1)
                self._on_success()

            return result

        except Exception:
            async with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.half_open_calls = max(0, self.half_open_calls - 1)
                self._on_failure()
            raise

    def get_state(self) -> Dict[str, Any]:
        """Snapshot of current breaker state (safe to serialize to JSON)."""
        return {
            "name":              self.name,
            "state":             self.state.value,
            "failure_count":     self.failure_count,
            "success_count":     self.success_count,
            "half_open_calls":   self.half_open_calls,
            "last_failure_time": self.last_failure_time,
            "state_changed_at":  self.state_changed_at,
            "uptime_seconds":    round(time.time() - self.state_changed_at, 1),
            "config": {
                "failure_threshold":   self.config.failure_threshold,
                "recovery_timeout":    self.config.recovery_timeout,
                "success_threshold":   self.config.success_threshold,
                "half_open_max_calls": self.config.half_open_max_calls,
            },
        }


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is open (fail-fast path)."""
    pass


class CircuitBreakerRegistry:
    """Thread-safe registry for multiple named circuit breakers."""

    def __init__(self):
        self.breakers: Dict[str, CircuitBreaker] = {}

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> CircuitBreaker:
        """Return an existing breaker or create a new one."""
        if name not in self.breakers:
            self.breakers[name] = CircuitBreaker(name, config)
        return self.breakers[name]

    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        return {name: b.get_state() for name, b in self.breakers.items()}

    def reset_all(self):
        """Force-close all breakers (use in tests or after manual recovery)."""
        for b in self.breakers.values():
            b._close_circuit()

    def reset(self, name: str):
        """Force-close a single named breaker."""
        if name in self.breakers:
            self.breakers[name]._close_circuit()


# ── Module-level singleton ─────────────────────────────────────────────────────
_registry = CircuitBreakerRegistry()


def get_circuit_breaker(
    name: str,
    config: Optional[CircuitBreakerConfig] = None,
) -> CircuitBreaker:
    """Get or create a named circuit breaker from the global registry."""
    return _registry.get_or_create(name, config)


def get_all_circuit_states() -> Dict[str, Dict[str, Any]]:
    """Get a snapshot of all registered circuit breakers."""
    return _registry.get_all_states()


def reset_circuit_breaker(name: str):
    """Force-close a single circuit breaker by name."""
    _registry.reset(name)
