"""
Webhook Security Module
Handles HMAC signature verification, request validation, and rate limiting.
"""
import hmac
import hashlib
import time
import logging
from typing import Optional, Dict
from collections import defaultdict, deque
from dataclasses import dataclass
from fastapi import Request, HTTPException

logger = logging.getLogger(__name__)


@dataclass
class RateLimitInfo:
    """Rate limit tracking information."""
    requests: deque
    blocked_until: float = 0.0


class RateLimiter:
    """Simple in-memory rate limiter."""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.clients: Dict[str, RateLimitInfo] = defaultdict(
            lambda: RateLimitInfo(requests=deque())
        )
        self.block_duration = 300  # 5 minutes

    def is_allowed(self, client_ip: str) -> bool:
        now = time.time()
        info = self.clients[client_ip]
        if info.blocked_until > now:
            return False
        cutoff = now - self.window_seconds
        while info.requests and info.requests[0] < cutoff:
            info.requests.popleft()
        if len(info.requests) >= self.max_requests:
            info.blocked_until = now + self.block_duration
            logger.warning(f"Rate limit exceeded for {client_ip}. Blocked for {self.block_duration}s")
            return False
        info.requests.append(now)
        return True

    def get_remaining(self, client_ip: str) -> int:
        now = time.time()
        info = self.clients[client_ip]
        cutoff = now - self.window_seconds
        while info.requests and info.requests[0] < cutoff:
            info.requests.popleft()
        return max(0, self.max_requests - len(info.requests))


class WebhookValidator:
    """Validates webhook requests."""

    def __init__(self, secret_key: Optional[str] = None):
        self.secret_key = secret_key
        self.max_payload_size = 1_000_000  # 1MB

    def verify_signature(self, payload: bytes, signature: str) -> bool:
        if not self.secret_key:
            logger.warning("No secret key configured, skipping signature verification")
            return True
        try:
            expected_signature = hmac.new(
                self.secret_key.encode(),
                payload,
                hashlib.sha256
            ).hexdigest()
            return hmac.compare_digest(signature, expected_signature)
        except Exception as e:
            logger.error(f"Signature verification error: {e}")
            return False

    def validate_payload_size(self, payload: bytes) -> bool:
        return len(payload) <= self.max_payload_size

    def validate_payload_structure(self, data: dict) -> bool:
        if not isinstance(data, (dict, list)):
            return False
        if isinstance(data, list):
            return all(self.validate_payload_structure(item) for item in data)
        return True

    def sanitize_mint_address(self, mint: str) -> Optional[str]:
        if not mint or not isinstance(mint, str):
            return None
        mint = mint.strip()
        if len(mint) < 32 or len(mint) > 44:
            return None
        valid_chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        if not all(c in valid_chars for c in mint):
            return None
        return mint


async def verify_webhook_request(
    request: Request,
    validator: WebhookValidator,
    rate_limiter: Optional[RateLimiter] = None
) -> bytes:
    client_ip = request.client.host if request.client else "unknown"
    if rate_limiter and not rate_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Too many requests.")
    payload = await request.body()
    if not validator.validate_payload_size(payload):
        raise HTTPException(status_code=413, detail="Payload too large")
    if validator.secret_key:
        signature = request.headers.get("X-Webhook-Signature", "")
        if not signature:
            raise HTTPException(status_code=401, detail="Missing webhook signature")
        if not validator.verify_signature(payload, signature):
            logger.warning(f"Invalid webhook signature from {client_ip}")
            raise HTTPException(status_code=401, detail="Invalid webhook signature")
    return payload


def get_client_identifier(request: Request) -> str:
    ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("User-Agent", "unknown")
    return f"{ip}:{user_agent}"
