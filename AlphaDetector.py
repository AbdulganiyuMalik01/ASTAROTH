#!/usr/bin/env python3
"""
AlphaDetector.py — DEPRECATED
==============================
This file uses the old Solana RPC WebSocket `account_subscribe` approach to
detect new token launches. That method subscribes to any account-state change
on the launchpad program, which does NOT reliably yield new mint events — it
fires on every state change (swaps, liquidity events, etc.), flooding the
handler with irrelevant data.

REPLACEMENT
-----------
Use `token_tracker_webhook_v3.py` + `alpha_engine.py` (v5 scoring) instead.

The v3/v5 stack replaces this file entirely:
  • Real-time launch detection via Helius enhanced-transaction webhooks
  • DexScreener polling for tokens that bypass Helius
  • Composite v5 scoring with hard gates + quality normalization
  • PumpSwap / Raydium / Meteora / Moonshot launchpad detection
  • Circuit breakers, deduplication, Telegram alerts

How to run the production system:
    python token_tracker_webhook.py

This file is kept for historical reference only. DO NOT use in production.
"""

import asyncio
import json
import base64
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional, List, Set

import aiohttp
import requests
from solana.rpc.async_api import AsyncClient
from solana.rpc.websocket_api import connect
from solana.publickey import PublicKey
from solana.transaction import Transaction
from solana.rpc.types import MemcmpOpts

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
RPC_HTTPS = "https://api.mainnet-beta.solana.com"   # or your private endpoint
RPC_WSS   = "wss://api.mainnet-beta.solana.com"
HELIUS_API_KEY = "YOUR_HELIUS_API_KEY_HERE"  # Required for smart/cabal detection
HELIUS_URL = f"https://api.helius.xyz/v0"

# Launchpads (expandable)
LAUNCHPADS = {
    "Pump.fun": PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
    "LetsBonk.fun": PublicKey("FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1"),
    "Moonshot": PublicKey("moonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG"),
}

# Discriminators (assumed shared; customize if needed)
DISCRIMINATOR = bytes([0xE2, 0xE9, 0x1B, 0xB6, 0xA3, 0xC4, 0xD5, 0xF6])  # create
SWAP_DISCRIMINATOR = base64.b64encode(bytes([0xD7, 0xD6, 0xB5, 0x94, 0xF2, 0xC1, 0xA0, 0x3F])).decode()

# Smart wallet criteria
SMART_AGE_DAYS = 30
SMART_BALANCE_SOL = 10
SMART_TX_COUNT = 100

# Cabal thresholds
CABAL_EARLY_BUYERS = 3  # In first 5 min
CABAL_SHARED_FUNDERS = 2  # Overlapping funders

# ----------------------------------------------------------------------
# Data structures
# ----------------------------------------------------------------------
@dataclass
class WalletInfo:
    age_days: float = 0.0
    balance_sol: float = 0.0
    tx_count: int = 0
    is_smart: bool = False

@dataclass
class TokenInfo:
    mint: PublicKey
    launchpad: str
    created_at: float
    volume_sol: float = 0.0
    smart_volume_sol: float = 0.0
    buyers: Dict[str, float] = None  # buyer_addr -> sol_spent
    smart_wallets: Set[str] = None
    cabal_score: int = 0  # 0-10

    def __post_init__(self):
        self.buyers = defaultdict(float)
        self.smart_wallets = set()

# Global state
tokens: Dict[str, TokenInfo] = {}
wallets_cache: Dict[str, WalletInfo] = {}  # Cache for efficiency
processed_sigs = set()

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def parse_create_account(data_b64: str) -> Optional[PublicKey]:
    try:
        data = base64.b64decode(data_b64)
        if len(data) < 40:
            return None
        return PublicKey(data[8:40])
    except Exception:
        return None

async def get_wallet_info(session: aiohttp.ClientSession, addr: str) -> WalletInfo:
    if addr in wallets_cache:
        return wallets_cache[addr]

    info = WalletInfo()
    try:
        # Balance
        bal_resp = requests.post(RPC_HTTPS, json={"jsonrpc": "2.0", "id": 1, "method": "getBalance", "params": [addr]})
        info.balance_sol = bal_resp.json()["result"]["value"] / 1e9

        # Tx count (approx via signatures)
        sigs_resp = requests.post(RPC_HTTPS, json={"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [addr, {"limit": 1}]})
        info.tx_count = len(sigs_resp.json().get("result", []))  # Rough; use Helius for accurate

        # Age: First tx timestamp via Helius
        url = f"{HELIUS_URL}/transactions?api-key={HELIUS_API_KEY}"
        params = {"accounts": [addr], "limit": 1, "sort": "asc"}
        async with session.get(url, params=params) as resp:
            txs = await resp.json()
            if txs:
                info.age_days = (time.time() - txs[0]["timestamp"]) / 86400

        info.is_smart = (
            info.age_days > SMART_AGE_DAYS and
            info.balance_sol > SMART_BALANCE_SOL and
            info.tx_count > SMART_TX_COUNT
        )
    except Exception as e:
        print(f"Wallet info error for {addr}: {e}")

    wallets_cache[addr] = info
    return info

async def detect_cabal(session: aiohttp.ClientSession, token: TokenInfo) -> int:
    score = 0
    early_buyers = [b for b, amt in token.buyers.items() if time.time() - token.created_at < 300]  # 5 min
    if len(early_buyers) > CABAL_EARLY_BUYERS:
        score += 4

    # Check shared funders (sample 3 early buyers)
    funders = set()
    for buyer in early_buyers[:3]:
        url = f"{HELIUS_URL}/transactions?api-key={HELIUS_API_KEY}"
        params = {"accounts": [buyer], "limit": 10, "sort": "desc"}
        async with session.get(url, params=params) as resp:
            txs = await resp.json()
            for tx in txs:
                if "transfer" in tx.get("description", "").lower():
                    funders.add(tx.get("source", ""))

    if len(funders) < CABAL_SHARED_FUNDERS:
        score += 3

    score += 3 if score > 0 else 0  # Bonus for clusters

    return min(score, 10)

def extract_mint_from_tx(tx_json) -> Optional[PublicKey]:
    instructions = tx_json.get("transaction", {}).get("message", {}).get("instructions", [])
    for ix in instructions:
        if "programId" in ix and ix["programId"] in [str(p) for p in LAUNCHPADS.values()]:
            inner = tx_json.get("meta", {}).get("innerInstructions", [])
            for inner_ix in inner:
                for iix in inner_ix.get("instructions", []):
                    if iix.get("programId") == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
                        parsed = iix.get("parsed", {})
                        if parsed.get("type") == "transfer":
                            info = parsed.get("info", {})
                            mint = info.get("mint")
                            if mint:
                                return PublicKey(mint)
    return None

# ----------------------------------------------------------------------
# NOTE: Everything below is deprecated. Use token_tracker_webhook_v3.py
# ----------------------------------------------------------------------

async def listen_to_launchpads():
    raise NotImplementedError(
        "AlphaDetector.py is deprecated. Use token_tracker_webhook_v3.py instead."
    )

async def poll_swaps(client: AsyncClient):
    raise NotImplementedError(
        "AlphaDetector.py is deprecated. Use token_tracker_webhook_v3.py instead."
    )

async def print_table():
    raise NotImplementedError(
        "AlphaDetector.py is deprecated. Use token_tracker_webhook_v3.py instead."
    )

async def main():
    raise NotImplementedError(
        "AlphaDetector.py is deprecated.\n"
        "Run the production system with: python token_tracker_webhook.py\n"
        "See MASTER_README.md for full documentation."
    )

if __name__ == "__main__":
    print("=" * 60)
    print("  AlphaDetector.py — DEPRECATED")
    print("=" * 60)
    print()
    print("  This file is no longer the production entry point.")
    print("  The RPC WebSocket approach it uses is unreliable for")
    print("  new token detection — account_subscribe fires on")
    print("  ALL program state changes, not just new mints.")
    print()
    print("  Run the production system instead:")
    print("    python token_tracker_webhook.py")
    print()
    print("  See MASTER_README.md for full documentation.")
    print("=" * 60)
