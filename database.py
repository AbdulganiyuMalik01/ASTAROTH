"""
[v4.31] SQLite alert-history log for ASTAROTH.

This is deliberately additive, not a replacement for the existing JSON
state-file persistence (astaroth_state.json) — that mechanism already works
and stays exactly as-is. What it doesn't give you is queryable history: it
only ever holds the *current* live-tracking snapshot, and the in-memory
near-miss ring (_analysis_ring, in token_tracker_polling.py) is capped and
resets on every restart.

This module logs one row per alert actually fired — a low, already-rate-
limited write frequency (ALERT_RATE_LIMIT + GEM_COOLDOWN already cap this to
at most a couple of writes per minute), so it can't become a performance
problem. It answers questions the live dict/JSON snapshot can't: how many
gems has chain X produced this week, what's the historical hit rate per
alert path (GEM vs FAST vs VOL💰 etc), what did a specific ticker's alert
look like last time.

Design choices, and why:
  - Plain stdlib `sqlite3`, not an ORM or async driver (aiosqlite etc) — zero
    new pip dependency, zero new requirements.txt entry, nothing that can
    fail to install on a Railway redeploy. Blocking calls are pushed off the
    event loop via asyncio.to_thread so they never stall the detection loop.
  - A fresh connection per operation rather than one long-lived shared
    connection — avoids any cross-thread SQLite connection-sharing pitfalls
    entirely; at this write frequency the per-call connection overhead
    (sub-millisecond on local/volume disk) is not a real cost.
  - Lives on the same directory as astaroth_state.json (_DATA_DIR, passed in
    from the caller) — so it survives Railway redeploys exactly when the
    JSON state does (a Volume is mounted), and degrades exactly the same way
    (resets on redeploy) when one isn't. No new persistence story to reason
    about.
  - Every public function catches its own exceptions and logs+no-ops instead
    of raising — a disk hiccup or a locked file must never be able to break
    the alert pipeline that's trying to log to it. The DB is a nice-to-have
    on top of a working bot, never a dependency the bot needs to function.
"""
import os
import sqlite3
import asyncio
import logging
import time
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)

_db_path: Optional[str] = None
_enabled = False

_SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    mint          TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    chain_id      TEXT NOT NULL,
    alert_reason  TEXT NOT NULL,
    alerted_at    REAL NOT NULL,
    market_cap    REAL,
    volume_usd    REAL,
    liquidity     REAL,
    buy_ratio     REAL,
    buys_h1       INTEGER,
    ws_discovered INTEGER,
    age_seconds   REAL
);
CREATE INDEX IF NOT EXISTS idx_alerts_chain_time ON alerts(chain_id, alerted_at);
CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alerts(symbol);

-- [v4.50] Multi-subscriber access control for the admin panel. A row here is
-- one Telegram user who has ever messaged the bot with /start. status starts
-- "pending" and only an admin-panel action moves it to "approved" (alerts
-- start going to telegram_chat_id) or "revoked" (alerts stop, and stay
-- stopped even if the same person messages /start again — see
-- request_subscriber's explicit "don't resurrect a revoked row" behavior).
-- The bot owner's own TELEGRAM_CHAT_ID never goes through this table at all
-- (checked separately, see ADMIN_CHAT_ID in token_tracker_polling.py) — this
-- is only for everyone else.
CREATE TABLE IF NOT EXISTS subscribers (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_user_id  TEXT NOT NULL UNIQUE,
    telegram_chat_id  TEXT NOT NULL,
    username          TEXT,
    first_name        TEXT,
    status            TEXT NOT NULL DEFAULT 'pending',  -- pending | approved | revoked
    requested_at      REAL NOT NULL,
    decided_at        REAL
);
CREATE INDEX IF NOT EXISTS idx_subscribers_status ON subscribers(status);
"""


def init_db(data_dir: str) -> bool:
    """
    Create/open the SQLite file at {data_dir}/astaroth.db and ensure the
    schema exists. Safe to call every startup. Returns True if the DB is
    usable, False if it failed (in which case every other function in this
    module silently no-ops — the bot runs exactly as it did before this
    feature existed).
    """
    global _db_path, _enabled
    path = os.path.join(data_dir, "astaroth.db")
    try:
        conn = sqlite3.connect(path, timeout=5)
        try:
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            conn.close()
        _db_path = path
        _enabled = True
        logger.info(f"🗄️ Alert history DB ready: {path}")
        return True
    except Exception as e:
        _enabled = False
        logger.warning(f"⚠️ Alert history DB unavailable ({e}) — continuing without it")
        return False


def _insert_alert(row: Dict) -> None:
    conn = sqlite3.connect(_db_path, timeout=5)
    try:
        conn.execute(
            """INSERT INTO alerts
               (mint, symbol, chain_id, alert_reason, alerted_at, market_cap,
                volume_usd, liquidity, buy_ratio, buys_h1, ws_discovered, age_seconds)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                row.get("mint", ""), row.get("symbol", ""), row.get("chain_id", ""),
                row.get("alert_reason", ""), row.get("alerted_at", time.time()),
                row.get("market_cap"), row.get("volume_usd"), row.get("liquidity"),
                row.get("buy_ratio"), row.get("buys_h1"),
                1 if row.get("ws_discovered") else 0, row.get("age_seconds"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


async def log_alert_async(row: Dict) -> None:
    """Fire-and-forget style: await this from asyncio.create_task so a slow
    disk never delays the alert path itself. Never raises."""
    if not _enabled:
        return
    try:
        await asyncio.to_thread(_insert_alert, row)
    except Exception as e:
        logger.debug(f"Alert history write failed: {e}")


def _select_alerts(limit: int, chain_id: Optional[str], symbol: Optional[str]) -> List[Dict]:
    conn = sqlite3.connect(_db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        clauses, params = [], []
        if chain_id:
            clauses.append("chain_id = ?")
            params.append(chain_id)
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol.upper())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(limit, 500)))
        cur = conn.execute(
            f"SELECT * FROM alerts {where} ORDER BY alerted_at DESC LIMIT ?", params
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


async def query_alerts_async(limit: int = 50, chain_id: Optional[str] = None,
                              symbol: Optional[str] = None) -> List[Dict]:
    if not _enabled:
        return []
    try:
        return await asyncio.to_thread(_select_alerts, limit, chain_id, symbol)
    except Exception as e:
        logger.debug(f"Alert history read failed: {e}")
        return []


def _select_stats() -> Dict:
    conn = sqlite3.connect(_db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        total = conn.execute("SELECT COUNT(*) AS n FROM alerts").fetchone()["n"]
        by_chain = {
            r["chain_id"]: r["n"] for r in conn.execute(
                "SELECT chain_id, COUNT(*) AS n FROM alerts GROUP BY chain_id"
            ).fetchall()
        }
        by_reason = {
            r["alert_reason"]: r["n"] for r in conn.execute(
                "SELECT alert_reason, COUNT(*) AS n FROM alerts GROUP BY alert_reason"
            ).fetchall()
        }
        return {"total": total, "by_chain": by_chain, "by_reason": by_reason}
    finally:
        conn.close()


async def get_stats_async() -> Dict:
    if not _enabled:
        return {"total": 0, "by_chain": {}, "by_reason": {}, "enabled": False}
    try:
        stats = await asyncio.to_thread(_select_stats)
        stats["enabled"] = True
        return stats
    except Exception as e:
        logger.debug(f"Alert history stats failed: {e}")
        return {"total": 0, "by_chain": {}, "by_reason": {}, "enabled": False}


# ============================================================================
# [v4.50] Subscriber access control (admin-approved alert fan-out)
# ============================================================================

def _request_subscriber(user_id: str, chat_id: str, username: str, first_name: str) -> str:
    """
    Registers (or looks up) a /start request. Returns one of:
      "new_pending"       — first time seeing this user, row created as pending
      "already_pending"   — they already have an open request
      "already_approved"  — they're already an approved subscriber
      "already_revoked"   — an admin previously revoked them; does NOT
                             resurrect the row to pending (a revoked user
                             re-messaging /start should not silently get a
                             second chance at approval without the admin
                             deciding again on purpose via the panel)
    Never raises -- caller treats any DB failure as "new_pending" being
    unavailable and should fail safe (not send alerts).
    """
    conn = sqlite3.connect(_db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT status FROM subscribers WHERE telegram_user_id = ?", (user_id,)
        ).fetchone()
        if row:
            # Keep contact info fresh (username/chat_id can change) without
            # touching status.
            conn.execute(
                "UPDATE subscribers SET telegram_chat_id = ?, username = ?, first_name = ? "
                "WHERE telegram_user_id = ?",
                (chat_id, username, first_name, user_id),
            )
            conn.commit()
            return f"already_{row['status']}"
        conn.execute(
            "INSERT INTO subscribers (telegram_user_id, telegram_chat_id, username, "
            "first_name, status, requested_at) VALUES (?, ?, ?, ?, 'pending', ?)",
            (user_id, chat_id, username, first_name, time.time()),
        )
        conn.commit()
        return "new_pending"
    finally:
        conn.close()


async def request_subscriber_async(user_id: str, chat_id: str, username: str, first_name: str) -> str:
    if not _enabled:
        return "unavailable"
    try:
        return await asyncio.to_thread(_request_subscriber, user_id, chat_id, username, first_name)
    except Exception as e:
        logger.warning(f"Subscriber request failed: {e}")
        return "unavailable"


def _is_approved(user_id: str) -> bool:
    conn = sqlite3.connect(_db_path, timeout=5)
    try:
        row = conn.execute(
            "SELECT 1 FROM subscribers WHERE telegram_user_id = ? AND status = 'approved'", (user_id,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


async def is_approved_async(user_id: str) -> bool:
    """Used to gate read-only bot commands (/status, /tokens, /gems, etc.) to
    admin + approved subscribers only -- fails CLOSED (returns False) on any
    DB error, since the safe default for an access check is to deny, not
    silently let an unapproved user through."""
    if not _enabled or not user_id:
        return False
    try:
        return await asyncio.to_thread(_is_approved, user_id)
    except Exception as e:
        logger.debug(f"Approval check failed: {e}")
        return False


def _list_subscribers(status: Optional[str]) -> List[Dict]:
    conn = sqlite3.connect(_db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        if status:
            cur = conn.execute(
                "SELECT * FROM subscribers WHERE status = ? ORDER BY requested_at DESC", (status,)
            )
        else:
            cur = conn.execute("SELECT * FROM subscribers ORDER BY requested_at DESC")
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


async def list_subscribers_async(status: Optional[str] = None) -> List[Dict]:
    if not _enabled:
        return []
    try:
        return await asyncio.to_thread(_list_subscribers, status)
    except Exception as e:
        logger.warning(f"Subscriber list failed: {e}")
        return []


def _list_approved_chat_ids() -> List[str]:
    conn = sqlite3.connect(_db_path, timeout=5)
    try:
        cur = conn.execute("SELECT telegram_chat_id FROM subscribers WHERE status = 'approved'")
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


async def list_approved_chat_ids_async() -> List[str]:
    """Used on every alert fire (fan-out) -- must never raise or block on a
    disk hiccup, and fails to an empty list (no fan-out, admin still gets
    the alert through the normal unconditional path) rather than crashing
    the alert pipeline."""
    if not _enabled:
        return []
    try:
        return await asyncio.to_thread(_list_approved_chat_ids)
    except Exception as e:
        logger.debug(f"Approved-subscriber list failed: {e}")
        return []


def _set_subscriber_status(sub_id: int, status: str) -> Optional[Dict]:
    conn = sqlite3.connect(_db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "UPDATE subscribers SET status = ?, decided_at = ? WHERE id = ?",
            (status, time.time(), sub_id),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM subscribers WHERE id = ?", (sub_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


async def approve_subscriber_async(sub_id: int) -> Optional[Dict]:
    if not _enabled:
        return None
    try:
        return await asyncio.to_thread(_set_subscriber_status, sub_id, "approved")
    except Exception as e:
        logger.warning(f"Subscriber approve failed: {e}")
        return None


async def revoke_subscriber_async(sub_id: int) -> Optional[Dict]:
    if not _enabled:
        return None
    try:
        return await asyncio.to_thread(_set_subscriber_status, sub_id, "revoked")
    except Exception as e:
        logger.warning(f"Subscriber revoke failed: {e}")
        return None


def _count_subscribers() -> Dict[str, int]:
    conn = sqlite3.connect(_db_path, timeout=5)
    try:
        rows = conn.execute("SELECT status, COUNT(*) FROM subscribers GROUP BY status").fetchall()
        return {status: n for status, n in rows}
    finally:
        conn.close()


async def count_subscribers_async() -> Dict[str, int]:
    if not _enabled:
        return {}
    try:
        return await asyncio.to_thread(_count_subscribers)
    except Exception as e:
        logger.debug(f"Subscriber count failed: {e}")
        return {}
