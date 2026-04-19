"""
database.py — AlphaDegen persistent storage layer

Improvements applied:
  #8  — TTL-based cleanup for seen_signatures (runs as background task;
         also exposed as cleanup_old_signatures() for manual calls)
  #10 — alert_performance table: records price at alert time, re-checked
         at 30m / 1h / 4h to build a win-rate dataset for scoring tuning.
         Use log_alert_performance() + update_alert_performance_snapshot()
         + run_performance_snapshot_worker() in token_tracker_webhook_v3.py.
"""

import aiosqlite
import asyncio
import logging
import os
import time
from typing import List, Tuple, Optional, Dict

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("SQLITE_PATH", "tracker_data.db")

# How long to keep seen_signatures (hours). Default 48h so dedup covers
# the full monitoring window without the table growing forever.
SIGNATURE_TTL_HOURS = int(os.getenv("SIGNATURE_TTL_HOURS", "48"))

# ============================================================================
# Schema
# ============================================================================

async def init_db():
    """Initialise the SQLite database, creating all tables and indexes."""
    async with aiosqlite.connect(DB_NAME) as db:

        # Seen DeFi programs — dedup for DeFi alerts
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seen_programs (
                program_id TEXT PRIMARY KEY,
                detected_at REAL
            )
        """)

        # Seen webhook signatures — primary dedup table
        # Improvement #8: detected_at index already present; cleanup_old_signatures
        # now runs as a background task.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seen_signatures (
                signature   TEXT PRIMARY KEY,
                mint        TEXT,
                detected_at REAL
            )
        """)

        # Token volume history snapshots
        await db.execute("""
            CREATE TABLE IF NOT EXISTS token_history (
                mint       TEXT,
                timestamp  REAL,
                volume_sol REAL,
                price_usd  REAL,
                PRIMARY KEY (mint, timestamp)
            )
        """)

        # Webhook retry queue
        await db.execute("""
            CREATE TABLE IF NOT EXISTS webhook_retry_queue (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                payload      TEXT    NOT NULL,
                retry_count  INTEGER DEFAULT 0,
                max_retries  INTEGER DEFAULT 3,
                next_retry_at REAL,
                created_at   REAL,
                last_error   TEXT,
                status       TEXT DEFAULT 'pending'
            )
        """)

        # Dead-letter queue for permanently failed webhooks
        await db.execute("""
            CREATE TABLE IF NOT EXISTS webhook_failed_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                payload       TEXT NOT NULL,
                error_message TEXT,
                failed_at     REAL,
                retry_count   INTEGER
            )
        """)

        # Alert history (lightweight log)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                mint            TEXT,
                alert_type      TEXT,
                sent_at         REAL,
                message_preview TEXT
            )
        """)

        # ── Improvement #10: Performance tracking ───────────────────────────
        # Stores price at alert time and outcome snapshots at 30m / 1h / 4h.
        # Rows are written by log_alert_performance() immediately on alert.
        # A background worker fills the snapshot columns after the delay.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS alert_performance (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                mint            TEXT    NOT NULL,
                alert_type      TEXT    NOT NULL,
                alerted_at      REAL    NOT NULL,
                score           REAL,
                price_at_alert  REAL,
                mc_at_alert     REAL,
                liq_at_alert    REAL,
                -- outcome snapshots (filled later by background worker)
                price_30m       REAL,
                price_1h        REAL,
                price_4h        REAL,
                pct_change_30m  REAL,
                pct_change_1h   REAL,
                pct_change_4h   REAL,
                peak_pct_4h     REAL,   -- max price gain seen within 4h window
                snapshot_30m_at REAL,   -- epoch when 30m snapshot was taken
                snapshot_1h_at  REAL,
                snapshot_4h_at  REAL,
                launchpad       TEXT,
                chain           TEXT DEFAULT 'solana'
            )
        """)

        # ── Indexes ──────────────────────────────────────────────────────────
        await db.execute("CREATE INDEX IF NOT EXISTS idx_history_mint     ON token_history(mint)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_retry_status     ON webhook_retry_queue(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_retry_next       ON webhook_retry_queue(next_retry_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_alert_mint       ON alert_history(mint)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sig_detected     ON seen_signatures(detected_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_perf_alerted_at  ON alert_performance(alerted_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_perf_mint        ON alert_performance(mint)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_perf_snapshots   ON alert_performance(snapshot_30m_at, snapshot_1h_at, snapshot_4h_at)")

        await db.commit()
        logger.info("✅ Database initialised")


# ============================================================================
# Seen Programs
# ============================================================================

async def add_seen_program(program_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR IGNORE INTO seen_programs (program_id, detected_at) VALUES (?, ?)",
            (program_id, time.time())
        )
        await db.commit()

async def is_program_seen(program_id: str) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT 1 FROM seen_programs WHERE program_id = ?", (program_id,)
        ) as cursor:
            return await cursor.fetchone() is not None


# ============================================================================
# Webhook Signature Deduplication (#8: TTL cleanup)
# ============================================================================

async def is_signature_seen(signature: str) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT 1 FROM seen_signatures WHERE signature = ?", (signature,)
        ) as cursor:
            return await cursor.fetchone() is not None

async def add_seen_signature(signature: str, mint: str = None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR IGNORE INTO seen_signatures (signature, mint, detected_at) VALUES (?, ?, ?)",
            (signature, mint, time.time())
        )
        await db.commit()

async def get_cached_mint_for_signature(signature: str) -> Optional[str]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT mint FROM seen_signatures WHERE signature = ? AND mint IS NOT NULL",
            (signature,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def cleanup_old_signatures(max_age_hours: int = SIGNATURE_TTL_HOURS) -> int:
    """
    Remove signatures older than *max_age_hours*.

    Returns the number of rows deleted.
    Called automatically by cleanup_signatures_worker() every hour.
    """
    cutoff = time.time() - (max_age_hours * 3600)
    async with aiosqlite.connect(DB_NAME) as db:
        result = await db.execute(
            "DELETE FROM seen_signatures WHERE detected_at < ?", (cutoff,)
        )
        deleted = result.rowcount
        await db.commit()
    if deleted:
        logger.info(f"🧹 [DB] Signature cleanup: removed {deleted} rows older than {max_age_hours}h")
    return deleted

async def cleanup_signatures_worker(interval_seconds: int = 3600):
    """
    Background task: purge old signatures once per hour.
    Spawn in lifespan():
        asyncio.create_task(cleanup_signatures_worker())
    """
    while True:
        try:
            await cleanup_old_signatures()
        except Exception as e:
            logger.error(f"[DB] cleanup_signatures_worker error: {e}")
        await asyncio.sleep(interval_seconds)


# ============================================================================
# Token History
# ============================================================================

async def log_token_volume(mint: str, volume_sol: float, price_usd: float = 0.0):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO token_history (mint, timestamp, volume_sol, price_usd) VALUES (?, ?, ?, ?)",
            (mint, time.time(), volume_sol, price_usd)
        )
        await db.commit()

async def get_token_history(mint: str, hours_back: int = 24) -> List[Tuple[float, float]]:
    cutoff = time.time() - (hours_back * 3600)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT timestamp, volume_sol FROM token_history WHERE mint = ? AND timestamp > ? ORDER BY timestamp ASC",
            (mint, cutoff)
        ) as cursor:
            return await cursor.fetchall()

async def cleanup_old_data(days: int = 7):
    cutoff = time.time() - (days * 86400)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM token_history WHERE timestamp < ?", (cutoff,))
        await db.execute("DELETE FROM alert_history WHERE sent_at < ?", (cutoff,))
        await db.commit()


# ============================================================================
# Webhook Retry Queue
# ============================================================================

async def add_to_retry_queue(payload: str, next_retry_at: float, max_retries: int = 3) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """INSERT INTO webhook_retry_queue
            (payload, retry_count, max_retries, next_retry_at, created_at, status)
            VALUES (?, 0, ?, ?, ?, 'pending')""",
            (payload, max_retries, next_retry_at, time.time())
        )
        await db.commit()
        return cursor.lastrowid

async def get_pending_retries() -> List[Tuple]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            """SELECT id, payload, retry_count, max_retries
            FROM webhook_retry_queue
            WHERE status = 'pending' AND next_retry_at <= ?
            ORDER BY next_retry_at ASC LIMIT 100""",
            (time.time(),)
        ) as cursor:
            return await cursor.fetchall()

async def update_retry_status(retry_id: int, success: bool, error_message: Optional[str] = None):
    async with aiosqlite.connect(DB_NAME) as db:
        if success:
            await db.execute(
                "UPDATE webhook_retry_queue SET status = 'completed' WHERE id = ?",
                (retry_id,)
            )
        else:
            await db.execute(
                """UPDATE webhook_retry_queue
                SET retry_count = retry_count + 1,
                    last_error = ?,
                    next_retry_at = ?
                WHERE id = ?""",
                (error_message, time.time() + (2 ** (retry_id % 5)) * 60, retry_id)
            )
            async with db.execute(
                "SELECT retry_count, max_retries, payload FROM webhook_retry_queue WHERE id = ?",
                (retry_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row and row[0] >= row[1]:
                    await db.execute(
                        "UPDATE webhook_retry_queue SET status = 'failed' WHERE id = ?",
                        (retry_id,)
                    )
                    await add_to_dead_letter_queue(row[2], error_message, row[0])
        await db.commit()

async def add_to_dead_letter_queue(payload: str, error_message: Optional[str], retry_count: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """INSERT INTO webhook_failed_events (payload, error_message, failed_at, retry_count)
            VALUES (?, ?, ?, ?)""",
            (payload, error_message, time.time(), retry_count)
        )
        await db.commit()


# ============================================================================
# Alert History
# ============================================================================

async def log_alert(mint: str, alert_type: str, message_preview: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """INSERT INTO alert_history (mint, alert_type, sent_at, message_preview)
            VALUES (?, ?, ?, ?)""",
            (mint, alert_type, time.time(), message_preview[:200])
        )
        await db.commit()

async def get_alert_history(mint: str, hours_back: int = 24) -> List[Tuple]:
    cutoff = time.time() - (hours_back * 3600)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            """SELECT alert_type, sent_at, message_preview
            FROM alert_history
            WHERE mint = ? AND sent_at > ?
            ORDER BY sent_at DESC""",
            (mint, cutoff)
        ) as cursor:
            return await cursor.fetchall()

async def was_alert_sent_recently(mint: str, alert_type: str, hours: int = 1) -> bool:
    cutoff = time.time() - (hours * 3600)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            """SELECT 1 FROM alert_history
            WHERE mint = ? AND alert_type = ? AND sent_at > ?
            LIMIT 1""",
            (mint, alert_type, cutoff)
        ) as cursor:
            return await cursor.fetchone() is not None


# ============================================================================
# Alert Performance Tracking  (Improvement #10)
# ============================================================================

async def log_alert_performance(
    mint: str,
    alert_type: str,
    score: float,
    price_at_alert: float,
    mc_at_alert: float,
    liq_at_alert: float,
    launchpad: str = "Unknown",
    chain: str = "solana",
) -> int:
    """
    Record a new alert for performance tracking.

    Returns the row ID so the caller can reference it for snapshot updates.
    Call this immediately when an alert fires, right after send_telegram_message().
    """
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """INSERT INTO alert_performance
               (mint, alert_type, alerted_at, score,
                price_at_alert, mc_at_alert, liq_at_alert,
                launchpad, chain)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (mint, alert_type, time.time(), score,
             price_at_alert, mc_at_alert, liq_at_alert,
             launchpad, chain)
        )
        await db.commit()
        return cursor.lastrowid


async def update_alert_performance_snapshot(
    row_id: int,
    window: str,         # "30m" | "1h" | "4h"
    current_price: float,
    price_at_alert: float,
) -> None:
    """
    Fill in a snapshot column for a previously logged alert.
    Also tracks peak gain within the 4h window via peak_pct_4h.

    window must be one of: "30m", "1h", "4h"
    """
    if window not in ("30m", "1h", "4h"):
        raise ValueError(f"Invalid window '{window}'. Use 30m, 1h or 4h.")

    pct_change = ((current_price - price_at_alert) / price_at_alert * 100.0
                  if price_at_alert else 0.0)
    now = time.time()

    col_price   = f"price_{window}"
    col_pct     = f"pct_change_{window.replace('m','m').replace('h','h')}"  # keeps name
    col_snap_at = f"snapshot_{window}_at"

    async with aiosqlite.connect(DB_NAME) as db:
        # Update the primary snapshot columns
        await db.execute(
            f"""UPDATE alert_performance
                SET {col_price} = ?, {col_pct} = ?, {col_snap_at} = ?
                WHERE id = ?""",
            (current_price, pct_change, now, row_id)
        )
        # Update peak_pct_4h if this is a new high
        await db.execute(
            """UPDATE alert_performance
               SET peak_pct_4h = MAX(COALESCE(peak_pct_4h, -999), ?)
               WHERE id = ?""",
            (pct_change, row_id)
        )
        await db.commit()


async def get_pending_performance_snapshots() -> List[Dict]:
    """
    Return rows that still need snapshot(s) taken.
    Called by run_performance_snapshot_worker() every 60s.

    A row needs a snapshot when:
      - 30m has passed and snapshot_30m_at IS NULL
      - 1h  has passed and snapshot_1h_at  IS NULL
      - 4h  has passed and snapshot_4h_at  IS NULL
    """
    now = time.time()
    rows = []
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            """SELECT id, mint, alerted_at,
                      price_at_alert,
                      snapshot_30m_at, snapshot_1h_at, snapshot_4h_at
               FROM alert_performance
               WHERE alerted_at > ?        -- only within last 5h (4h window + buffer)
                 AND (
                   (snapshot_30m_at IS NULL AND ? - alerted_at >= 1800) OR
                   (snapshot_1h_at  IS NULL AND ? - alerted_at >= 3600) OR
                   (snapshot_4h_at  IS NULL AND ? - alerted_at >= 14400)
                 )
               ORDER BY alerted_at ASC
               LIMIT 50""",
            (now - 18000, now, now, now)
        ) as cursor:
            for r in await cursor.fetchall():
                rows.append({
                    "id":             r[0],
                    "mint":           r[1],
                    "alerted_at":     r[2],
                    "price_at_alert": r[3],
                    "need_30m":  r[4] is None and (now - r[2]) >= 1800,
                    "need_1h":   r[5] is None and (now - r[2]) >= 3600,
                    "need_4h":   r[6] is None and (now - r[2]) >= 14400,
                })
    return rows


async def get_performance_summary(days_back: int = 7) -> Dict:
    """
    Aggregate win-rate stats for tuning the scoring algorithm.
    Returns:
      {
        "total_alerts": int,
        "win_rate_1h":  float,   # % of alerts where pct_change_1h > 0
        "win_rate_4h":  float,
        "avg_change_1h": float,
        "avg_change_4h": float,
        "best_alert":   dict,
        "worst_alert":  dict,
      }
    """
    cutoff = time.time() - (days_back * 86400)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            """SELECT
                COUNT(*) as total,
                AVG(CASE WHEN pct_change_1h IS NOT NULL THEN 1 ELSE NULL END) as has_1h,
                SUM(CASE WHEN pct_change_1h > 0 THEN 1 ELSE 0 END) as wins_1h,
                SUM(CASE WHEN pct_change_4h > 0 THEN 1 ELSE 0 END) as wins_4h,
                AVG(pct_change_1h) as avg_1h,
                AVG(pct_change_4h) as avg_4h,
                COUNT(pct_change_1h) as count_1h,
                COUNT(pct_change_4h) as count_4h
               FROM alert_performance
               WHERE alerted_at > ?""",
            (cutoff,)
        ) as cursor:
            row = await cursor.fetchone()

        if not row or row[0] == 0:
            return {"total_alerts": 0}

        total, _, wins_1h, wins_4h, avg_1h, avg_4h, count_1h, count_4h = row

        # Best and worst by 4h return
        async with db.execute(
            """SELECT mint, alert_type, score, pct_change_4h, alerted_at
               FROM alert_performance
               WHERE alerted_at > ? AND pct_change_4h IS NOT NULL
               ORDER BY pct_change_4h DESC LIMIT 1""",
            (cutoff,)
        ) as c:
            best = await c.fetchone()

        async with db.execute(
            """SELECT mint, alert_type, score, pct_change_4h, alerted_at
               FROM alert_performance
               WHERE alerted_at > ? AND pct_change_4h IS NOT NULL
               ORDER BY pct_change_4h ASC LIMIT 1""",
            (cutoff,)
        ) as c:
            worst = await c.fetchone()

    return {
        "total_alerts":   total,
        "win_rate_1h":    round(wins_1h / count_1h * 100, 1) if count_1h else None,
        "win_rate_4h":    round(wins_4h / count_4h * 100, 1) if count_4h else None,
        "avg_change_1h":  round(avg_1h, 2) if avg_1h is not None else None,
        "avg_change_4h":  round(avg_4h, 2) if avg_4h is not None else None,
        "snapshots_with_1h": count_1h,
        "snapshots_with_4h": count_4h,
        "best_alert":  dict(zip(
            ["mint","alert_type","score","pct_change_4h","alerted_at"], best
        )) if best else None,
        "worst_alert": dict(zip(
            ["mint","alert_type","score","pct_change_4h","alerted_at"], worst
        )) if worst else None,
    }


async def run_performance_snapshot_worker(
    fetch_price_fn,   # async (mint: str) -> float | None
    interval_seconds: int = 60,
):
    """
    Background worker that fills in 30m / 1h / 4h price snapshots.

    Usage in lifespan():
        from database import run_performance_snapshot_worker
        from alpha_engine import fetch_price_for_mint   # or any price fetcher

        asyncio.create_task(
            run_performance_snapshot_worker(fetch_price_for_mint)
        )

    Args:
        fetch_price_fn:   Async callable (mint: str) -> Optional[float]
                          Should return current USD price or None on failure.
        interval_seconds: How often to check for pending snapshots (default 60s).
    """
    logger.info("📊 [PerfTracker] Performance snapshot worker started")
    while True:
        try:
            pending = await get_pending_performance_snapshots()
            if pending:
                logger.debug(f"[PerfTracker] {len(pending)} rows need snapshots")

            for row in pending:
                try:
                    price = await fetch_price_fn(row["mint"])
                    if price is None or price <= 0:
                        continue

                    for window in ("30m", "1h", "4h"):
                        if row.get(f"need_{window}"):
                            await update_alert_performance_snapshot(
                                row["id"], window, price, row["price_at_alert"]
                            )
                            logger.debug(
                                f"[PerfTracker] {window} snapshot for {row['mint'][:12]}… "
                                f"price={price:.8f}"
                            )
                    await asyncio.sleep(0.2)  # polite spacing
                except Exception as e:
                    logger.debug(f"[PerfTracker] snapshot error for {row['mint'][:12]}: {e}")

        except Exception as e:
            logger.error(f"[PerfTracker] worker loop error: {e}")

        await asyncio.sleep(interval_seconds)
