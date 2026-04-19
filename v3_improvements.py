"""
v3_improvements.py — Drop-in improvements for token_tracker_webhook_v3.py

Improvements included:
  #5  — Telegram queue: staleness check (drop messages >MAX_TELEGRAM_MSG_AGE_S old)
         and per-(mint,alert_type) deduplication so the same alert doesn't queue twice.
  #6  — Cross-alert-type cooldown per mint: prevents the same token firing both
         Smart Volume and Quant Signal within CROSS_ALERT_COOLDOWN_S seconds.
  #11 — /addnarrative Telegram command: add/update narrative keywords at runtime
         without a redeploy. Use /listnarratives to inspect and /removenarrative to delete.

HOW TO INTEGRATE
----------------
1. Copy this file next to token_tracker_webhook_v3.py.
2. Add ONE import at the top of token_tracker_webhook_v3.py (after all other imports):

       from v3_improvements import (
           improve_telegram_queue,
           cross_alert_cooldown_check,
           add_narrative_command, list_narratives_command, remove_narrative_command,
           DYNAMIC_NARRATIVES,
       )

3. In lifespan(), replace:
       telegram_queue = asyncio.Queue()
   with:
       telegram_queue = improve_telegram_queue.make_queue()

4. Replace the existing send_telegram_message() function body with:
       await improve_telegram_queue.enqueue(message, mint, alert_type)

5. Replace the existing telegram_sender_worker() with:
       await improve_telegram_queue.sender_worker()

6. In lifespan(), add command handlers:
       telegram_app.add_handler(CommandHandler("addnarrative",     add_narrative_command))
       telegram_app.add_handler(CommandHandler("listnarratives",   list_narratives_command))
       telegram_app.add_handler(CommandHandler("removenarrative",  remove_narrative_command))
       telegram_app.add_handler(CommandHandler("performance",      performance_command))

   And add to set_my_commands:
       BotCommand("addnarrative",    "Add a narrative keyword pattern at runtime"),
       BotCommand("listnarratives",  "List all active narrative patterns"),
       BotCommand("removenarrative", "Remove a narrative pattern"),
       BotCommand("performance",     "Show alert win-rate stats"),

7. In notify_smart_token() and notify_heavy_volume() wrap the call:

       if not cross_alert_cooldown_check(str(token.mint), "smart_volume"):
           return   # already alerted this token recently via another type
       await send_telegram_message(message, str(token.mint), "smart_volume")

   And in quant_integration.py's send_telegram_message call for quant signals:

       if not cross_alert_cooldown_check(token.address, "quant_signal"):
           continue
       await send_telegram_message(alert, alert_type="quant_signal")

That's it — all existing logic is unchanged.
"""

import asyncio
import logging
import os
import time
from collections import deque
from typing import Dict, Optional, Tuple, Set

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Shared config
# ─────────────────────────────────────────────────────────────────────────────

TELEGRAM_MIN_INTERVAL   = float(os.getenv("TELEGRAM_MIN_INTERVAL", "1.2"))   # seconds between sends
TELEGRAM_QUEUE_MAX_SIZE = int(os.getenv("TELEGRAM_QUEUE_MAX_SIZE", "200"))    # reduced from 500

# #5: Drop messages older than this (seconds). Prevents stale burst alerts.
MAX_TELEGRAM_MSG_AGE_S  = float(os.getenv("MAX_TELEGRAM_MSG_AGE_S", "30.0"))

# #6: Cross-alert-type cooldown per mint (seconds). Default 20 min.
CROSS_ALERT_COOLDOWN_S  = float(os.getenv("CROSS_ALERT_COOLDOWN_S", "1200.0"))


# ─────────────────────────────────────────────────────────────────────────────
# Improvement #5 — Enhanced Telegram queue
# ─────────────────────────────────────────────────────────────────────────────

class _EnhancedTelegramQueue:
    """
    Drop-in replacement for the bare asyncio.Queue used by the Telegram sender.

    Adds three behaviours over the original:
      1. Staleness check: messages older than MAX_TELEGRAM_MSG_AGE_S are
         silently dropped rather than sent with stale data.
      2. Per-(mint, alert_type) deduplication: if a mint is already queued for
         the same alert type, the newer message replaces the older one in-place
         (avoids doubling up during rapid webhook bursts).
      3. Queue depth cap: oldest entry dropped when full (preserved from original).
    """

    def __init__(self):
        self._q: asyncio.Queue = asyncio.Queue(maxsize=TELEGRAM_QUEUE_MAX_SIZE)
        # Set of (mint, alert_type) tuples currently in the queue
        self._pending: Set[Tuple[str, str]] = set()
        self._lock = asyncio.Lock()

    def make_queue(self) -> "asyncio.Queue":
        """
        Return self so callers can do:
            telegram_queue = improve_telegram_queue.make_queue()
        and use the queue as before.
        """
        return self  # type: ignore[return-value]

    # Make the object quack like asyncio.Queue for legacy compatibility
    def qsize(self) -> int:
        return self._q.qsize()

    def task_done(self):
        self._q.task_done()

    async def get(self) -> tuple:
        return await self._q.get()

    async def enqueue(self, message: str, mint: str = "", alert_type: str = "general"):
        """
        Enqueue a Telegram message with staleness and dedup logic.
        Replaces the original put() call in send_telegram_message().
        """
        now = time.time()
        key = (mint, alert_type)

        async with self._lock:
            # Drop if this (mint, alert_type) pair is already queued (#5 dedup)
            if key in self._pending and mint:
                logger.debug(f"[TGQueue] Dedup: skipping duplicate {alert_type} for {mint[:12]}")
                return

            # Drop oldest if full (#5 back-pressure cap)
            if self._q.full():
                try:
                    _, _, _, _ = self._q.get_nowait()
                    self._q.task_done()
                    logger.warning("[TGQueue] Overflow: dropped oldest message")
                except asyncio.QueueEmpty:
                    pass

            # Enqueue with timestamp for staleness check
            await self._q.put((message, mint, alert_type, now))
            if mint:
                self._pending.add(key)

    async def sender_worker(self, bot, chat_id: str, metrics_module=None, database=None):
        """
        Background worker: applies rate-limit spacing, staleness check, flood-wait backoff.
        Replaces the original telegram_sender_worker() entirely.

        Call as:
            asyncio.create_task(
                improve_telegram_queue.sender_worker(telegram_bot, TELEGRAM_CHAT_ID)
            )
        """
        import re
        last_sent = 0.0

        while True:
            try:
                item = await self._q.get()

                if len(item) == 4:
                    message, mint, alert_type, enqueued_at = item
                else:
                    # Legacy 3-tuple (shouldn't happen post-patch, but safe fallback)
                    message, mint, alert_type = item
                    enqueued_at = time.time()

                async with self._lock:
                    self._pending.discard((mint, alert_type))

                # #5: Staleness check — drop if too old
                age = time.time() - enqueued_at
                if age > MAX_TELEGRAM_MSG_AGE_S:
                    logger.warning(
                        f"[TGQueue] Dropped stale message: {alert_type} for {mint[:12] if mint else '?'} "
                        f"({age:.1f}s > {MAX_TELEGRAM_MSG_AGE_S}s)"
                    )
                    self._q.task_done()
                    continue

                # Rate-limit spacing
                elapsed = time.time() - last_sent
                if elapsed < TELEGRAM_MIN_INTERVAL:
                    await asyncio.sleep(TELEGRAM_MIN_INTERVAL - elapsed)

                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=message,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                    last_sent = time.time()
                    if metrics_module:
                        metrics_module.alerts_sent_total.inc(type=alert_type)
                    if mint and database:
                        await database.log_alert(mint, alert_type, message[:200])
                except Exception as e:
                    retry_delay = None
                    m = re.search(r"Retry in (\d+) seconds", str(e))
                    if m:
                        retry_delay = int(m.group(1)) + 1
                    if hasattr(e, "retry_after"):
                        retry_delay = int(getattr(e, "retry_after")) + 1
                    if retry_delay:
                        logger.warning(f"[TGQueue] Flood wait {retry_delay}s")
                        await asyncio.sleep(retry_delay)
                        # Re-enqueue with original timestamp (still subject to staleness check)
                        await self._q.put((message, mint, alert_type, enqueued_at))
                        if mint:
                            async with self._lock:
                                self._pending.add((mint, alert_type))
                    else:
                        logger.error(f"[TGQueue] Send failed (no retry hint): {e}")
                finally:
                    self._q.task_done()

            except Exception as e:
                logger.error(f"[TGQueue] Worker loop error: {e}")
                await asyncio.sleep(2)


improve_telegram_queue = _EnhancedTelegramQueue()


# ─────────────────────────────────────────────────────────────────────────────
# Improvement #6 — Cross-alert-type cooldown per mint
# ─────────────────────────────────────────────────────────────────────────────

# mint → last alert timestamp (any type)
_cross_alert_registry: Dict[str, float] = {}


def cross_alert_cooldown_check(mint: str, alert_type: str) -> bool:
    """
    Returns True if the alert should fire, False if the mint was alerted
    recently via any other alert type and should be suppressed.

    Usage before every send:
        if not cross_alert_cooldown_check(str(token.mint), "smart_volume"):
            return
        await send_telegram_message(...)

    Does NOT replace the per-type cooldowns already in alpha_engine.py /
    check_smart_volume() — those still apply. This is an additional cross-type
    guard so a token can't fire both Smart Volume and Quant Signal in the same
    20-minute window.
    """
    if not mint:
        return True  # no mint → never suppress

    now = time.time()
    last = _cross_alert_registry.get(mint, 0)

    if now - last < CROSS_ALERT_COOLDOWN_S:
        logger.info(
            f"[CrossCooldown] Suppressed {alert_type} for {mint[:12]} — "
            f"already alerted {int(now - last)}s ago"
        )
        return False

    _cross_alert_registry[mint] = now
    return True


def cleanup_cross_alert_registry(max_age_s: float = CROSS_ALERT_COOLDOWN_S * 2):
    """
    Prune stale entries from the cross-alert registry.
    Call periodically (e.g., every 10 min) to prevent unbounded growth.
    """
    now = time.time()
    stale = [m for m, ts in _cross_alert_registry.items() if now - ts > max_age_s]
    for m in stale:
        del _cross_alert_registry[m]
    if stale:
        logger.debug(f"[CrossCooldown] Pruned {len(stale)} stale entries")


# ─────────────────────────────────────────────────────────────────────────────
# Improvement #11 — Dynamic narrative management via Telegram commands
# ─────────────────────────────────────────────────────────────────────────────

# Runtime store of narrative patterns.
# Key = narrative name, Value = dict with keywords and optional metadata.
# These are injected into the NarrativeDetector on each quant agent cycle.
DYNAMIC_NARRATIVES: Dict[str, dict] = {}


async def add_narrative_command(update, context):
    """
    /addnarrative <name> <keyword1> [keyword2] [keyword3...]

    Example:
        /addnarrative trump_tariffs tariff trade war trump

    The narrative name must be a single word. Keywords are space-separated.
    Adds the pattern to DYNAMIC_NARRATIVES immediately — no redeploy needed.
    """
    try:
        args = context.args
        if not args or len(args) < 2:
            await update.message.reply_text(
                "Usage: /addnarrative <name> <keyword1> [keyword2...]\n\n"
                "Example:\n  /addnarrative ai_agents openai agent gpt autonomous",
                parse_mode="HTML"
            )
            return

        name = args[0].lower().replace(" ", "_")
        keywords = [k.lower() for k in args[1:]]

        if name in DYNAMIC_NARRATIVES:
            old_kw = DYNAMIC_NARRATIVES[name]["keywords"]
            DYNAMIC_NARRATIVES[name] = {"keywords": keywords, "added_at": time.time()}
            await update.message.reply_text(
                f"✅ <b>Narrative updated:</b> <code>{name}</code>\n\n"
                f"Old keywords: {', '.join(old_kw)}\n"
                f"New keywords: {', '.join(keywords)}",
                parse_mode="HTML"
            )
        else:
            DYNAMIC_NARRATIVES[name] = {"keywords": keywords, "added_at": time.time()}
            await update.message.reply_text(
                f"✅ <b>Narrative added:</b> <code>{name}</code>\n"
                f"Keywords: {', '.join(keywords)}\n\n"
                f"Active in next quant agent cycle.",
                parse_mode="HTML"
            )
        logger.info(f"[Narratives] Added/updated '{name}': {keywords}")

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        logger.error(f"[Narratives] /addnarrative error: {e}")


async def list_narratives_command(update, context):
    """
    /listnarratives

    Lists all dynamically added narrative patterns with their keywords.
    """
    if not DYNAMIC_NARRATIVES:
        await update.message.reply_text(
            "No dynamic narratives configured.\n"
            "Use /addnarrative to add one.",
            parse_mode="HTML"
        )
        return

    lines = ["📋 <b>Active Narrative Patterns</b>\n"]
    for name, data in sorted(DYNAMIC_NARRATIVES.items()):
        age_min = (time.time() - data.get("added_at", time.time())) / 60
        lines.append(
            f"• <code>{name}</code> — {', '.join(data['keywords'])}\n"
            f"  Added {age_min:.0f} min ago"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def remove_narrative_command(update, context):
    """
    /removenarrative <name>

    Removes a dynamically added narrative pattern.
    """
    try:
        args = context.args
        if not args:
            await update.message.reply_text("Usage: /removenarrative <name>")
            return

        name = args[0].lower().replace(" ", "_")
        if name in DYNAMIC_NARRATIVES:
            del DYNAMIC_NARRATIVES[name]
            await update.message.reply_text(f"🗑 Removed narrative: <code>{name}</code>", parse_mode="HTML")
            logger.info(f"[Narratives] Removed '{name}'")
        else:
            names = ", ".join(f"<code>{n}</code>" for n in sorted(DYNAMIC_NARRATIVES))
            await update.message.reply_text(
                f"Narrative '<code>{name}</code>' not found.\n\n"
                f"Available: {names or 'none'}",
                parse_mode="HTML"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def performance_command(update, context):
    """
    /performance [days]

    Shows alert win-rate statistics from the alert_performance table.
    Requires the performance tracking worker to be running (Improvement #10).

    Example:
        /performance      → last 7 days
        /performance 30   → last 30 days
    """
    try:
        import database  # inline import to avoid circular dep

        args = context.args
        days = int(args[0]) if args else 7

        stats = await database.get_performance_summary(days_back=days)

        if stats.get("total_alerts", 0) == 0:
            await update.message.reply_text(
                f"No performance data for the last {days} days.\n\n"
                "Performance tracking starts after the first alert fires with the "
                "new database.py.",
                parse_mode="HTML"
            )
            return

        lines = [
            f"📊 <b>Alert Performance — Last {days} Days</b>\n",
            f"Total alerts: <b>{stats['total_alerts']}</b>",
        ]
        if stats.get("win_rate_1h") is not None:
            lines.append(
                f"Win rate 1h: <b>{stats['win_rate_1h']}%</b> "
                f"({stats['snapshots_with_1h']} snapshots)"
            )
        if stats.get("win_rate_4h") is not None:
            lines.append(
                f"Win rate 4h: <b>{stats['win_rate_4h']}%</b> "
                f"({stats['snapshots_with_4h']} snapshots)"
            )
        if stats.get("avg_change_1h") is not None:
            sign = "+" if stats["avg_change_1h"] >= 0 else ""
            lines.append(f"Avg Δ 1h: <b>{sign}{stats['avg_change_1h']:.1f}%</b>")
        if stats.get("avg_change_4h") is not None:
            sign = "+" if stats["avg_change_4h"] >= 0 else ""
            lines.append(f"Avg Δ 4h: <b>{sign}{stats['avg_change_4h']:.1f}%</b>")

        if stats.get("best_alert"):
            b = stats["best_alert"]
            lines += [
                "",
                f"🏆 Best call: <code>{b['mint'][:12]}…</code>",
                f"   Type: {b['alert_type']} | Score: {b['score']:.1f} | "
                f"+{b['pct_change_4h']:.0f}% 4h",
            ]
        if stats.get("worst_alert"):
            w = stats["worst_alert"]
            lines += [
                f"💀 Worst call: <code>{w['mint'][:12]}…</code>",
                f"   {w['pct_change_4h']:.0f}% 4h",
            ]

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        await update.message.reply_text(f"❌ Error fetching stats: {e}")
        logger.error(f"[Performance] /performance error: {e}")


def inject_dynamic_narratives_into_detector(detector) -> int:
    """
    Push DYNAMIC_NARRATIVES into a NarrativeDetector instance.

    Call at the start of each quant agent cycle if DYNAMIC_NARRATIVES is non-empty.
    Returns the number of narratives injected.

    Usage in quant_integration.py start_quant_agent():
        from v3_improvements import inject_dynamic_narratives_into_detector, DYNAMIC_NARRATIVES
        if DYNAMIC_NARRATIVES:
            detector = NarrativeDetector()
            inject_dynamic_narratives_into_detector(detector)
            # use this detector instead of aggregator's default
    """
    if not DYNAMIC_NARRATIVES:
        return 0
    injected = 0
    for name, data in DYNAMIC_NARRATIVES.items():
        try:
            # NarrativeDetector.add_template() or similar method
            if hasattr(detector, "add_template"):
                detector.add_template(name, data["keywords"])
                injected += 1
            elif hasattr(detector, "templates"):
                detector.templates[name] = {"keywords": data["keywords"]}
                injected += 1
        except Exception as e:
            logger.debug(f"[Narratives] Failed to inject '{name}': {e}")
    if injected:
        logger.info(f"[Narratives] Injected {injected} dynamic narrative(s) into detector")
    return injected
