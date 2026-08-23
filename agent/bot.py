# ⚠️ NOT PART OF THE DEPLOYED SERVICE — see LEGACY_STANDALONE_BOT.md at the
# repo root. Railway only ever runs token_tracker_polling.py (Procfile /
# railway.toml / nixpacks.toml); this class is never instantiated in
# production. Its KOL_ACCOUNTS-driven tracking below is a separate,
# unused implementation of the same idea as token_tracker_polling.py's live
# kol_polling_loop() — don't extend KOL logic here expecting it to affect
# the running bot.
import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

from signals.monitor import SignalAggregator, Signal
from signals.narrative import NarrativeDetector
from tracker.pumpfun import PumpFunTracker, PumpToken
from utils.keywords import format_signal_alert, ANIMAL_TICKER_MAP
from utils.kol_accounts import KOL_ACCOUNTS

logger = logging.getLogger(__name__)


class QuantAgentBot:
    """
    The main Telegram quant agent.
    Combines signal intelligence + pump.fun tracking into one bot.
    """

    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.helius_key = os.getenv("HELIUS_API_KEY", "")
        self.min_liquidity = float(os.getenv("MIN_LIQUIDITY_USD", "5000"))
        self.min_signal_score = int(os.getenv("MIN_RISK_SCORE", "5"))
        self.auto_buy_enabled = os.getenv("AUTO_BUY_ENABLED", "false").lower() == "true"
        self.poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
        self.trend_interval = int(os.getenv("TREND_POLL_INTERVAL_SECONDS", "300"))

        # Core components
        self.signals = SignalAggregator()
        self.tracker = PumpFunTracker(self.helius_key)

        # State
        self.watchlist: Dict[str, PumpToken] = {}
        self.pnl_log: List[Dict] = []
        self.app: Optional[Application] = None
        self.last_signal_run: Optional[datetime] = None
        self.last_trend_run: Optional[datetime] = None

    # ── Bot setup ──────────────────────────────────────────────────────────────

    def build_app(self) -> Application:
        self.app = (
            Application.builder()
            .token(self.token)
            .build()
        )
        # Register handlers
        handlers = [
            ("start",     self.cmd_start),
            ("help",      self.cmd_help),
            ("signals",   self.cmd_signals),
            ("trends",    self.cmd_trends),
            ("scan",      self.cmd_scan),
            ("movers",    self.cmd_movers),
            ("watchlist", self.cmd_watchlist),
            ("watch",     self.cmd_watch),
            ("unwatch",   self.cmd_unwatch),
            ("score",     self.cmd_score),
            ("autobuy",   self.cmd_autobuy),
            ("pnl",       self.cmd_pnl),
            ("animals",   self.cmd_animals),
            ("narratives",self.cmd_narratives),
            ("kols",      self.cmd_kols),
        ]
        for cmd, handler in handlers:
            self.app.add_handler(CommandHandler(cmd, handler))
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))
        return self.app

    # ── Commands ───────────────────────────────────────────────────────────────

    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        text = (
            "🤖 *Quant Agent Online*\n\n"
            f"Monitoring *{len(KOL_ACCOUNTS)} KOL accounts* + news + trends + wildlife.\n\n"
            "*Signal Commands:*\n"
            "/signals — latest signals from all sources\n"
            "/narratives — emerging narratives + ticker predictions\n"
            "/trends — what's trending right now\n"
            "/animals — trending animal tickers\n"
            "/kols — KOL coverage stats\n\n"
            "*Trading Commands:*\n"
            "/scan — find new pump.fun launches\n"
            "/movers — top volume movers\n"
            "/watch $TICKER — add to watchlist\n"
            "/unwatch $TICKER — remove from watchlist\n"
            "/watchlist — your watched tokens\n"
            "/score $TICKER — risk score a token\n"
            "/autobuy on|off — toggle auto-buy\n"
            "/pnl — trading performance\n"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await self.cmd_start(update, ctx)

    async def cmd_signals(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show latest signals from all sources."""
        msg = await update.message.reply_text("📡 Collecting signals... (this may take 20-30s)")

        try:
            all_signals = await self.signals.collect_all_signals()
            top = self.signals.get_top_signals(min_score=self.min_signal_score, limit=5)

            if not top:
                await msg.edit_text("😴 No strong signals right now. Try again in a few minutes.")
                return

            await msg.edit_text(f"📡 *{len(all_signals)} signals collected. Top {len(top)}:*",
                                  parse_mode=ParseMode.MARKDOWN)

            for sig in top:
                # Find matching tokens
                matched = await self.tracker.find_tokens_matching_signals(
                    sig.animal_matches, sig.cashtags
                )
                token_dicts = [
                    {
                        "symbol": t.symbol,
                        "liquidity": t.liquidity_usd,
                        "volume": t.volume_24h,
                        "risk_score": t.risk_score,
                        "address": t.address,
                    }
                    for t in matched
                ]
                alert_text = format_signal_alert(
                    source=sig.source,
                    text=sig.text,
                    animal_matches=sig.animal_matches,
                    cashtags=sig.cashtags,
                    signal_score=sig.signal_score,
                    matched_tokens=token_dicts,
                )
                keyboard = None
                if matched:
                    buttons = [
                        [
                            InlineKeyboardButton(f"👀 Watch ${matched[0].symbol}",
                                                  callback_data=f"watch:{matched[0].address}:{matched[0].symbol}"),
                            InlineKeyboardButton("🔗 DexScreener",
                                                  url=matched[0].dex_url),
                        ]
                    ]
                    keyboard = InlineKeyboardMarkup(buttons)

                await update.message.reply_text(
                    alert_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                    disable_web_page_preview=True,
                )
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Signals command error: {e}")
            await msg.edit_text(f"❌ Error collecting signals: {str(e)[:100]}")

    async def cmd_trends(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show what's currently trending across all sources."""
        msg = await update.message.reply_text("📈 Fetching trends...")

        trending_animals = self.signals.get_trending_animals()
        trending_tickers = self.signals.get_trending_tickers()

        lines = ["📈 *TREND RADAR*\n"]

        if trending_animals:
            lines.append("🐾 *Trending Animals:*")
            for i, (animal, score) in enumerate(list(trending_animals.items())[:8], 1):
                tickers = ANIMAL_TICKER_MAP.get(animal, [])
                tickers_str = " | ".join([f"`${t}`" for t in tickers[:3]])
                lines.append(f"{i}. *{animal.title()}* (score: {score}) → {tickers_str}")
            lines.append("")

        if trending_tickers:
            lines.append("💰 *Trending Tickers Mentioned:*")
            for i, (ticker, count) in enumerate(list(trending_tickers.items())[:10], 1):
                lines.append(f"{i}. `${ticker}` — {count} mentions")
            lines.append("")

        # Recent signal source breakdown
        source_counts: Dict[str, int] = {}
        for sig in self.signals.recent_signals:
            source_counts[sig.source_type] = source_counts.get(sig.source_type, 0) + 1

        if source_counts:
            lines.append("📡 *Signal Sources Active:*")
            type_emojis = {
                "kol": "🐦 KOL Tweets",
                "hashtag": "#️⃣ Hashtags",
                "news": "📰 Crypto News",
                "animal_news": "🦁 Animal News",
                "trends": "📈 Google Trends",
            }
            for stype, count in source_counts.items():
                label = type_emojis.get(stype, stype)
                lines.append(f"  {label}: {count} signals")

        if len(lines) <= 1:
            await msg.edit_text("😴 Run /signals first to populate trend data.")
            return

        lines.append(f"\n_Last updated: {datetime.utcnow().strftime('%H:%M UTC')}_")
        await msg.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    async def cmd_scan(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Scan for newest pump.fun launches."""
        msg = await update.message.reply_text("🔍 Scanning new pump.fun launches...")

        try:
            tokens = await self.tracker.get_new_launches()
            filtered = [
                t for t in tokens
                if t.liquidity_usd >= self.min_liquidity
                and t.risk_score >= self.min_signal_score
            ]

            if not filtered:
                await msg.edit_text(
                    f"😐 No new launches meeting criteria\n"
                    f"(min liquidity: ${self.min_liquidity:,.0f}, min risk score: {self.min_signal_score}/10)"
                )
                return

            await msg.edit_text(
                f"🔍 Found *{len(tokens)}* new launches, *{len(filtered)}* meet criteria:",
                parse_mode=ParseMode.MARKDOWN,
            )

            for token in filtered[:5]:
                card = self.tracker.format_token_card(token)
                buttons = [[
                    InlineKeyboardButton(f"👀 Watch",
                                          callback_data=f"watch:{token.address}:{token.symbol}"),
                    InlineKeyboardButton("🔗 Chart", url=token.dex_url),
                ]]
                await update.message.reply_text(
                    card,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(buttons),
                    disable_web_page_preview=True,
                )
                await asyncio.sleep(0.3)

        except Exception as e:
            logger.error(f"Scan error: {e}")
            await msg.edit_text(f"❌ Scan failed: {str(e)[:100]}")

    async def cmd_movers(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show top volume movers on pump.fun."""
        msg = await update.message.reply_text("📊 Fetching volume movers...")

        try:
            movers = await self.tracker.get_volume_movers(min_volume=10000)

            if not movers:
                await msg.edit_text("😐 No significant volume movers found right now.")
                return

            lines = [f"📊 *TOP {min(len(movers), 10)} VOLUME MOVERS*\n"]
            for i, token in enumerate(movers[:10], 1):
                risk_emoji = "🟢" if token.risk_score >= 7 else "🟡" if token.risk_score >= 5 else "🔴"
                lines.append(
                    f"{i}. {risk_emoji} *${token.symbol}*\n"
                    f"   Vol: ${token.volume_24h:,.0f} | Liq: ${token.liquidity_usd:,.0f}\n"
                    f"   [Chart]({token.dex_url})"
                )

            await msg.edit_text(
                "\n".join(lines),
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
        except Exception as e:
            await msg.edit_text(f"❌ Error: {str(e)[:100]}")

    async def cmd_animals(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show animal-to-ticker reference map + currently trending animals."""
        trending = self.signals.get_trending_animals()

        lines = ["🐾 *ANIMAL TICKER RADAR*\n"]

        if trending:
            lines.append("🔥 *Currently Trending:*")
            for animal, score in list(trending.items())[:5]:
                tickers = ANIMAL_TICKER_MAP.get(animal, [])
                tickers_str = " | ".join([f"`${t}`" for t in tickers[:4]])
                lines.append(f"• *{animal.title()}* → {tickers_str}")
            lines.append("")

        lines += [
            "📖 *Animal → Ticker Map (sample):*",
            "🐿️ Squirrel → `$PNUT` `$SQRL$`,",
            "🦛 Hippo → `$MOODENG` `$HIPPO`",
            "🐧 Penguin → `$PENGU` `$PNG`",
            "🦭 Seal → `$SEAL` `$PUNCH`",
            "🐶 Dog → `$DOGE` `$WIF` `$BONK`",
            "🐸 Frog → `$PEPE` `$FROG`",
            "🦊 Fox → `$FOX`",
            "🐻 Bear → `$BEAR`",
            "🦁 Lion → `$LION`",
            "🦈 Shark → `$SHARK`",
            "",
            "_Run /signals to detect animal mentions in real-time_",
        ]

        await update.message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
        )

    async def cmd_watchlist(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show current watchlist."""
        if not self.watchlist:
            await update.message.reply_text(
                "📋 Your watchlist is empty.\nUse /watch $TICKER or tap Watch on any token."
            )
            return

        lines = ["📋 *YOUR WATCHLIST*\n"]
        for addr, token in self.watchlist.items():
            risk_emoji = "🟢" if token.risk_score >= 7 else "🟡" if token.risk_score >= 5 else "🔴"
            lines.append(
                f"{risk_emoji} *${token.symbol}* — {token.name}\n"
                f"   Liq: ${token.liquidity_usd:,.0f} | Risk: {token.risk_score}/10\n"
                f"   [Chart]({token.dex_url})"
            )

        await update.message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )

    async def cmd_watch(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Add a token to watchlist by ticker. Usage: /watch $DOGE"""
        args = ctx.args
        if not args:
            await update.message.reply_text("Usage: /watch $TICKER\nExample: /watch $PNUT")
            return

        ticker = args[0].upper().lstrip("$")
        msg = await update.message.reply_text(f"🔍 Looking up ${ticker}...")

        pairs = await self.tracker.dex.search_token(ticker)
        if not pairs:
            await msg.edit_text(f"❌ Couldn't find ${ticker} on DexScreener.")
            return

        parsed = self.tracker.dex.parse_pair_to_token(pairs[0])
        if not parsed:
            await msg.edit_text("❌ Failed to parse token data.")
            return

        risk_score = await self.tracker.risk_scorer.score_token(
            parsed, await self.tracker.dex._get_session()
        )
        token = PumpToken(**{**parsed, "risk_score": risk_score})
        self.watchlist[token.address] = token

        await msg.edit_text(
            f"✅ Added to watchlist:\n{self.tracker.format_token_card(token)}",
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )

    async def cmd_unwatch(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Remove token from watchlist."""
        args = ctx.args
        if not args:
            await update.message.reply_text("Usage: /unwatch $TICKER")
            return

        ticker = args[0].upper().lstrip("$")
        removed = False
        for addr, token in list(self.watchlist.items()):
            if token.symbol == ticker:
                del self.watchlist[addr]
                removed = True
                break

        if removed:
            await update.message.reply_text(f"✅ Removed ${ticker} from watchlist.")
        else:
            await update.message.reply_text(f"❌ ${ticker} not in your watchlist.")

    async def cmd_score(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Risk score a token. Usage: /score $PNUT"""
        args = ctx.args
        if not args:
            await update.message.reply_text("Usage: /score $TICKER\nExample: /score $PNUT")
            return

        ticker = args[0].upper().lstrip("$")
        msg = await update.message.reply_text(f"🔬 Scoring ${ticker}...")

        pairs = await self.tracker.dex.search_token(ticker)
        if not pairs:
            await msg.edit_text(f"❌ Couldn't find ${ticker}.")
            return

        parsed = self.tracker.dex.parse_pair_to_token(pairs[0])
        if not parsed:
            await msg.edit_text("❌ Failed to parse token data.")
            return

        risk_score = await self.tracker.risk_scorer.score_token(
            parsed, await self.tracker.dex._get_session()
        )

        verdict = (
            "✅ Looks relatively safe" if risk_score >= 7
            else "⚠️ Moderate risk — DYOR" if risk_score >= 5
            else "🚨 HIGH RISK — potential rug"
        )

        lines = [
            f"🔬 *Risk Analysis: ${ticker}*\n",
            f"Score: {'⭐' * risk_score} ({risk_score}/10)",
            f"Verdict: {verdict}\n",
            f"💧 Liquidity: ${parsed['liquidity_usd']:,.0f}",
            f"📊 Volume 24h: ${parsed['volume_24h']:,.0f}",
            f"💰 Market Cap: ${parsed['market_cap']:,.0f}",
            f"📅 Created: {parsed['created_at'].strftime('%Y-%m-%d %H:%M UTC')}",
            "",
            f"🔗 [View on DexScreener]({parsed['dex_url']})",
        ]
        await msg.edit_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )

    async def cmd_autobuy(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Toggle auto-buy mode."""
        args = ctx.args
        if args and args[0].lower() in ("on", "off"):
            self.auto_buy_enabled = args[0].lower() == "on"

        status = "✅ *ENABLED*" if self.auto_buy_enabled else "❌ *DISABLED*"
        amount = os.getenv("AUTO_BUY_AMOUNT_SOL", "0.1")
        await update.message.reply_text(
            f"🤖 Auto-buy is {status}\n"
            f"Amount per trade: {amount} SOL\n\n"
            f"_Set AUTO\\_BUY\\_ENABLED and AUTO\\_BUY\\_AMOUNT\\_SOL in .env_",
            parse_mode=ParseMode.MARKDOWN,
        )

    async def cmd_pnl(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show trading performance log."""
        if not self.pnl_log:
            await update.message.reply_text(
                "📈 No trades logged yet.\n"
                "Auto-buy trades will appear here once you enable /autobuy."
            )
            return

        total_sol_in = sum(t.get("sol_in", 0) for t in self.pnl_log)
        total_sol_out = sum(t.get("sol_out", 0) for t in self.pnl_log)
        pnl = total_sol_out - total_sol_in

        lines = [
            "📈 *TRADING PERFORMANCE*\n",
            f"Total trades: {len(self.pnl_log)}",
            f"Total in: {total_sol_in:.4f} SOL",
            f"Total out: {total_sol_out:.4f} SOL",
            f"Net PnL: {'🟢' if pnl >= 0 else '🔴'} {pnl:+.4f} SOL",
            "",
            "*Recent Trades:*",
        ]
        for trade in self.pnl_log[-5:]:
            lines.append(
                f"• ${trade.get('symbol')} | "
                f"In: {trade.get('sol_in', 0):.3f} SOL | "
                f"Out: {trade.get('sol_out', 0):.3f} SOL"
            )

        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    # ── Callback handler ───────────────────────────────────────────────────────

    async def handle_callback(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        data = query.data
        if data.startswith("watch:"):
            _, address, symbol = data.split(":", 2)
            pairs = await self.tracker.dex.search_token(symbol)
            if pairs:
                parsed = self.tracker.dex.parse_pair_to_token(pairs[0])
                if parsed:
                    risk_score = await self.tracker.risk_scorer.score_token(
                        parsed, await self.tracker.dex._get_session()
                    )
                    token = PumpToken(**{**parsed, "risk_score": risk_score})
                    self.watchlist[address] = token
                    await query.edit_message_reply_markup(None)
                    await ctx.bot.send_message(
                        chat_id=query.message.chat_id,
                        text=f"✅ Added *${symbol}* to watchlist!",
                        parse_mode=ParseMode.MARKDOWN,
                    )

    # ── Background tasks ───────────────────────────────────────────────────────

    async def auto_signal_monitor(self, app: Application):
        """Background task: poll signals and push high-score ones automatically."""
        await asyncio.sleep(10)  # Startup delay
        while True:
            try:
                signals = await self.signals.collect_all_signals()
                top = [s for s in signals if s.signal_score >= 8]  # Only fire on very high scores

                for sig in top[:3]:
                    matched = await self.tracker.find_tokens_matching_signals(
                        sig.animal_matches, sig.cashtags
                    )
                    if not matched:
                        continue

                    token_dicts = [
                        {
                            "symbol": t.symbol,
                            "liquidity": t.liquidity_usd,
                            "volume": t.volume_24h,
                            "risk_score": t.risk_score,
                            "address": t.address,
                        }
                        for t in matched
                    ]
                    alert = format_signal_alert(
                        source=sig.source,
                        text=sig.text,
                        animal_matches=sig.animal_matches,
                        cashtags=sig.cashtags,
                        signal_score=sig.signal_score,
                        matched_tokens=token_dicts,
                    )

                    buttons = []
                    if matched:
                        buttons = [[
                            InlineKeyboardButton(
                                f"👀 Watch ${matched[0].symbol}",
                                callback_data=f"watch:{matched[0].address}:{matched[0].symbol}"
                            ),
                            InlineKeyboardButton("🔗 Chart", url=matched[0].dex_url),
                        ]]

                    await app.bot.send_message(
                        chat_id=self.chat_id,
                        text=alert,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
                        disable_web_page_preview=True,
                    )

            except Exception as e:
                logger.error(f"Auto monitor error: {e}")

            await asyncio.sleep(self.trend_interval)

    async def auto_launch_scanner(self, app: Application):
        """Background task: scan for new launches and auto-alert."""
        await asyncio.sleep(30)
        while True:
            try:
                tokens = await self.tracker.get_new_launches()
                hot = [
                    t for t in tokens
                    if t.liquidity_usd >= self.min_liquidity
                    and t.risk_score >= 7  # Only push strong ones
                ]

                for token in hot[:2]:
                    card = self.tracker.format_token_card(token)
                    buttons = [[
                        InlineKeyboardButton("👀 Watch",
                                              callback_data=f"watch:{token.address}:{token.symbol}"),
                        InlineKeyboardButton("🔗 Chart", url=token.dex_url),
                    ]]
                    await app.bot.send_message(
                        chat_id=self.chat_id,
                        text=f"🚀 *NEW LAUNCH DETECTED*\n\n{card}",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup(buttons),
                        disable_web_page_preview=True,
                    )

            except Exception as e:
                logger.error(f"Launch scanner error: {e}")

            await asyncio.sleep(self.poll_interval)

    async def cmd_narratives(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show all active narratives with ticker predictions."""
        narratives = self.signals.get_active_narratives()

        if not narratives:
            await update.message.reply_text(
                "🧠 No active narratives yet.\n"
                "Run /signals first to populate the narrative engine."
            )
            return

        # Show summary first
        summary = self.signals.format_narrative_summary()
        await update.message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)

        # Show top 3 confirmed/strong narratives in detail
        confirmed = self.signals.get_confirmed_narratives()
        top = confirmed[:3] if confirmed else narratives[:3]

        for narrative in top:
            alert = self.signals.narrative_detector.format_narrative_alert(narrative)

            # Try to find matching tokens for each narrative's suggested tickers
            matched = await self.tracker.find_tokens_matching_signals(
                {},  # no animal map — use cashtag-style tickers
                narrative.suggested_tickers[:5],
            )

            buttons = []
            if matched:
                buttons = [[
                    InlineKeyboardButton(
                        f"👀 Watch ${matched[0].symbol}",
                        callback_data=f"watch:{matched[0].address}:{matched[0].symbol}"
                    ),
                    InlineKeyboardButton("🔗 Chart", url=matched[0].dex_url),
                ]]

            await update.message.reply_text(
                alert,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
                disable_web_page_preview=True,
            )
            await asyncio.sleep(0.5)

    async def cmd_kols(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show KOL account stats and coverage."""
        total = len(KOL_ACCOUNTS)

        # Count by category
        by_category: Dict[str, int] = {}
        by_weight: Dict[int, int] = {}
        for acc in KOL_ACCOUNTS:
            cat = acc["category"]
            w = acc["weight"]
            by_category[cat] = by_category.get(cat, 0) + 1
            by_weight[w] = by_weight.get(w, 0) + 1

        batch_size = self.signals.nitter.batch_size
        total_batches = (total // batch_size) + 1
        current_batch = self.signals.nitter.current_batch_idx

        lines = [
            f"📊 *KOL COVERAGE — {total} Accounts*\n",
            f"⚡ Priority (weight 9-10): {by_weight.get(9, 0) + by_weight.get(10, 0)} accounts",
            f"   Polled every cycle",
            f"🔄 Batch rotation: {batch_size} accounts/cycle across {total_batches} batches",
            f"📍 Current batch: {current_batch}/{total_batches}",
            f"",
            f"*By Category:*",
        ]
        cat_emojis = {
            "mainstream": "🌍", "crypto_kol": "₿", "solana": "◎",
            "memecoin": "🐸", "defi": "📊", "vc_founder": "💼",
            "news": "📰", "politics": "🏛️", "sports": "🥊",
            "entertainment": "🎬", "wildlife": "🦁", "meme": "😂",
            "tech": "🤖", "global_kol": "🌐", "ecosystem": "⚙️",
            "alpha": "🎯",
        }
        for cat, count in sorted(by_category.items(), key=lambda x: x[1], reverse=True):
            emoji = cat_emojis.get(cat, "•")
            lines.append(f"{emoji} {cat.replace('_', ' ').title()}: {count}")

        lines += [
            f"",
            f"*Top 10 highest-weight accounts:*",
        ]
        top_kols = sorted(KOL_ACCOUNTS, key=lambda x: x["weight"], reverse=True)[:10]
        for acc in top_kols:
            lines.append(f"  ⭐ @{acc['username']} (weight: {acc['weight']}/10, {acc['category']})")

        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    def run(self):
        """Start the bot."""
        app = self.build_app()

        # Register background jobs
        app.job_queue.run_once(
            lambda ctx: asyncio.ensure_future(self.auto_signal_monitor(app)), when=5
        )
        app.job_queue.run_once(
            lambda ctx: asyncio.ensure_future(self.auto_launch_scanner(app)), when=15
        )

        logger.info("🤖 Quant Agent Bot starting...")
        app.run_polling(drop_pending_updates=True)
