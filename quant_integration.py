"""
signals/quant_integration.py

Bridges the Signal Intelligence Layer (KOL monitor, narrative detector,
Google Trends, wildlife news) into the existing token_tracker_webhook_v3
architecture.

Improvement #9 — Nitter fallback:
  Nitter instances go down frequently. This version adds:
    1. Instance rotation: tries multiple Nitter mirrors before giving up.
    2. RSS fallback: for KOLs with known RSS feeds, fetches feed directly
       when all Nitter instances fail.
    3. Error tracking per instance: automatically deprioritises broken
       instances within a session.
    4. Graceful degradation: when all Nitter+RSS paths fail, signals from
       that cycle's KOL monitoring are simply skipped (logged, not crashed).

Usage — add ONE line to token_tracker_webhook_v3.py lifespan():
    asyncio.create_task(start_quant_agent(tokens, tokens_lock, send_telegram_message))
"""

import asyncio
import logging
import os
import time
from typing import Dict, Optional, Callable, Awaitable, List
from dataclasses import dataclass, field
from collections import deque, defaultdict

import aiohttp
from solders.pubkey import Pubkey as PublicKey

logger = logging.getLogger(__name__)

# ── Lazy imports ────────────────────────────────────────────────────────────
def _import_signal_stack():
    """Import signal stack lazily so missing deps don't break the base tracker."""
    try:
        from signals.monitor import SignalAggregator
        from signals.narrative import NarrativeDetector
        from utils.keywords import format_signal_alert, ANIMAL_TICKER_MAP
        from utils.kol_accounts import KOL_ACCOUNTS
        return SignalAggregator, NarrativeDetector, format_signal_alert, ANIMAL_TICKER_MAP, KOL_ACCOUNTS
    except ImportError as e:
        logger.error(f"Signal stack import failed: {e}. Install quant_agent deps.")
        return None

# ── Config ───────────────────────────────────────────────────────────────────
QUANT_AGENT_ENABLED       = os.getenv("QUANT_AGENT_ENABLED", "1") == "1"
QUANT_SIGNAL_INTERVAL     = int(os.getenv("QUANT_SIGNAL_INTERVAL", "300"))   # 5 min
QUANT_MIN_SIGNAL_SCORE    = int(os.getenv("QUANT_MIN_SIGNAL_SCORE", "7"))
QUANT_MIN_NARRATIVE_CONF  = int(os.getenv("QUANT_MIN_NARRATIVE_CONF", "6"))
QUANT_AUTO_INJECT_TOKENS  = os.getenv("QUANT_AUTO_INJECT_TOKENS", "1") == "1"
QUANT_MIN_INJECT_LIQ      = float(os.getenv("QUANT_MIN_INJECT_LIQ", "5000"))

# ── Nitter instance pool (#9) ─────────────────────────────────────────────
# Public instances roughly sorted by historical reliability.
# Add/remove instances via NITTER_INSTANCES env var (comma-separated URLs).
_DEFAULT_NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.cz",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
    "https://nitter.unixfox.eu",
    "https://nitter.namazso.eu",
]

NITTER_INSTANCES: List[str] = [
    i.strip() for i in
    os.getenv("NITTER_INSTANCES", ",".join(_DEFAULT_NITTER_INSTANCES)).split(",")
    if i.strip()
]

# Consecutive failure count per instance before it's skipped for this session
NITTER_MAX_FAILS = int(os.getenv("NITTER_MAX_FAILS", "3"))

# RSS fallback: Twitter/X RSS feeds for a subset of high-priority KOLs.
# These are third-party RSS bridges that don't rely on Nitter.
# Override via RSS_KOL_FEEDS env var: "handle=url,handle=url,..."
_DEFAULT_RSS_FEEDS = os.getenv("RSS_KOL_FEEDS", "")
_RSS_KOL_MAP: Dict[str, str] = {}
if _DEFAULT_RSS_FEEDS:
    for item in _DEFAULT_RSS_FEEDS.split(","):
        if "=" in item:
            handle, url = item.split("=", 1)
            _RSS_KOL_MAP[handle.strip().lower()] = url.strip()


# ─────────────────────────────────────────────────────────────────────────────
# Nitter instance manager  (#9)
# ─────────────────────────────────────────────────────────────────────────────

class NitterInstanceManager:
    """
    Rotates through a pool of Nitter instances, tracking failure counts.
    Automatically skips instances that have exceeded NITTER_MAX_FAILS.

    Usage:
        manager = NitterInstanceManager()
        async with manager.get_session() as session:
            url = manager.build_url("elonmusk", "/rss")
            async with session.get(url) as resp:
                ...
            manager.report_success(url)   # or manager.report_failure(url)
    """

    def __init__(self, instances: List[str] = None):
        self._instances = list(instances or NITTER_INSTANCES)
        self._failures: Dict[str, int] = defaultdict(int)
        self._current_idx = 0

    def healthy_instances(self) -> List[str]:
        return [i for i in self._instances if self._failures[i] < NITTER_MAX_FAILS]

    def report_success(self, base_url: str):
        if base_url in self._failures:
            self._failures[base_url] = max(0, self._failures[base_url] - 1)

    def report_failure(self, base_url: str):
        self._failures[base_url] += 1
        remaining = len(self.healthy_instances())
        if remaining == 0:
            logger.warning(
                "[Nitter] All instances have exceeded failure threshold — "
                "resetting counters to allow retry."
            )
            self._failures.clear()
        else:
            logger.debug(f"[Nitter] {base_url} failure #{self._failures[base_url]} "
                         f"({remaining} healthy instances left)")

    def next_instance(self) -> Optional[str]:
        healthy = self.healthy_instances()
        if not healthy:
            return None
        instance = healthy[self._current_idx % len(healthy)]
        self._current_idx += 1
        return instance

    def build_url(self, handle: str, path: str = "", base: Optional[str] = None) -> str:
        """Build a Nitter URL for a KOL handle."""
        base = base or self.next_instance()
        if not base:
            raise RuntimeError("No healthy Nitter instances available")
        return f"{base.rstrip('/')}/{handle.lstrip('@')}{path}"

    async def fetch_kol_rss(
        self,
        session: aiohttp.ClientSession,
        handle: str,
        max_items: int = 10,
    ) -> List[dict]:
        """
        Fetch recent posts for a KOL.

        Tries Nitter instances in rotation first; falls back to any registered
        RSS feed URL for this handle if all Nitter instances fail.

        Returns a list of simple dicts: {"text": str, "url": str, "ts": float}
        """
        # ── Nitter rotation ────────────────────────────────────────────────
        healthy = self.healthy_instances()
        for base in healthy:
            try:
                url = f"{base.rstrip('/')}/{handle.lstrip('@')}/rss"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        self.report_success(base)
                        return _parse_rss_items(text, max_items)
                    else:
                        self.report_failure(base)
            except Exception as e:
                logger.debug(f"[Nitter] {base} → {handle}: {e}")
                self.report_failure(base)

        # ── RSS fallback (#9) ──────────────────────────────────────────────
        rss_url = _RSS_KOL_MAP.get(handle.lower().lstrip("@"))
        if rss_url:
            try:
                async with session.get(rss_url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        logger.info(f"[Nitter→RSS] Used RSS fallback for @{handle}")
                        return _parse_rss_items(text, max_items)
            except Exception as e:
                logger.debug(f"[RSS fallback] {handle}: {e}")

        return []


def _parse_rss_items(xml_text: str, max_items: int = 10) -> List[dict]:
    """
    Parse an RSS/Atom feed XML string into a flat list of post dicts.
    Uses stdlib xml.etree so no extra dep needed.
    """
    import xml.etree.ElementTree as ET

    items = []
    try:
        root = ET.fromstring(xml_text)
        ns = ""
        # Support both RSS <item> and Atom <entry>
        for tag in ("item", "{http://www.w3.org/2005/Atom}entry"):
            for elem in root.iter(tag):
                title = elem.findtext("title") or elem.findtext("{http://www.w3.org/2005/Atom}title") or ""
                link  = elem.findtext("link")  or elem.findtext("{http://www.w3.org/2005/Atom}link") or ""
                desc  = elem.findtext("description") or elem.findtext("{http://www.w3.org/2005/Atom}content") or ""
                pub   = elem.findtext("pubDate") or ""

                # Best-effort timestamp parsing
                ts = time.time()
                if pub:
                    try:
                        from email.utils import parsedate_to_datetime
                        ts = parsedate_to_datetime(pub).timestamp()
                    except Exception:
                        pass

                text = (desc or title).strip()
                # Strip simple HTML tags
                import re
                text = re.sub(r"<[^>]+>", " ", text).strip()

                if text:
                    items.append({"text": text[:500], "url": link, "ts": ts})
                if len(items) >= max_items:
                    break
            if items:
                break
    except Exception as e:
        logger.debug(f"[RSS parse error]: {e}")
    return items


# ─────────────────────────────────────────────────────────────────────────────
# Existing helper classes (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MatchedToken:
    address: str
    symbol: str
    name: str
    liquidity_usd: float
    volume_24h: float
    market_cap: float
    dex_url: str
    pump_url: str
    risk_score: int = 5
    matched_keywords: list = field(default_factory=list)


async def search_dexscreener(query: str, session: aiohttp.ClientSession) -> list:
    try:
        url = f"https://api.dexscreener.com/latest/dex/search?q={query}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            pairs = data.get("pairs", []) or []
            return [p for p in pairs if p.get("chainId", "") == "solana"][:5]
    except Exception as e:
        logger.debug(f"DexScreener search error for '{query}': {e}")
        return []


def parse_pair(pair: dict) -> Optional[MatchedToken]:
    try:
        base    = pair.get("baseToken", {})
        liq     = pair.get("liquidity", {})
        vol     = pair.get("volume", {})
        address = base.get("address", "")
        if not address:
            return None
        return MatchedToken(
            address=address,
            symbol=base.get("symbol", "???").upper(),
            name=base.get("name", "Unknown"),
            liquidity_usd=float(liq.get("usd", 0) or 0),
            volume_24h=float(vol.get("h24", 0) or 0),
            market_cap=float(pair.get("fdv", 0) or 0),
            dex_url=pair.get("url", f"https://dexscreener.com/solana/{address}"),
            pump_url=f"https://pump.fun/{address}",
        )
    except Exception:
        return None


def quick_risk_score(token: MatchedToken) -> int:
    score = 5
    if token.liquidity_usd >= 50000:    score += 2
    elif token.liquidity_usd >= 10000:  score += 1
    elif token.liquidity_usd < 3000:    score -= 2
    if token.volume_24h >= 100000:      score += 1
    elif token.volume_24h < 1000:       score -= 1
    if token.market_cap >= 100000:      score += 1
    elif token.market_cap < 5000:       score -= 1
    return max(1, min(10, score))


async def find_tokens_for_signal(
    animal_matches: dict,
    cashtags: list,
    suggested_tickers: list,
    session: aiohttp.ClientSession,
) -> list:
    found: dict = {}
    search_terms: list = []
    search_terms.extend(cashtags[:5])
    search_terms.extend(suggested_tickers[:5])
    for tickers in list(animal_matches.values())[:4]:
        search_terms.extend(tickers[:2])
    search_terms.extend(list(animal_matches.keys())[:4])

    seen_terms: set = set()
    unique_terms: list = []
    for t in search_terms:
        tl = t.lower()
        if tl not in seen_terms:
            seen_terms.add(tl)
            unique_terms.append(t)

    for term in unique_terms[:12]:
        pairs = await search_dexscreener(term, session)
        for pair in pairs:
            token = parse_pair(pair)
            if token and token.address not in found:
                token.risk_score = quick_risk_score(token)
                token.matched_keywords.append(term)
                found[token.address] = token
        await asyncio.sleep(0.15)

    return sorted(found.values(), key=lambda t: t.liquidity_usd, reverse=True)[:8]


def format_signal_telegram_alert(
    source, text, animal_matches, cashtags,
    signal_score, narrative_title, narrative_tickers,
    matched_tokens, source_type="kol",
) -> str:
    emoji_map = {
        "kol": "🐦", "hashtag": "#️⃣", "news": "📰",
        "animal_news": "🐾", "trends": "📈",
    }
    emoji    = emoji_map.get(source_type, "📡")
    strength = "🔥" if signal_score >= 9 else "⚡" if signal_score >= 7 else "👀"

    lines = [
        f"{strength}{strength}{strength}",
        "",
        f"{emoji} <b>QUANT SIGNAL — {source}</b>",
        "",
        f"💬 <i>{text[:200]}{'...' if len(text) > 200 else ''}</i>",
        "",
        f"📊 Signal Score: {'⭐' * min(signal_score, 10)} ({signal_score}/10)",
    ]
    if narrative_title:
        lines.append(f"🧠 Narrative: <b>{narrative_title}</b>")
        if narrative_tickers:
            lines.append(f"🎯 Expected tickers: {' | '.join(['$'+t for t in narrative_tickers[:6]])}")
    if cashtags:
        lines.append(f"💰 Mentioned: {' '.join(['$'+t for t in cashtags[:5]])}")
    if animal_matches:
        animals_str = " | ".join([
            f"{a.title()} → {', '.join(['$'+t for t in tickers[:3]])}"
            for a, tickers in list(animal_matches.items())[:3]
        ])
        lines.append(f"🐾 Animal map: {animals_str}")
    if matched_tokens:
        lines += ["", "🎯 <b>Matching tokens on-chain:</b>"]
        for i, token in enumerate(matched_tokens[:4], 1):
            risk_icon = "🟢" if token.risk_score >= 7 else "🟡" if token.risk_score >= 5 else "🔴"
            lines.append(
                f"{i}. {risk_icon} <b>${token.symbol}</b> — {token.name}\n"
                f"   Liq: ${token.liquidity_usd:,.0f} | Vol: ${token.volume_24h:,.0f} | Risk: {token.risk_score}/10\n"
                f"   <a href='{token.dex_url}'>DexScreener</a> | <a href='{token.pump_url}'>Pump.fun</a>\n"
                f"   <code>{token.address}</code>"
            )
    else:
        lines += ["", "🔍 No matching tokens found yet — monitoring for launches..."]
    lines += ["", "⚡ <i>AlphaDegen Quant Agent</i>"]
    return "\n".join(lines)


def format_narrative_telegram_alert(narrative) -> str:
    type_emojis = {
        "animal_viral":       "🐾",
        "political_moment":   "🏛",
        "sports_moment":      "🥊",
        "tech_ai_narrative":  "🤖",
        "celebrity_moment":   "⭐",
        "meme_moment":        "😂",
        "geopolitical_event": "🌍",
        "crypto_narrative":   "📈",
        "conservation_moment":"🌿",
        "space_moment":       "🚀",
        "food_viral":         "🍔",
    }
    emoji    = type_emojis.get(narrative.narrative_type, "🔥")
    confirmed= "✅ CONFIRMED" if narrative.is_confirmed else "🔍 EMERGING"
    tickers  = " | ".join([f"${t}" for t in narrative.suggested_tickers[:6]])
    lines = [
        "🧠🧠🧠",
        "",
        f"{emoji} <b>NARRATIVE DETECTED — {confirmed}</b>",
        "",
        f"📖 <b>{narrative.title}</b>",
        f"🏷 Type: {narrative.narrative_type.replace('_', ' ').title()}",
        f"📡 Sources: {narrative.source_count} ({', '.join(narrative.sources[:3])})",
        f"🔑 Keywords: {', '.join(narrative.keywords_matched[:5])}",
        f"💪 Confidence: {'⭐' * narrative.confidence} ({narrative.confidence}/10)",
        "",
        "💰 <b>Likely tickers to watch:</b>",
        tickers,
        "",
    ]
    if narrative.raw_signals:
        lines.append(f"💬 <i>{narrative.raw_signals[0][:150]}...</i>")
        lines.append("")
    lines.append("⚡ <i>AlphaDegen Quant Agent — Narrative Engine</i>")
    return "\n".join(lines)


def format_trend_digest(
    trending_animals, trending_tickers, source_counts,
    total_signals, kol_count,
) -> str:
    lines = [
        "📡 <b>QUANT AGENT — TREND RADAR</b>",
        "",
        f"Monitoring <b>{kol_count} KOLs</b> + news + Google Trends + wildlife",
        f"Signals collected: <b>{total_signals}</b>",
        "",
    ]
    if trending_animals:
        lines.append("🐾 <b>Trending Animals:</b>")
        for i, (animal, score) in enumerate(list(trending_animals.items())[:6], 1):
            lines.append(f"  {i}. <b>{animal.title()}</b> (score: {score})")
        lines.append("")
    if trending_tickers:
        lines.append("💰 <b>Trending Tickers Mentioned:</b>")
        for i, (ticker, count) in enumerate(list(trending_tickers.items())[:8], 1):
            lines.append(f"  {i}. <code>${ticker}</code> — {count} mentions")
        lines.append("")
    src_emoji = {"kol": "🐦", "hashtag": "#️⃣", "news": "📰", "animal_news": "🐾", "trends": "📈"}
    if source_counts:
        lines.append("📡 <b>Active Sources:</b>")
        for stype, count in source_counts.items():
            label = src_emoji.get(stype, "•") + " " + stype.replace("_", " ").title()
            lines.append(f"  {label}: {count} signals")
    lines += ["", "⚡ <i>AlphaDegen Quant Agent</i>"]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Main background task  (with Nitter fallback wired in, #9)
# ─────────────────────────────────────────────────────────────────────────────

# Global instance manager — shared so failure counters persist across cycles
_nitter_manager = NitterInstanceManager()


async def start_quant_agent(
    tokens: dict,
    tokens_lock: asyncio.Lock,
    send_telegram_message: Callable[..., Awaitable],
    TokenInfo_cls=None,
):
    """
    Main quant agent loop.

    Nitter fallback (#9):
      - Uses NitterInstanceManager for instance rotation + failure tracking.
      - Falls back to RSS feeds registered in RSS_KOL_FEEDS env var.
      - Gracefully skips KOL monitoring for a cycle if all paths fail,
        without crashing the agent.

    Drop into token_tracker_webhook_v3.py lifespan():
        asyncio.create_task(start_quant_agent(tokens, tokens_lock, send_telegram_message))
    """
    if not QUANT_AGENT_ENABLED:
        logger.info("🤖 Quant Agent disabled (QUANT_AGENT_ENABLED=0)")
        return

    imports = _import_signal_stack()
    if not imports:
        logger.error("🤖 Quant Agent: signal stack unavailable, shutting down")
        return

    SignalAggregator, NarrativeDetector, format_signal_alert, ANIMAL_TICKER_MAP, KOL_ACCOUNTS = imports

    # Patch the aggregator to use our instance manager for Nitter calls
    aggregator  = SignalAggregator()
    _patch_aggregator_nitter(aggregator)

    kol_count = len(KOL_ACCOUNTS)
    logger.info(
        f"🤖 Quant Agent started | {kol_count} KOLs | "
        f"{len(_nitter_manager.healthy_instances())} Nitter instances | "
        f"interval: {QUANT_SIGNAL_INTERVAL}s"
    )

    await send_telegram_message(
        f"🤖 <b>Quant Agent Online</b>\n\n"
        f"Monitoring <b>{kol_count} KOL accounts</b>\n"
        f"Sources: X/Twitter · News RSS · Google Trends · Wildlife\n"
        f"Nitter: <b>{len(_nitter_manager.healthy_instances())} instances</b> "
        f"(+ RSS fallback where configured)\n"
        f"Narrative engine: <b>Active</b>\n"
        f"Signal threshold: {QUANT_MIN_SIGNAL_SCORE}/10\n\n"
        f"⚡ <i>AlphaDegen Quant Agent</i>",
        alert_type="quant_startup"
    )

    consecutive_errors = 0
    cycle = 0

    while True:
        try:
            cycle += 1
            logger.info(
                f"🤖 Quant Agent cycle #{cycle} | "
                f"Nitter healthy: {len(_nitter_manager.healthy_instances())}/{len(NITTER_INSTANCES)}"
            )

            # Collect all signals — Nitter failures are handled inside aggregator
            all_signals = await aggregator.collect_all_signals()
            top_signals = aggregator.get_top_signals(min_score=QUANT_MIN_SIGNAL_SCORE, limit=5)
            trending_animals = aggregator.get_trending_animals()
            trending_tickers = aggregator.get_trending_tickers()
            confirmed_narratives = aggregator.get_confirmed_narratives()

            logger.info(
                f"🤖 Cycle #{cycle}: {len(all_signals)} signals | "
                f"{len(top_signals)} top | {len(confirmed_narratives)} confirmed narratives"
            )

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:

                for sig in top_signals[:3]:
                    try:
                        matched = await find_tokens_for_signal(
                            sig.animal_matches, sig.cashtags, [], session,
                        )
                        if QUANT_AUTO_INJECT_TOKENS and TokenInfo_cls:
                            await _inject_tokens(matched, tokens, tokens_lock, TokenInfo_cls)

                        narrative = sig.narrative
                        alert = format_signal_telegram_alert(
                            source=sig.source, text=sig.text,
                            animal_matches=sig.animal_matches, cashtags=sig.cashtags,
                            signal_score=sig.signal_score,
                            narrative_title=narrative.title if narrative else None,
                            narrative_tickers=narrative.suggested_tickers if narrative else [],
                            matched_tokens=matched, source_type=sig.source_type,
                        )
                        await send_telegram_message(alert, alert_type="quant_signal")
                        await asyncio.sleep(1.5)
                    except Exception as e:
                        logger.error(f"Signal processing error: {e}")

                for narrative in confirmed_narratives[:2]:
                    try:
                        matched = await find_tokens_for_signal(
                            {}, narrative.suggested_tickers[:5], narrative.suggested_tickers, session,
                        )
                        if QUANT_AUTO_INJECT_TOKENS and TokenInfo_cls:
                            await _inject_tokens(matched, tokens, tokens_lock, TokenInfo_cls)

                        alert = format_narrative_telegram_alert(narrative)
                        if matched:
                            alert += "\n\n🎯 <b>Matching tokens:</b>"
                            for t in matched[:3]:
                                risk_icon = "🟢" if t.risk_score >= 7 else "🟡" if t.risk_score >= 5 else "🔴"
                                alert += (
                                    f"\n{risk_icon} <b>${t.symbol}</b> — Liq: ${t.liquidity_usd:,.0f} | "
                                    f"Risk: {t.risk_score}/10\n"
                                    f"<a href='{t.dex_url}'>DexScreener</a> | "
                                    f"<code>{t.address}</code>"
                                )
                        await send_telegram_message(alert, alert_type="quant_narrative")
                        await asyncio.sleep(1.5)
                    except Exception as e:
                        logger.error(f"Narrative alert error: {e}")

            # Trend digest every 3 cycles (~15 min)
            if cycle % 3 == 0:
                source_counts: dict = {}
                for sig in aggregator.recent_signals:
                    source_counts[sig.source_type] = source_counts.get(sig.source_type, 0) + 1
                digest = format_trend_digest(
                    trending_animals=trending_animals, trending_tickers=trending_tickers,
                    source_counts=source_counts, total_signals=len(all_signals),
                    kol_count=kol_count,
                )
                await send_telegram_message(digest, alert_type="quant_trends")

            consecutive_errors = 0

        except Exception as e:
            consecutive_errors += 1
            logger.error(f"🤖 Quant Agent cycle error (#{consecutive_errors}): {e}")
            backoff = min(60 * consecutive_errors, 600)
            logger.info(f"🤖 Backing off {backoff}s")
            await asyncio.sleep(backoff)
            continue

        await asyncio.sleep(QUANT_SIGNAL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _inject_tokens(matched, tokens, tokens_lock, TokenInfo_cls):
    """Inject DexScreener-matched tokens into v3's token registry."""
    for token in matched:
        if token.liquidity_usd >= QUANT_MIN_INJECT_LIQ:
            async with tokens_lock:
                if token.address not in tokens:
                    try:
                        tokens[token.address] = TokenInfo_cls(
                            mint=PublicKey.from_string(token.address),
                            launchpad="Pump.fun",
                            created_at=time.time(),
                            name=token.name,
                            symbol=token.symbol,
                            market_cap=token.market_cap,
                            volume_sol=token.volume_24h / 180.0,
                        )
                        logger.info(
                            f"🤖 Injected ${token.symbol} "
                            f"({token.address[:12]}…) into v3 tracker"
                        )
                    except Exception as e:
                        logger.debug(f"Token inject error: {e}")


def _patch_aggregator_nitter(aggregator):
    """
    Monkey-patch the SignalAggregator's Nitter fetch method to use our
    instance manager with rotation + fallback.

    If SignalAggregator exposes a `nitter_fetch` or `_fetch_kol` method,
    we replace it. If it doesn't (API changed), we log a warning and continue
    without the patch — the agent will still work, just without rotation.
    """
    try:
        original_monitor = getattr(aggregator, "nitter_monitor", None)
        if original_monitor is None:
            return  # Can't patch — no known attribute

        # If the monitor has a fetch_user method, wrap it
        if hasattr(original_monitor, "fetch_user"):
            original_fetch = original_monitor.fetch_user

            async def _patched_fetch(handle: str, session, **kwargs):
                items = await _nitter_manager.fetch_kol_rss(session, handle)
                if items:
                    return items
                # Fallback to original implementation if our method returns nothing
                return await original_fetch(handle, session, **kwargs)

            original_monitor.fetch_user = _patched_fetch
            logger.info("[Nitter] Instance manager patched into SignalAggregator")
        else:
            logger.debug("[Nitter] Could not patch fetch_user — proceeding without rotation")

    except Exception as e:
        logger.debug(f"[Nitter] Patch attempt failed (non-critical): {e}")
