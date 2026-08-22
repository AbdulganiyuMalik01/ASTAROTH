import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field

import aiohttp
import feedparser
from pytrends.request import TrendReq

from utils.kol_accounts import KOL_ACCOUNTS
from utils.keywords import (
    NITTER_INSTANCES,
    NEWS_FEEDS,
    extract_keywords,
    extract_hashtags,
    extract_cashtags,
    match_animals_to_tickers,
    score_signal_strength,
)
from signals.narrative import NarrativeDetector, Narrative

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    source: str
    source_type: str
    source_weight: int
    text: str
    url: str
    timestamp: datetime
    keywords: Set[str]
    hashtags: List[str]
    cashtags: List[str]
    animal_matches: Dict[str, List[str]]
    signal_score: int
    narrative: Optional[Narrative] = None
    raw_data: Dict = field(default_factory=dict)


class NitterMonitor:
    """Polls 500+ KOL accounts via Nitter RSS in rotating batches."""

    def __init__(self):
        self.seen_urls: Set[str] = set()
        self.session: Optional[aiohttp.ClientSession] = None
        self.batch_size = 20          # Accounts polled per cycle (rotating)
        self.current_batch_idx = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "Mozilla/5.0 (compatible; QuantAgent/1.0)"},
            )
        return self.session

    def _get_nitter_base(self) -> str:
        return random.choice(NITTER_INSTANCES)

    async def fetch_rss(self, url: str) -> Optional[feedparser.FeedParserDict]:
        try:
            session = await self._get_session()
            async with session.get(url) as resp:
                if resp.status == 200:
                    return feedparser.parse(await resp.text())
        except Exception as e:
            logger.debug(f"RSS fetch failed {url}: {e}")
        return None

    def _next_batch(self) -> List[Dict]:
        """Return next rotating batch of accounts."""
        total = len(KOL_ACCOUNTS)
        start = self.current_batch_idx * self.batch_size
        end = min(start + self.batch_size, total)
        batch = KOL_ACCOUNTS[start:end]
        self.current_batch_idx = (self.current_batch_idx + 1) % ((total // self.batch_size) + 1)
        return batch

    def _priority_accounts(self) -> List[Dict]:
        """Always poll weight≥9 accounts every cycle — they move markets."""
        return [a for a in KOL_ACCOUNTS if a["weight"] >= 9]

    async def poll_account(self, account: Dict, nd: NarrativeDetector) -> List[Signal]:
        username = account["username"]
        weight = account["weight"]
        url = f"{self._get_nitter_base()}/{username}/rss"

        feed = await self.fetch_rss(url)
        if not feed:
            return []

        signals = []
        for entry in feed.entries[:10]:
            entry_url = entry.get("link", "")
            if entry_url in self.seen_urls:
                continue

            try:
                pub_date = datetime(*entry.published_parsed[:6])
                if datetime.utcnow() - pub_date > timedelta(hours=6):
                    continue
            except Exception:
                pub_date = datetime.utcnow()

            text = entry.get("title", "") + " " + entry.get("summary", "")
            keywords = extract_keywords(text)
            hashtags = extract_hashtags(text)
            cashtags = extract_cashtags(text)
            animal_matches = match_animals_to_tickers(keywords)
            narrative = nd.analyse_signal(text, f"@{username}", weight)

            if not (animal_matches or cashtags or narrative or len(keywords) > 5):
                self.seen_urls.add(entry_url)
                continue

            score = score_signal_strength(
                text, source_weight=weight, has_cashtag=bool(cashtags),
                is_trending=narrative is not None and narrative.source_count > 2,
            )
            signals.append(Signal(
                source=f"@{username}", source_type="kol", source_weight=weight,
                text=text[:500], url=entry_url, timestamp=pub_date,
                keywords=keywords, hashtags=hashtags, cashtags=cashtags,
                animal_matches=animal_matches, signal_score=score, narrative=narrative,
            ))
            self.seen_urls.add(entry_url)
        return signals

    async def poll_batch(self, nd: NarrativeDetector) -> List[Signal]:
        priority = self._priority_accounts()
        batch = self._next_batch()

        seen_names: Set[str] = set()
        to_poll: List[Dict] = []
        for acc in priority + batch:
            if acc["username"] not in seen_names:
                seen_names.add(acc["username"])
                to_poll.append(acc)

        total_accounts = len(KOL_ACCOUNTS)
        batch_num = self.current_batch_idx
        total_batches = (total_accounts // self.batch_size) + 1
        logger.info(
            f"📡 Polling {len(to_poll)} accounts | "
            f"{len(priority)} priority always-on | "
            f"batch {batch_num}/{total_batches} | "
            f"{total_accounts} total KOLs"
        )

        results = await asyncio.gather(
            *[self.poll_account(acc, nd) for acc in to_poll],
            return_exceptions=True,
        )
        signals = []
        for r in results:
            if isinstance(r, list):
                signals.extend(r)
        return signals

    async def poll_hashtags(self, nd: NarrativeDetector) -> List[Signal]:
        tags = [
            "solana", "pumpfun", "memecoin", "newtoken", "100x",
            "gem", "alphacall", "solanagem", "memeseason", "degenalpha",
            "pumpfunlaunch", "solanaalpha", "newlisting", "cryptotwitter",
        ]

        async def _poll_tag(tag: str) -> List[Signal]:
            url = f"{self._get_nitter_base()}/search/rss?q=%23{tag}&f=tweets"
            feed = await self.fetch_rss(url)
            if not feed:
                return []
            sigs = []
            for entry in feed.entries[:5]:
                entry_url = entry.get("link", "")
                if entry_url in self.seen_urls:
                    continue
                text = entry.get("title", "") + " " + entry.get("summary", "")
                keywords = extract_keywords(text)
                cashtags = extract_cashtags(text)
                animal_matches = match_animals_to_tickers(keywords)
                narrative = nd.analyse_signal(text, f"#{tag}", 5)
                if not (animal_matches or cashtags or narrative):
                    self.seen_urls.add(entry_url)
                    continue
                score = score_signal_strength(text, source_weight=5, has_cashtag=bool(cashtags))
                sigs.append(Signal(
                    source=f"#{tag}", source_type="hashtag", source_weight=5,
                    text=text[:500], url=entry_url, timestamp=datetime.utcnow(),
                    keywords=keywords, hashtags=[tag], cashtags=cashtags,
                    animal_matches=animal_matches, signal_score=score, narrative=narrative,
                ))
                self.seen_urls.add(entry_url)
            return sigs

        results = await asyncio.gather(*[_poll_tag(t) for t in tags], return_exceptions=True)
        signals = []
        for r in results:
            if isinstance(r, list):
                signals.extend(r)
        return signals


class NewsMonitor:
    def __init__(self):
        self.seen_urls: Set[str] = set()

    async def poll_feed(self, feed_info: Dict, nd: NarrativeDetector) -> List[Signal]:
        try:
            feed = feedparser.parse(feed_info["url"])
        except Exception as e:
            logger.warning(f"Feed failed {feed_info['name']}: {e}")
            return []

        is_crypto = any(k in feed_info["name"].lower() for k in ["coin", "decrypt", "block"])
        signals = []

        for entry in feed.entries[:15]:
            entry_url = entry.get("link", "")
            if entry_url in self.seen_urls:
                continue

            text = entry.get("title", "") + " " + entry.get("summary", "")
            keywords = extract_keywords(text)
            cashtags = extract_cashtags(text)
            animal_matches = match_animals_to_tickers(keywords)
            narrative = nd.analyse_signal(text, feed_info["name"], 6)

            relevant = (
                (is_crypto and (cashtags or animal_matches or narrative)) or
                (not is_crypto and (animal_matches or narrative))
            )
            if not relevant:
                self.seen_urls.add(entry_url)
                continue

            try:
                pub_date = datetime(*entry.published_parsed[:6])
                if datetime.utcnow() - pub_date > timedelta(hours=12):
                    self.seen_urls.add(entry_url)
                    continue
            except Exception:
                pub_date = datetime.utcnow()

            score = score_signal_strength(
                text, source_weight=6 if is_crypto else 5,
                has_cashtag=bool(cashtags),
                is_trending=narrative is not None and narrative.is_confirmed,
            )
            signals.append(Signal(
                source=feed_info["name"],
                source_type="news" if is_crypto else "animal_news",
                source_weight=6, text=text[:500], url=entry_url, timestamp=pub_date,
                keywords=keywords, hashtags=extract_hashtags(text), cashtags=cashtags,
                animal_matches=animal_matches, signal_score=score, narrative=narrative,
            ))
            self.seen_urls.add(entry_url)
        return signals

    async def poll_all(self, nd: NarrativeDetector) -> List[Signal]:
        results = await asyncio.gather(
            *[self.poll_feed(f, nd) for f in NEWS_FEEDS], return_exceptions=True
        )
        signals = []
        for r in results:
            if isinstance(r, list):
                signals.extend(r)
        return signals


class GoogleTrendsMonitor:
    def __init__(self):
        self.pytrends = TrendReq(hl="en-US", tz=0)

    def poll(self, nd: NarrativeDetector) -> List[Signal]:
        signals = []
        try:
            trending_terms = self.pytrends.trending_searches(pn="united_states")[0].tolist()[:20]
            for term in trending_terms:
                keywords = extract_keywords(term)
                animal_matches = match_animals_to_tickers(keywords)
                cashtags = extract_cashtags(term)
                narrative = nd.analyse_signal(term, "Google Trends", 7)
                crypto_kw = {"crypto", "coin", "token", "nft", "solana", "bitcoin", "ethereum"}
                if not (animal_matches or (crypto_kw & keywords) or narrative):
                    continue
                score = score_signal_strength(term, source_weight=7, is_trending=True,
                                               has_cashtag=bool(cashtags))
                signals.append(Signal(
                    source="Google Trends", source_type="trends", source_weight=7,
                    text=f"Trending: '{term}'", url="https://trends.google.com",
                    timestamp=datetime.utcnow(), keywords=keywords, hashtags=[],
                    cashtags=cashtags, animal_matches=animal_matches,
                    signal_score=score, narrative=narrative,
                    raw_data={"trending_term": term},
                ))
        except Exception as e:
            logger.warning(f"Google Trends error: {e}")
        return signals


class SignalAggregator:
    def __init__(self):
        self.nitter = NitterMonitor()
        self.news = NewsMonitor()
        self.trends = GoogleTrendsMonitor()
        self.narrative_detector = NarrativeDetector()
        self.recent_signals: List[Signal] = []
        self.signal_history: List[Signal] = []

    async def collect_all_signals(self) -> List[Signal]:
        logger.info(f"📡 Signal collection | {len(KOL_ACCOUNTS)} KOLs loaded")
        nd = self.narrative_detector

        kol_sigs, tag_sigs, news_sigs = await asyncio.gather(
            self.nitter.poll_batch(nd),
            self.nitter.poll_hashtags(nd),
            self.news.poll_all(nd),
            return_exceptions=True,
        )
        loop = asyncio.get_event_loop()
        trend_sigs = await loop.run_in_executor(None, self.trends.poll, nd)

        all_signals = []
        for r in [kol_sigs, tag_sigs, news_sigs]:
            if isinstance(r, list):
                all_signals.extend(r)
        all_signals.extend(trend_sigs)
        all_signals.sort(key=lambda s: s.signal_score, reverse=True)

        seen_texts: Set[str] = set()
        unique = []
        for sig in all_signals:
            key = sig.text[:80].lower()
            if key not in seen_texts:
                seen_texts.add(key)
                unique.append(sig)

        self.recent_signals = unique
        self.signal_history.extend(unique)
        if len(self.signal_history) > 1000:
            self.signal_history = self.signal_history[-1000:]

        self.narrative_detector.cleanup_old_narratives(24)

        narratives = self.narrative_detector.active_narratives
        logger.info(f"✅ {len(unique)} signals | {len(narratives)} narratives active")
        return unique

    def get_top_signals(self, min_score: int = 6, limit: int = 10) -> List[Signal]:
        return [s for s in self.recent_signals if s.signal_score >= min_score][:limit]

    def get_trending_animals(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for sig in self.recent_signals:
            for animal in sig.animal_matches:
                counts[animal] = counts.get(animal, 0) + sig.signal_score
        return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))

    def get_trending_tickers(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for sig in self.recent_signals:
            for t in sig.cashtags:
                counts[t] = counts.get(t, 0) + sig.signal_score
            for tickers in sig.animal_matches.values():
                for t in tickers:
                    counts[t] = counts.get(t, 0) + 1
        return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True)[:20])

    def get_active_narratives(self):
        return self.narrative_detector.get_active_narratives()

    def get_confirmed_narratives(self):
        return self.narrative_detector.get_confirmed_narratives()

    def format_narrative_summary(self) -> str:
        return self.narrative_detector.format_narrative_summary()
