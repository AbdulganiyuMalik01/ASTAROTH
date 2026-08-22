import re
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ── Narrative category definitions ────────────────────────────────────────────

NARRATIVE_TEMPLATES: Dict[str, Dict] = {

    # ── Animal viral moments ──────────────────────────────────────────────────
    "animal_viral": {
        "description": "A specific animal goes viral (rescue, zoo, video)",
        "trigger_keywords": [
            "rescued", "orphaned", "saved", "adopted", "endangered",
            "zoo", "sanctuary", "viral animal", "cute", "baby animal",
            "wildlife", "rare", "discovered", "first ever", "world record",
        ],
        "narrative_strength": 9,
        "ticker_pattern": "ANIMAL_NAME",
        "examples": ["$PNUT (peanut squirrel)", "$MOODENG (baby hippo)", "$PUNCH (baby seal)"],
        "how_to_spot": "Animal name appears in multiple sources + emotional framing",
        "time_to_ticker": "1-6 hours after viral moment",
    },

    # ── Political moment ──────────────────────────────────────────────────────
    "political_moment": {
        "description": "Political event, election, policy, politician does something",
        "trigger_keywords": [
            "president", "election", "senate", "congress", "bill", "law",
            "executive order", "impeach", "resign", "scandal", "debate",
            "crypto bill", "regulation", "sec", "ban", "approve",
        ],
        "narrative_strength": 8,
        "ticker_pattern": "PERSON_NAME or EVENT_NAME",
        "examples": ["$TRUMP", "$KAMALA", "$DOGE (dept of gov efficiency)", "$BASED"],
        "how_to_spot": "High-weight political account + policy/election keywords",
        "time_to_ticker": "Minutes to 2 hours",
    },

    # ── Sports event ──────────────────────────────────────────────────────────
    "sports_moment": {
        "description": "Big sports event, fight, championship, athlete controversy",
        "trigger_keywords": [
            "knockout", "champion", "fight", "vs", "championship", "finals",
            "world cup", "superbowl", "title", "goat", "record", "retire",
            "transfer", "signed", "debut", "comeback",
        ],
        "narrative_strength": 7,
        "ticker_pattern": "ATHLETE_NAME or TEAM_NAME",
        "examples": ["$TYSON", "$FURY", "$GOAT", "$LAKERS", "$BULLS"],
        "how_to_spot": "Sports account + athlete name + hype keywords",
        "time_to_ticker": "During or after the event",
    },

    # ── Tech/AI narrative ─────────────────────────────────────────────────────
    "tech_ai_narrative": {
        "description": "New AI model, tech product, breakthrough announcement",
        "trigger_keywords": [
            "launches", "releases", "new model", "breakthrough", "agi",
            "chatgpt", "gpt", "claude", "gemini", "llm", "robot",
            "self-driving", "autopilot", "humanoid", "singularity",
        ],
        "narrative_strength": 7,
        "ticker_pattern": "PRODUCT_NAME or CONCEPT",
        "examples": ["$GPT", "$GROK", "$AGI", "$ROBO", "$SORA"],
        "how_to_spot": "Tech account + product name + launch keywords",
        "time_to_ticker": "Same day as announcement",
    },

    # ── Celebrity moment ──────────────────────────────────────────────────────
    "celebrity_moment": {
        "description": "Celebrity scandal, tweet, controversy, or endorsement",
        "trigger_keywords": [
            "breaks internet", "viral", "trending", "cancelled", "beef",
            "feud", "collab", "drops", "new album", "tour", "married",
            "divorced", "arrested", "apology", "comeback", "exposed",
        ],
        "narrative_strength": 7,
        "ticker_pattern": "CELEB_NAME or REFERENCE",
        "examples": ["$TAYLOR", "$KANYE", "$HAWK (Hawk Tuah girl)"],
        "how_to_spot": "Mainstream influencer + viral moment keywords",
        "time_to_ticker": "1-4 hours after trending",
    },

    # ── Meme/internet moment ──────────────────────────────────────────────────
    "meme_moment": {
        "description": "A meme format, phrase, or internet moment goes viral",
        "trigger_keywords": [
            "meme", "twitter moment", "reddit", "tiktok viral", "goes viral",
            "breaks internet", "everyone is talking", "trending worldwide",
            "no context", "plot twist", "npc", "sigma", "based", "ratio",
            "touch grass", "skill issue", "cope", "slay",
        ],
        "narrative_strength": 8,
        "ticker_pattern": "MEME_WORD or PHRASE",
        "examples": ["$HARAMBE", "$HAWK", "$NPC", "$SIGMA", "$BASED"],
        "how_to_spot": "Meme account + phrase appearing across multiple sources",
        "time_to_ticker": "Same day, sometimes within hours",
    },

    # ── Geopolitical/world event ──────────────────────────────────────────────
    "geopolitical_event": {
        "description": "War, conflict, peace deal, sanctions, country event",
        "trigger_keywords": [
            "war", "ceasefire", "peace deal", "sanctions", "invasion",
            "conflict", "nato", "un", "nuclear", "treaty", "alliance",
            "tariff", "trade war", "embargo", "missile", "attack",
        ],
        "narrative_strength": 6,
        "ticker_pattern": "COUNTRY_NAME or EVENT_CONCEPT",
        "examples": ["$UKRAINE", "$PEACE", "$WAR", "$TARIFF", "$TRUMP"],
        "how_to_spot": "World event accounts + conflict/policy keywords",
        "time_to_ticker": "Hours to 1 day",
    },

    # ── Market/crypto narrative ───────────────────────────────────────────────
    "crypto_narrative": {
        "description": "A new crypto sector narrative emerges or rotates",
        "trigger_keywords": [
            "narrative", "season", "rotation", "meta", "ai coins", "depin",
            "rwa", "desci", "defi summer", "nft season", "meme season",
            "layer 2", "modular", "restaking", "points", "airdrop",
        ],
        "narrative_strength": 8,
        "ticker_pattern": "NARRATIVE_CONCEPT",
        "examples": ["$AI", "$DEPIN", "$RWA", "$DESCI", "$REFI"],
        "how_to_spot": "Multiple crypto KOLs using same terminology",
        "time_to_ticker": "Days to weeks (slower but bigger)",
    },

    # ── Environmental/conservation ────────────────────────────────────────────
    "conservation_moment": {
        "description": "Species added to endangered list, conservation win/loss",
        "trigger_keywords": [
            "endangered", "extinct", "protected", "conservation", "species",
            "habitat", "poaching", "hunting ban", "wildlife refuge",
            "population decline", "critically endangered", "red list",
        ],
        "narrative_strength": 8,
        "ticker_pattern": "SPECIES_NAME",
        "examples": ["$PNUT type plays — squirrel, seal, hippo, penguin"],
        "how_to_spot": "Wildlife account + specific species name + emotional story",
        "time_to_ticker": "12-48 hours (needs time to go mainstream)",
    },

    # ── Space/exploration ─────────────────────────────────────────────────────
    "space_moment": {
        "description": "SpaceX launch, NASA discovery, space news",
        "trigger_keywords": [
            "launch", "mars", "moon", "starship", "rocket", "astronaut",
            "space station", "discovery", "alien", "ufo", "uap",
            "exoplanet", "black hole", "supernova",
        ],
        "narrative_strength": 7,
        "ticker_pattern": "SPACE_CONCEPT or MISSION_NAME",
        "examples": ["$MARS", "$MOON", "$STARSHIP", "$UAP", "$ALIEN"],
        "how_to_spot": "SpaceX/NASA + mission keywords + Elon involvement",
        "time_to_ticker": "Same day for launches",
    },

    # ── Food/drink viral moment ───────────────────────────────────────────────
    "food_viral": {
        "description": "A food trend, restaurant chain controversy, or viral food moment",
        "trigger_keywords": [
            "wendy's", "mcdonald's", "taco bell", "viral food", "food trend",
            "recipe", "restaurant", "chef", "mukbang", "food challenge",
        ],
        "narrative_strength": 5,
        "ticker_pattern": "FOOD_NAME or BRAND_NAME",
        "examples": ["$WENDYS", "$GRIMACE", "$MCRIB"],
        "how_to_spot": "Mainstream account + food name + viral engagement",
        "time_to_ticker": "1-3 days",
    },
}


# ── Ticker suggestion engine ──────────────────────────────────────────────────

# Maps common words/concepts → likely pump.fun ticker format
TICKER_GENERATION_RULES: Dict[str, str] = {
    # Name shortening
    "donald": "DONALD",
    "trump": "TRUMP",
    "elon": "ELON",
    "kamala": "KAMALA",
    "taylor": "TAYLOR",
    "kanye": "YE",
    "obama": "OBAMA",
    "biden": "BIDEN",
    "musk": "MUSK",
    "bezos": "BEZOS",
    "beast": "BEAST",

    # Tech
    "openai": "OPENAI",
    "chatgpt": "GPT",
    "grok": "GROK",
    "gemini": "GEMINI",
    "sora": "SORA",
    "neuralink": "NEURA",
    "starship": "STARSHIP",
    "spacex": "SPACEX",

    # Concepts
    "artificial intelligence": "AI",
    "department of government efficiency": "DOGE",
    "make america great again": "MAGA",
    "to the moon": "MOON",
    "world war": "WAR",
    "ceasefire": "PEACE",
}


@dataclass
class Narrative:
    """A detected narrative with ticker prediction."""
    narrative_type: str
    title: str
    description: str
    sources: List[str]
    keywords_matched: List[str]
    suggested_tickers: List[str]
    confidence: int          # 1-10
    strength: int            # 1-10 (narrative_strength from template)
    first_detected: datetime
    source_count: int        # How many sources picked this up
    is_confirmed: bool       # Multiple sources = confirmed
    raw_signals: List[str] = field(default_factory=list)


class NarrativeDetector:
    """
    Analyses signals across all sources to identify emerging narratives
    and predict what tickers they'll create on pump.fun.
    """

    def __init__(self):
        self.active_narratives: Dict[str, Narrative] = {}
        self.narrative_history: List[Narrative] = []
        self.keyword_hits: Dict[str, List[str]] = {}  # keyword → [source1, source2...]

    def analyse_signal(self, text: str, source: str, source_weight: int) -> Optional[Narrative]:
        """
        Analyse a single signal text against all narrative templates.
        Returns a Narrative if one is detected.
        """
        text_lower = text.lower()
        words = set(re.findall(r'\b\w+\b', text_lower))

        best_match = None
        best_score = 0

        for narrative_type, template in NARRATIVE_TEMPLATES.items():
            triggers = template["trigger_keywords"]
            hits = [kw for kw in triggers if kw in text_lower]

            if not hits:
                continue

            score = len(hits) * source_weight
            if score > best_score:
                best_score = score
                best_match = (narrative_type, template, hits)

        if not best_match or best_score < 5:
            return None

        narrative_type, template, hits = best_match

        # Generate ticker suggestions
        suggested_tickers = self._suggest_tickers(text, narrative_type, hits)

        # Check if this narrative already exists (update it)
        narrative_key = f"{narrative_type}_{':'.join(sorted(hits[:3]))}"

        if narrative_key in self.active_narratives:
            existing = self.active_narratives[narrative_key]
            existing.source_count += 1
            existing.sources.append(source)
            existing.raw_signals.append(text[:200])
            existing.is_confirmed = existing.source_count >= 3
            existing.confidence = min(10, existing.confidence + 1)
            # Add any new ticker suggestions
            for ticker in suggested_tickers:
                if ticker not in existing.suggested_tickers:
                    existing.suggested_tickers.append(ticker)
            return existing

        # New narrative
        narrative = Narrative(
            narrative_type=narrative_type,
            title=self._generate_narrative_title(text, narrative_type, hits),
            description=template["description"],
            sources=[source],
            keywords_matched=hits,
            suggested_tickers=suggested_tickers,
            confidence=min(10, source_weight),
            strength=template["narrative_strength"],
            first_detected=datetime.utcnow(),
            source_count=1,
            is_confirmed=False,
            raw_signals=[text[:200]],
        )

        self.active_narratives[narrative_key] = narrative
        return narrative

    def _generate_narrative_title(self, text: str, narrative_type: str, hits: List[str]) -> str:
        """Generate a human-readable narrative title."""
        text_lower = text.lower()

        # Extract proper nouns (capitalized words)
        proper_nouns = re.findall(r'\b[A-Z][a-z]{2,}\b', text)

        if narrative_type == "animal_viral" and proper_nouns:
            return f"Viral Animal: {proper_nouns[0]}"
        elif narrative_type == "political_moment" and proper_nouns:
            return f"Political: {' '.join(proper_nouns[:2])}"
        elif narrative_type == "sports_moment" and proper_nouns:
            return f"Sports: {' '.join(proper_nouns[:2])}"
        elif narrative_type == "tech_ai_narrative":
            return f"Tech/AI: {hits[0].title()}"
        elif narrative_type == "celebrity_moment" and proper_nouns:
            return f"Celebrity: {proper_nouns[0]}"
        elif narrative_type == "meme_moment":
            return f"Meme: '{hits[0].title()}'"
        elif narrative_type == "geopolitical_event":
            return f"World Event: {hits[0].title()}"
        elif narrative_type == "crypto_narrative":
            return f"Crypto Narrative: {hits[0].upper()}"
        elif narrative_type == "conservation_moment":
            return f"Conservation: {proper_nouns[0] if proper_nouns else hits[0].title()}"
        elif narrative_type == "space_moment":
            return f"Space: {hits[0].title()}"
        else:
            return f"{narrative_type.replace('_', ' ').title()}: {hits[0].title()}"

    def _suggest_tickers(self, text: str, narrative_type: str, hits: List[str]) -> List[str]:
        """
        Generate likely pump.fun ticker suggestions from the narrative.
        """
        suggestions: List[str] = []
        text_lower = text.lower()

        # Check known ticker rules
        for phrase, ticker in TICKER_GENERATION_RULES.items():
            if phrase in text_lower:
                suggestions.append(ticker)

        # Extract proper nouns → convert to ticker format
        proper_nouns = re.findall(r'\b[A-Z][a-z]{2,15}\b', text)
        for noun in proper_nouns[:5]:
            ticker = noun.upper()
            if 3 <= len(ticker) <= 8:
                suggestions.append(ticker)
            # Common shortenings
            if len(noun) > 6:
                suggestions.append(noun[:4].upper())
                suggestions.append(noun[:5].upper())

        # Narrative-specific logic
        if narrative_type == "animal_viral":
            # Animal name → ticker  (already in ANIMAL_TICKER_MAP in keywords.py)
            for word in re.findall(r'\b\w{3,}\b', text_lower):
                if word in ["squirrel", "hippo", "penguin", "seal", "bear", "fox"]:
                    suggestions.extend([word.upper(), word[:4].upper()])

        elif narrative_type == "political_moment":
            # DOGE (dept of gov efficiency) pattern
            if "efficiency" in text_lower or "government" in text_lower:
                suggestions.append("DOGE")
            if "crypto" in text_lower and "bill" in text_lower:
                suggestions.extend(["BILL", "LAW", "CRYPTO"])

        elif narrative_type == "meme_moment":
            # Extract the meme phrase as ticker
            for kw in hits:
                ticker = re.sub(r'\s+', '', kw).upper()[:6]
                suggestions.append(ticker)

        elif narrative_type == "space_moment":
            suggestions.extend(["MOON", "MARS", "ROCKET", "SPACE"])
            if "starship" in text_lower:
                suggestions.append("STARSHIP")
            if "alien" in text_lower or "uap" in text_lower or "ufo" in text_lower:
                suggestions.extend(["ALIEN", "UFO", "UAP"])

        elif narrative_type == "crypto_narrative":
            for kw in hits:
                suggestions.append(kw.upper()[:6])

        # Deduplicate + filter
        seen = set()
        clean = []
        for t in suggestions:
            t_clean = re.sub(r'[^A-Z]', '', t)
            if 2 <= len(t_clean) <= 10 and t_clean not in seen:
                seen.add(t_clean)
                clean.append(t_clean)

        return clean[:10]

    def get_active_narratives(self, min_confidence: int = 5) -> List[Narrative]:
        """Return narratives above confidence threshold, sorted by strength."""
        narratives = [
            n for n in self.active_narratives.values()
            if n.confidence >= min_confidence
        ]
        narratives.sort(key=lambda n: (n.source_count, n.confidence, n.strength), reverse=True)
        return narratives

    def get_confirmed_narratives(self) -> List[Narrative]:
        """Return only narratives confirmed by 3+ sources."""
        return [n for n in self.active_narratives.values() if n.is_confirmed]

    def format_narrative_alert(self, narrative: Narrative) -> str:
        """Format a narrative into a Telegram alert."""
        type_emojis = {
            "animal_viral":       "🐾",
            "political_moment":   "🏛️",
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
        emoji = type_emojis.get(narrative.narrative_type, "🔥")
        confirmed_badge = "✅ CONFIRMED" if narrative.is_confirmed else "🔍 EMERGING"
        confidence_stars = "⭐" * narrative.confidence

        tickers_str = " | ".join([f"`${t}`" for t in narrative.suggested_tickers[:6]])

        lines = [
            f"{emoji} *NARRATIVE DETECTED* — {confirmed_badge}",
            f"📖 *{narrative.title}*",
            f"",
            f"🏷️ Type: {narrative.narrative_type.replace('_', ' ').title()}",
            f"📡 Sources: {narrative.source_count} ({', '.join(narrative.sources[:3])})",
            f"🔑 Keywords: {', '.join(narrative.keywords_matched[:5])}",
            f"💪 Strength: {confidence_stars} ({narrative.confidence}/10)",
            f"",
            f"💰 *Likely Tickers to Watch:*",
            f"{tickers_str}",
            f"",
        ]

        if narrative.raw_signals:
            lines.append(f"💬 \"{narrative.raw_signals[0][:150]}...\"")
            lines.append("")

        template = NARRATIVE_TEMPLATES.get(narrative.narrative_type, {})
        time_to_ticker = template.get("time_to_ticker", "unknown")
        lines += [
            f"⏱️ Expected time to ticker: {time_to_ticker}",
            f"_Monitor pump.fun for these tickers launching_",
        ]

        return "\n".join(lines)

    def format_narrative_summary(self) -> str:
        """Format all active narratives as a summary digest."""
        narratives = self.get_active_narratives(min_confidence=4)

        if not narratives:
            return "😴 No active narratives detected. Run /signals first."

        lines = [
            f"🧠 *NARRATIVE RADAR — {len(narratives)} Active*\n",
        ]

        confirmed = [n for n in narratives if n.is_confirmed]
        emerging = [n for n in narratives if not n.is_confirmed]

        if confirmed:
            lines.append("✅ *CONFIRMED (3+ sources):*")
            for n in confirmed[:5]:
                tickers = " ".join([f"`${t}`" for t in n.suggested_tickers[:3]])
                lines.append(f"🔥 *{n.title}*")
                lines.append(f"   Tickers: {tickers}")
                lines.append(f"   Sources: {n.source_count} | Confidence: {n.confidence}/10")
                lines.append("")

        if emerging:
            lines.append("🔍 *EMERGING (watch these):*")
            for n in emerging[:5]:
                tickers = " ".join([f"`${t}`" for t in n.suggested_tickers[:3]])
                lines.append(f"👀 *{n.title}*")
                lines.append(f"   Tickers: {tickers}")
                lines.append("")

        lines.append("_Use /narrative <name> for full details_")
        return "\n".join(lines)

    def cleanup_old_narratives(self, max_age_hours: int = 24):
        """Remove narratives older than max_age_hours."""
        cutoff = datetime.utcnow()
        to_remove = [
            key for key, n in self.active_narratives.items()
            if (cutoff - n.first_detected).seconds > max_age_hours * 3600
        ]
        for key in to_remove:
            self.narrative_history.append(self.active_narratives.pop(key))
