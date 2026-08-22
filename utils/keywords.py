import re
from typing import List, Dict, Set

# ── Animal keyword dictionary ──────────────────────────────────────────────────
# Maps animal names / viral references → likely pump.fun ticker patterns
ANIMAL_TICKER_MAP: Dict[str, List[str]] = {
    # Rodents & small mammals
    "squirrel": ["SQUIRL", "SQRL", "PNUT", "NUT"],
    "peanut": ["PNUT", "NUT", "PEANUT"],
    "hamster": ["HAMSTR", "HAM", "HAMSTER"],
    "capybara": ["CAPY", "CAPYBARA", "BARA"],
    "beaver": ["BEAV", "BEAVER"],
    "otter": ["OTTER", "OTR"],
    "marmot": ["MARMOT", "MARM"],
    "mole": ["MOLE", "MOL"],

    # Aquatic & marine
    "penguin": ["PENGU", "PENGUIN", "PNG"],
    "seal": ["SEAL", "SL"],
    "punch": ["PUNCH", "FIST", "SEALPUNCH"],
    "whale": ["WHALE", "WHL"],
    "dolphin": ["DOLPH", "DOLPHIN", "DLP"],
    "shark": ["SHARK", "SHRK"],
    "octopus": ["OCTO", "OCT", "OCTOPUS"],
    "jellyfish": ["JELLY", "JLYFISH"],
    "turtle": ["TRTL", "TURTLE"],
    "frog": ["FROG", "PEPE", "FRG"],
    "fish": ["FISH", "FSH"],
    "crab": ["CRAB", "CRB"],
    "shrimp": ["SHRIMP", "SHRMP"],

    # Big cats & predators
    "cat": ["CAT", "KITTY", "MEOW", "NYAN"],
    "tiger": ["TIGER", "TGR"],
    "lion": ["LION", "LIO"],
    "leopard": ["LEO", "LEOP"],
    "cheetah": ["CHEETAH", "CHTA"],
    "wolf": ["WOLF", "WLF", "DOGE"],
    "fox": ["FOX", "FXS"],
    "bear": ["BEAR", "BER"],

    # Dogs & canines
    "dog": ["DOG", "DOGE", "DOGGO", "WIF", "BONK"],
    "doge": ["DOGE", "DOG", "SHIB"],
    "shiba": ["SHIB", "SHIBA", "INU"],
    "corgi": ["CORGI", "CRG"],
    "poodle": ["POODLE", "PDL"],

    # Primates
    "monkey": ["MONK", "APE", "MONKE"],
    "ape": ["APE", "BORED", "BAYC"],
    "gorilla": ["GORILLA", "GRIL"],
    "chimp": ["CHIMP", "CHP"],

    # Birds
    "bird": ["BIRD", "BRD"],
    "eagle": ["EAGLE", "EGL"],
    "parrot": ["PARROT", "PRRT"],
    "chicken": ["CHKN", "HEN", "CHICKEN"],
    "duck": ["DUCK", "DCK", "QUACK"],
    "crow": ["CROW", "CRW"],
    "owl": ["OWL"],
    "flamingo": ["FLMG", "FLAMINGO"],

    # Exotic / viral animals
    "moodeng": ["MOODENG", "HIPPO", "BABY"],
    "hippo": ["HIPPO", "HIP", "MOODENG"],
    "giraffe": ["GIRAFFE", "GRFF"],
    "elephant": ["ELEPH", "ELEFANT"],
    "rhino": ["RHINO", "RHN"],
    "panda": ["PANDA", "PND"],
    "koala": ["KOALA", "KLA"],
    "kangaroo": ["KANGA", "KNG", "ROO"],
    "sloth": ["SLOTH", "SLTH"],
    "axolotl": ["AXOL", "AXOLOTL"],
    "platypus": ["PLATY", "PLATYPUS"],
    "narwhal": ["NARWHAL", "NRW"],
    "manatee": ["MANATEE", "MANA"],
    "tapir": ["TAPIR"],
    "pangolin": ["PANGOLIN", "PGL"],
    "okapi": ["OKAPI"],

    # Reptiles
    "snake": ["SNAKE", "SNK", "SNEK"],
    "lizard": ["LIZARD", "LZD"],
    "chameleon": ["CHAM", "CHAMELEON"],
    "gecko": ["GECKO", "GCK"],
    "iguana": ["IGUANA", "IGN"],
    "crocodile": ["CROC", "CROCO"],

    # Insects & small creatures
    "bee": ["BEE", "HONEY", "BUZZ"],
    "butterfly": ["BFLY", "BUTTERFLY"],
    "ant": ["ANT", "ANTHILL"],
    "spider": ["SPIDER", "SPDR"],
    "snail": ["SNAIL", "SNL"],
}

# ── Viral/cultural keywords that signal memecoin opportunity ───────────────────
CULTURAL_SIGNALS: List[str] = [
    "endangered", "rescued", "viral", "trending", "cute", "baby",
    "zoo", "wildlife", "sanctuary", "saved", "orphaned", "rare",
    "extinct", "conservation", "adopted", "famous", "internet famous",
    "gone viral", "breaking", "shocking", "unbelievable",
]

# ── KOL accounts to monitor ───────────────────────────────────────────────────
KOL_ACCOUNTS: List[Dict] = [
    # Mainstream influencers
    {"username": "elonmusk",       "weight": 10, "category": "mainstream"},
    {"username": "realDonaldTrump","weight": 9,  "category": "mainstream"},
    {"username": "MrBeast",        "weight": 7,  "category": "mainstream"},
    {"username": "kanyewest",      "weight": 6,  "category": "mainstream"},

    # Crypto KOLs
    {"username": "AnsemSol",       "weight": 9,  "category": "crypto_kol"},
    {"username": "MustStopMurad",  "weight": 9,  "category": "crypto_kol"},
    {"username": "blknoiz06",      "weight": 8,  "category": "crypto_kol"},
    {"username": "CryptoGodJohn",  "weight": 7,  "category": "crypto_kol"},
    {"username": "solbigbrain",    "weight": 7,  "category": "crypto_kol"},
    {"username": "DegenSpartan",   "weight": 8,  "category": "crypto_kol"},
    {"username": "cobie",          "weight": 8,  "category": "crypto_kol"},
    {"username": "gainzy222",      "weight": 7,  "category": "crypto_kol"},
    {"username": "KookCapitalLLC", "weight": 7,  "category": "crypto_kol"},
    {"username": "rajgokal",       "weight": 7,  "category": "solana"},
    {"username": "aeyakovenko",    "weight": 7,  "category": "solana"},
]

# ── Nitter instances (rotate if one is down) ─────────────────────────────────
NITTER_INSTANCES: List[str] = [
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.1d4.us",
]

# ── News RSS feeds ─────────────────────────────────────────────────────────────
NEWS_FEEDS: List[Dict] = [
    {"name": "CoinTelegraph",  "url": "https://cointelegraph.com/rss"},
    {"name": "Decrypt",        "url": "https://decrypt.co/feed"},
    {"name": "CoinDesk",       "url": "https://coindesk.com/arc/outboundfeeds/rss/"},
    {"name": "BBC Wildlife",   "url": "https://www.bbc.co.uk/news/science_and_environment/rss.xml"},
    {"name": "WWF News",       "url": "https://www.worldwildlife.org/press-releases.rss"},
    {"name": "The Dodo",       "url": "https://www.thedodo.com/rss"},
]

# ── Google Trends keywords to track ───────────────────────────────────────────
TREND_KEYWORDS: List[str] = [
    "animal coin", "meme coin", "dog coin", "cat coin",
    "new crypto", "pump fun", "solana meme",
    "viral animal", "endangered animal", "cute animal viral",
]


def extract_keywords(text: str) -> Set[str]:
    """Extract meaningful keywords from raw text."""
    text_lower = text.lower()
    # Remove URLs, mentions, special chars
    text_clean = re.sub(r"http\S+|@\w+|#(\w+)|[^a-z\s]", r" \1 ", text_lower)
    words = set(text_clean.split())
    # Filter stop words
    stop_words = {
        "the", "a", "an", "is", "it", "in", "on", "at", "to", "for",
        "of", "and", "or", "but", "with", "this", "that", "i", "we",
        "you", "he", "she", "they", "my", "our", "your", "its",
        "have", "has", "had", "be", "been", "are", "was", "were",
        "will", "would", "could", "should", "do", "does", "did",
        "just", "now", "new", "get", "so", "up", "out", "if",
    }
    return words - stop_words


def extract_hashtags(text: str) -> List[str]:
    """Extract hashtags from text."""
    return re.findall(r"#(\w+)", text.lower())


def extract_cashtags(text: str) -> List[str]:
    """Extract cashtags ($TICKER) from text."""
    return [t.upper() for t in re.findall(r"\$([A-Za-z]{2,10})", text)]


def match_animals_to_tickers(keywords: Set[str]) -> Dict[str, List[str]]:
    """
    Given a set of keywords, return matched animals and their likely tickers.
    Returns: { "animal_name": ["TICKER1", "TICKER2"] }
    """
    matches = {}
    for word in keywords:
        if word in ANIMAL_TICKER_MAP:
            matches[word] = ANIMAL_TICKER_MAP[word]
        # Partial match (e.g. "dogs" matches "dog")
        for animal, tickers in ANIMAL_TICKER_MAP.items():
            if animal in word or word in animal:
                if animal not in matches:
                    matches[animal] = tickers
    return matches


def score_signal_strength(
    text: str,
    source_weight: int = 5,
    is_trending: bool = False,
    has_cashtag: bool = False,
) -> int:
    """
    Score a signal 1-10 based on various factors.
    Higher = stronger opportunity signal.
    """
    score = source_weight  # Base from KOL weight

    keywords = extract_keywords(text)
    text_lower = text.lower()

    # Cultural signal boost
    cultural_hits = sum(1 for s in CULTURAL_SIGNALS if s in text_lower)
    score += min(cultural_hits, 2)

    # Animal match boost
    animal_matches = match_animals_to_tickers(keywords)
    if animal_matches:
        score += min(len(animal_matches), 2)

    # Cashtag = someone already calling a ticker
    if has_cashtag:
        score += 1

    # Trending confirmation
    if is_trending:
        score += 1

    return min(score, 10)


def format_signal_alert(
    source: str,
    text: str,
    animal_matches: Dict,
    cashtags: List[str],
    signal_score: int,
    matched_tokens: List[Dict],
) -> str:
    """Format a signal into a clean Telegram alert message."""
    emoji = "🔥" if signal_score >= 8 else "⚡" if signal_score >= 6 else "👀"
    lines = [
        f"{emoji} *SIGNAL DETECTED* {emoji}",
        f"📡 Source: {source}",
        f"💬 \"{text[:200]}{'...' if len(text) > 200 else ''}\"",
        "",
    ]

    if cashtags:
        lines.append(f"💰 Cashtags mentioned: {' '.join(['$'+t for t in cashtags])}")

    if animal_matches:
        animals_str = ", ".join([f"{a} → {', '.join(['$'+t for t in tickers[:3]])}"
                                  for a, tickers in list(animal_matches.items())[:3]])
        lines.append(f"🐾 Animal matches: {animals_str}")

    lines += [
        f"📊 Signal Strength: {'⭐' * signal_score} ({signal_score}/10)",
        "",
    ]

    if matched_tokens:
        lines.append("🎯 *Matching tokens on pump.fun:*")
        for token in matched_tokens[:5]:
            risk_emoji = "🟢" if token.get('risk_score', 5) >= 7 else "🟡" if token.get('risk_score', 5) >= 5 else "🔴"
            lines.append(
                f"{risk_emoji} `{token.get('symbol', 'N/A')}` | "
                f"Liq: ${token.get('liquidity', 0):,.0f} | "
                f"Vol: ${token.get('volume', 0):,.0f} | "
                f"Risk: {token.get('risk_score', '?')}/10"
            )
            if token.get('address'):
                lines.append(f"   📋 `{token['address'][:20]}...`")
    else:
        lines.append("🔍 No matching tokens found yet — monitoring for launches...")

    lines += [
        "",
        "⚡ _Powered by Quant Agent_",
    ]

    return "\n".join(lines)
