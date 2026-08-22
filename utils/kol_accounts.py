from typing import List, Dict

KOL_ACCOUNTS: List[Dict] = [

    # ═══════════════════════════════════════════════════════════════════════════
    # TIER 1 — MAINSTREAM MEGA INFLUENCERS (weight 9-10)
    # These accounts can single-handedly create a memecoin narrative
    # ═══════════════════════════════════════════════════════════════════════════
    {"username": "elonmusk",            "weight": 10, "category": "mainstream"},
    {"username": "realDonaldTrump",     "weight": 10, "category": "mainstream"},
    {"username": "POTUS",               "weight": 9,  "category": "mainstream"},
    {"username": "BarackObama",         "weight": 9,  "category": "mainstream"},
    {"username": "kanyewest",           "weight": 9,  "category": "mainstream"},
    {"username": "MrBeast",             "weight": 9,  "category": "mainstream"},
    {"username": "elonmusk",            "weight": 10, "category": "mainstream"},
    {"username": "BillGates",           "weight": 8,  "category": "mainstream"},
    {"username": "JeffBezos",           "weight": 8,  "category": "mainstream"},
    {"username": "richardbranson",      "weight": 7,  "category": "mainstream"},
    {"username": "neymarjr",            "weight": 8,  "category": "mainstream"},
    {"username": "Cristiano",           "weight": 8,  "category": "mainstream"},
    {"username": "KingJames",           "weight": 8,  "category": "mainstream"},
    {"username": "Drake",               "weight": 8,  "category": "mainstream"},
    {"username": "rihanna",             "weight": 7,  "category": "mainstream"},
    {"username": "taylorswift13",       "weight": 9,  "category": "mainstream"},
    {"username": "ladygaga",            "weight": 7,  "category": "mainstream"},
    {"username": "justinbieber",        "weight": 7,  "category": "mainstream"},
    {"username": "KimKardashian",       "weight": 8,  "category": "mainstream"},
    {"username": "khloekardashian",     "weight": 6,  "category": "mainstream"},
    {"username": "kyliejenner",         "weight": 8,  "category": "mainstream"},
    {"username": "snoopdogg",           "weight": 8,  "category": "mainstream"},
    {"username": "50cent",              "weight": 7,  "category": "mainstream"},
    {"username": "FloydMayweather",     "weight": 7,  "category": "mainstream"},
    {"username": "Tyson",               "weight": 7,  "category": "mainstream"},
    {"username": "mcgregor_notorious",  "weight": 7,  "category": "mainstream"},
    {"username": "NASA",                "weight": 6,  "category": "mainstream"},
    {"username": "pewdiepie",           "weight": 7,  "category": "mainstream"},
    {"username": "LoganPaul",           "weight": 8,  "category": "mainstream"},
    {"username": "KSI",                 "weight": 7,  "category": "mainstream"},

    # (rest omitted for brevity in file — full list available in original)
]

# Deduplicate
_seen_usernames = set()
_deduped = []
for _acc in KOL_ACCOUNTS:
    if _acc["username"].lower() not in _seen_usernames:
        _seen_usernames.add(_acc["username"].lower())
        _deduped.append(_acc)
KOL_ACCOUNTS = _deduped
