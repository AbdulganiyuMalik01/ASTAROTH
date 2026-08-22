import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Optional

import aiohttp

# Use centralized token state module within the tracker package
try:
    from .token_state import tokens, TokenInfo, VOLUME_ALERT_THRESHOLD
except Exception:
    # Fallback to absolute import for direct execution contexts
    try:
        from tracker.token_state import tokens, TokenInfo, VOLUME_ALERT_THRESHOLD
    except Exception:
        tokens = {}
        TokenInfo = None
        VOLUME_ALERT_THRESHOLD = 5


@dataclass
class PumpToken:
    address: str
    symbol: str
    name: str
    liquidity_usd: float
    volume_24h: float
    risk_score: int
    dex_url: str
    created_at: datetime


class DexShim:
    """Lightweight DexScreener shim for basic token lookups."""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def search_token(self, ticker: str) -> List[Dict]:
        # Best-effort search using DexScreener search endpoint
        try:
            session = await self._get_session()
            url = f"https://api.dexscreener.com/latest/dex/search?q={ticker}"
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                # return raw pairs list if present
                return data.get('pairs', []) if isinstance(data, dict) else []
        except Exception:
            return []

    def parse_pair_to_token(self, pair: Dict) -> Optional[Dict]:
        # Parse a DexScreener pair dict to a minimal token dict expected by bot
        try:
            token = pair.get('token') or pair.get('baseToken') or {}
            return {
                'symbol': token.get('symbol') or pair.get('symbol') or 'UNK',
                'name': token.get('name') or pair.get('name') or 'Unknown',
                'address': token.get('address') or token.get('id') or '',
                'liquidity_usd': float(pair.get('liquidity', {}).get('usd') or 0),
                'volume_24h': float(pair.get('volume', {}).get('h24') or 0),
                'market_cap': float(pair.get('marketCap') or 0),
                'dex_url': f"https://dexscreener.com/" + pair.get('pairAddress', '')
            }
        except Exception:
            return None


class RiskScorer:
    async def score_token(self, parsed: Dict, session: aiohttp.ClientSession) -> int:
        # Very simple heuristic scorer: liquidity and volume drive score
        try:
            liq = parsed.get('liquidity_usd', 0) or 0
            vol = parsed.get('volume_24h', 0) or 0
            score = 5
            if liq >= 100000:
                score += 3
            elif liq >= 25000:
                score += 2
            elif liq >= 5000:
                score += 1

            if vol >= 50000:
                score += 2
            elif vol >= 10000:
                score += 1

            return min(max(int(score), 1), 10)
        except Exception:
            return 5


class PumpFunTracker:
    """Adapter that exposes a minimal interface used by the agent.

    It reads the in-memory `tokens` dict from `token_tracker` and presents
    PumpToken objects and helper methods used by `agent.bot`.
    """

    def __init__(self, helius_key: str = ""):
        self.helius_key = helius_key
        self.dex = DexShim()
        self.risk_scorer = RiskScorer()

    async def get_new_launches(self) -> List[PumpToken]:
        now = datetime.now(timezone.utc).timestamp()
        results: List[PumpToken] = []
        for mint, info in tokens.items():
            try:
                # TokenInfo.created_at stored as float timestamp in token_tracker
                created = getattr(info, 'created_at', None) or 0
                # Only recent tokens (within 24 hours) to be considered 'new'
                if now - created > 24 * 3600:
                    continue
                symbol = getattr(info, 'symbol', 'UNK') or 'UNK'
                name = getattr(info, 'name', 'Unknown') or 'Unknown'
                volume_sol = getattr(info, 'volume_sol', 0.0) or 0.0
                volume_24h = volume_sol * 180.0  # approximate conversion
                market_cap = getattr(info, 'market_cap', 0.0) or 0.0
                # liquidity approximation via market cap or volume
                liquidity_usd = market_cap or (volume_24h * 5)
                token = PumpToken(
                    address=str(mint),
                    symbol=symbol,
                    name=name,
                    liquidity_usd=liquidity_usd,
                    volume_24h=volume_24h,
                    risk_score=getattr(info, 'cabal_score', 5),
                    dex_url=f"https://dexscreener.com/solana/{str(mint)}",
                    created_at=datetime.fromtimestamp(created, tz=timezone.utc)
                )
                results.append(token)
            except Exception:
                continue
        # sort by created time descending
        results.sort(key=lambda x: x.created_at, reverse=True)
        return results

    async def find_tokens_matching_signals(self, animal_matches: Dict[str, List[str]], cashtags: List[str]) -> List[PumpToken]:
        matched: List[PumpToken] = []
        cashtags_up = [c.upper() for c in (cashtags or [])]
        # Flatten animal tickers
        animal_tickers = set()
        for tlist in (animal_matches or {}).values():
            for t in tlist:
                animal_tickers.add(str(t).upper())

        for mint, info in tokens.items():
            try:
                symbol = (getattr(info, 'symbol', '') or '').upper()
                name = getattr(info, 'name', '') or ''
                volume_sol = getattr(info, 'volume_sol', 0.0) or 0.0
                volume_24h = volume_sol * 180.0

                if cashtags_up and symbol in cashtags_up:
                    matched.append(PumpToken(str(mint), symbol, name, 0.0, volume_24h, getattr(info, 'cabal_score', 5), f"https://dexscreener.com/solana/{mint}", datetime.fromtimestamp(getattr(info,'created_at',0), tz=timezone.utc)))
                    continue

                # match by animal tickers
                if any(sym in symbol for sym in animal_tickers) or any(sym in name.upper() for sym in animal_tickers):
                    matched.append(PumpToken(str(mint), symbol, name, 0.0, volume_24h, getattr(info, 'cabal_score', 5), f"https://dexscreener.com/solana/{mint}", datetime.fromtimestamp(getattr(info,'created_at',0), tz=timezone.utc)))
            except Exception:
                continue

        # return top by volume
        matched.sort(key=lambda x: x.volume_24h, reverse=True)
        return matched

    async def get_volume_movers(self, min_volume: float = 10000) -> List[PumpToken]:
        movers: List[PumpToken] = []
        for mint, info in tokens.items():
            try:
                vol = (getattr(info, 'volume_sol', 0.0) or 0.0) * 180.0
                if vol >= min_volume:
                    movers.append(PumpToken(
                        address=str(mint),
                        symbol=(getattr(info,'symbol','') or 'UNK'),
                        name=(getattr(info,'name','') or 'Unknown'),
                        liquidity_usd=getattr(info,'market_cap',0.0) or 0.0,
                        volume_24h=vol,
                        risk_score=getattr(info,'cabal_score',5),
                        dex_url=f"https://dexscreener.com/solana/{mint}",
                        created_at=datetime.fromtimestamp(getattr(info,'created_at',0), tz=timezone.utc)
                    ))
            except Exception:
                continue
        movers.sort(key=lambda x: x.volume_24h, reverse=True)
        return movers

    def format_token_card(self, token: PumpToken) -> str:
        age_min = (datetime.now(timezone.utc) - token.created_at).total_seconds() / 60 if token.created_at else 0
        mc = f"${token.liquidity_usd:,.0f}" if token.liquidity_usd else "N/A"
        vol = f"${token.volume_24h:,.0f}"
        return (
            f"🪙 *{token.name}* (${token.symbol})\n"
            f"• 💰 Market Cap / Liq: {mc}\n"
            f"• 💹 Volume 24h: {vol}\n"
            f"• ⏰ Age: {age_min:.1f}m\n"
            f"• 🔗 Dex: {token.dex_url}"
        )
