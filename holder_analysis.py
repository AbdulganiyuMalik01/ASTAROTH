"""
Token Holder Analysis Module
Analyzes token holder distribution to detect rug risks.
"""
import logging
from typing import Optional, Dict, Tuple
from dataclasses import dataclass
import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class HolderMetrics:
    """Token holder distribution metrics."""
    total_holders: int = 0
    top10_percentage: float = 0.0
    top5_percentage: float = 0.0
    top1_percentage: float = 0.0
    rug_risk_score: int = 0  # 0-10, higher is riskier
    is_risky: bool = False


async def get_holder_data_helius(session: aiohttp.ClientSession, mint: str, api_key: str) -> Optional[Dict]:
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        payload = {
            "jsonrpc": "2.0",
            "id": "holder-analysis",
            "method": "getTokenLargestAccounts",
            "params": [mint]
        }
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if "result" not in data or "value" not in data["result"]:
                return None
            return {"largest_accounts": data["result"]["value"]}
    except Exception as e:
        logger.debug(f"Error fetching holder data from Helius: {e}")
        return None


def calculate_holder_metrics(largest_accounts: list, total_supply: Optional[int] = None) -> HolderMetrics:
    metrics = HolderMetrics()
    if not largest_accounts:
        return metrics
    sorted_accounts = sorted(largest_accounts, key=lambda x: int(x.get("amount", 0)), reverse=True)
    if total_supply is None:
        top_amounts = [int(acc.get("amount", 0)) for acc in sorted_accounts[:20]]
        if top_amounts:
            total_supply = sum(top_amounts) * 1.5
    if total_supply and total_supply > 0:
        top1_amount = int(sorted_accounts[0].get("amount", 0)) if sorted_accounts else 0
        top5_amount = sum(int(acc.get("amount", 0)) for acc in sorted_accounts[:5])
        top10_amount = sum(int(acc.get("amount", 0)) for acc in sorted_accounts[:10])
        metrics.top1_percentage = (top1_amount / total_supply) * 100
        metrics.top5_percentage = (top5_amount / total_supply) * 100
        metrics.top10_percentage = (top10_amount / total_supply) * 100
    metrics.total_holders = len(largest_accounts)
    risk_score = 0
    if metrics.top1_percentage > 50:
        risk_score += 5
    elif metrics.top1_percentage > 30:
        risk_score += 3
    elif metrics.top1_percentage > 20:
        risk_score += 2
    elif metrics.top1_percentage > 10:
        risk_score += 1
    if metrics.top10_percentage > 80:
        risk_score += 4
    elif metrics.top10_percentage > 60:
        risk_score += 2
    elif metrics.top10_percentage > 40:
        risk_score += 1
    if metrics.total_holders < 10:
        risk_score += 1
    metrics.rug_risk_score = min(risk_score, 10)
    metrics.is_risky = metrics.rug_risk_score >= 6
    return metrics


async def analyze_token_holders(session: aiohttp.ClientSession, mint: str, helius_api_key: str) -> HolderMetrics:
    helius_data = await get_holder_data_helius(session, mint, helius_api_key)
    if helius_data and "largest_accounts" in helius_data:
        return calculate_holder_metrics(helius_data["largest_accounts"])
    logger.debug(f"No holder data available for {mint}")
    return HolderMetrics()


def format_holder_analysis(metrics: HolderMetrics) -> str:
    if metrics.total_holders == 0:
        return "Holder data: N/A"
    risk_emoji = "🟢" if metrics.rug_risk_score < 4 else "🟡" if metrics.rug_risk_score < 7 else "🔴"
    text = f"{risk_emoji} Holders: {metrics.total_holders}\n"
    if metrics.top10_percentage > 0:
        text += f"Top 10: {metrics.top10_percentage:.1f}%\n"
    if metrics.top1_percentage > 0:
        text += f"Top 1: {metrics.top1_percentage:.1f}%\n"
    if metrics.is_risky:
        text += "⚠️ HIGH RUG RISK"
    return text.strip()
