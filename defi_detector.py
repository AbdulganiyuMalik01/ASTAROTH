"""
DeFi Protocol Detection Module
Detects new DeFi contract deployments and high-value protocol launches.
"""
import logging
from typing import Optional, Dict, List
from dataclasses import dataclass
import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class DeFiProtocol:
    """DeFi protocol information."""
    program_id: str
    protocol_type: str  # "AMM", "Lending", "Staking", etc.
    initial_liquidity_sol: float = 0.0
    deployer: Optional[str] = None
    is_high_value: bool = False


DEFI_PATTERNS = {
    "AMM": ["initialize", "swap", "addLiquidity", "removeLiquidity", "createPool"],
    "Lending": ["initLendingMarket", "deposit", "borrow", "repay", "withdraw"],
    "Staking": ["stake", "unstake", "claim", "initializeStakePool"],
    "Vault": ["deposit", "withdraw", "initializeVault"]
}

KNOWN_DEFI_PROGRAMS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "So11111111111111111111111111111111111111112",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
}


def is_defi_program(program_id: str, instructions: List[str]) -> Optional[str]:
    if program_id in KNOWN_DEFI_PROGRAMS:
        return None
    for protocol_type, patterns in DEFI_PATTERNS.items():
        matches = sum(1 for pattern in patterns if any(pattern.lower() in inst.lower() for inst in instructions))
        if matches >= 2:
            return protocol_type
    return None


def extract_instructions_from_transaction(tx_data: dict) -> List[str]:
    instructions = []
    try:
        tx_instructions = tx_data.get("transaction", {}).get("message", {}).get("instructions", [])
        for ix in tx_instructions:
            parsed = ix.get("parsed", {})
            if isinstance(parsed, dict):
                ix_type = parsed.get("type", "")
                if ix_type:
                    instructions.append(ix_type)
            program = ix.get("program", "")
            if program:
                instructions.append(program)
        inner = tx_data.get("meta", {}).get("innerInstructions", [])
        for inner_ix in inner:
            for iix in inner_ix.get("instructions", []):
                parsed = iix.get("parsed", {})
                if isinstance(parsed, dict):
                    ix_type = parsed.get("type", "")
                    if ix_type:
                        instructions.append(ix_type)
    except Exception as e:
        logger.debug(f"Error extracting instructions: {e}")
    return instructions


async def analyze_initial_liquidity(session: aiohttp.ClientSession, program_id: str, tx_data: dict) -> float:
    try:
        pre_balances = tx_data.get("meta", {}).get("preBalances", [])
        post_balances = tx_data.get("meta", {}).get("postBalances", [])
        if not pre_balances or not post_balances:
            return 0.0
        total_transferred = 0.0
        for i in range(min(len(pre_balances), len(post_balances))):
            diff = (post_balances[i] - pre_balances[i]) / 1e9
            if diff > 0:
                total_transferred += diff
        return total_transferred
    except Exception as e:
        logger.debug(f"Error analyzing initial liquidity: {e}")
        return 0.0


def get_deployer_address(tx_data: dict) -> Optional[str]:
    try:
        account_keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        if account_keys:
            first_account = account_keys[0]
            if isinstance(first_account, str):
                return first_account
            elif isinstance(first_account, dict):
                return first_account.get("pubkey")
    except Exception as e:
        logger.debug(f"Error extracting deployer: {e}")
    return None


async def detect_defi_deployment(
    session: aiohttp.ClientSession,
    tx_data: dict,
    min_liquidity_sol: float = 5.0
) -> Optional[DeFiProtocol]:
    try:
        account_keys = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        if not account_keys:
            return None
        instructions = extract_instructions_from_transaction(tx_data)
        for account in account_keys:
            if isinstance(account, dict):
                program_id = account.get("pubkey")
            else:
                program_id = str(account)
            if not program_id:
                continue
            protocol_type = is_defi_program(program_id, instructions)
            if not protocol_type:
                continue
            initial_liquidity = await analyze_initial_liquidity(session, program_id, tx_data)
            deployer = get_deployer_address(tx_data)
            is_high_value = initial_liquidity >= min_liquidity_sol
            return DeFiProtocol(
                program_id=program_id,
                protocol_type=protocol_type,
                initial_liquidity_sol=initial_liquidity,
                deployer=deployer,
                is_high_value=is_high_value,
            )
    except Exception as e:
        logger.debug(f"Error in DeFi detection: {e}")
    return None


def format_defi_alert(protocol: DeFiProtocol, token_name: str = "Unknown") -> str:
    if protocol.is_high_value:
        header = "🔥🔥🔥 HIGH VALUE PROTOCOL LAUNCH 🔥🔥🔥"
    else:
        header = "🆕 NEW DEFI CONTRACT DEPLOYED"
    message = f"{header}\n\n"
    message += f"Type: {protocol.protocol_type}\n"
    message += f"Program: <code>{protocol.program_id[:8]}...{protocol.program_id[-6:]}</code>\n\n"
    if protocol.initial_liquidity_sol > 0:
        message += f"💰 Initial Liquidity: {protocol.initial_liquidity_sol:.2f} SOL\n"
    if protocol.deployer:
        message += f"👤 Deployer: <code>{protocol.deployer[:8]}...{protocol.deployer[-6:]}</code>\n"
    message += f"\n🔗 <a href='https://solscan.io/account/{protocol.program_id}'>View on Solscan</a>"
    return message
