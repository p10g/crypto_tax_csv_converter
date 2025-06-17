from enums import SolanaTxType
from enums import KryptoSekkenType
from typing import Dict

SOLANA_TO_KRYPTOSEKKEN_TX_TYPES = {
    SolanaTxType.TRADE: KryptoSekkenType.HANDEL
}

PUBLIC_TICKERS_TO_KRYPTOSEKKEN_TICKERS: Dict[str, str] = {
    "TRUMP": "TRUMP2",
    "ME": "ME2",
    "GOAT": "GOAT3",
    "NVIDIA": "NVDA2",
}