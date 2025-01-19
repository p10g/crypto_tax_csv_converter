from enum import Enum

class Source(Enum):
    UNKNOWN = 0
    SOLANA = 1

class KryptoSekkenType(Enum):
    HANDEL = "handel"
    ERVERV = "erverv"
    MINING = "mining"
    INNTEKT = "inntekt"
    TAP = "tap"
    FORBRUK = "forbruk"
    RENTEINNTEKT = "renteinntekt"
    OVERFORING_INN = "overforing-inn"
    OVERFORING_UT = "overforing-ut"
    GAVE_INN = "gave-inn"
    GAVE_UT = "gave-ut"
    TAP_UTEN_FRADRAG = "tap-uten-fradrag"
    FORVALTNINGSKOSTNAD = "forvaltningskostnad"

class SolanaTxType(Enum):
    TRADE = "TRADE"
    UNKNOWN = "_UNKNOWN"
    TRANSFER = "TRANSFER"
    SPEND = "SPEND"
    JUPITER_DCA_OPEN = "_JUPITER_DCA_OPEN"
    JUPITER_LIMIT_OPEN = "_JUPITER_LIMIT_OPEN"
    STAKING = "STAKING"
