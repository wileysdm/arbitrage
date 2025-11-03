from dataclasses import dataclass, field
import time

@dataclass
class Position:
    side: str   # "POS" or "NEG"
    Q: float
    N: int
    spot_symbol: str = ""
    coinm_symbol: str = ""
    t0: float = field(default_factory=time.time)
    trade_id: str = ""

@dataclass
class OrderIds:
    spot_order_id: int | None
    cm_order_id: int | None
