# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass, field
import time

@dataclass
class Position:
    side: str                   # "POS" / "NEG"
    Q: float = 0.0              # 现货/UM 的 base 数量
    N: int = 0                  # COIN-M 的张数
    spot_symbol: str = ""
    coinm_symbol: str = ""
    trade_id: int = 0
    t0: float = field(default_factory=lambda: time.time())
