# -*- coding: utf-8 -*-
# arbitrage/data/schemas.py
from __future__ import annotations
from dataclasses import dataclass, field
from time import time
from typing import List, Tuple, Optional, Literal

Side = Literal["bid", "ask"]
Kind = Literal["spot", "coinm", "usdtm", "usdcm"]

# 订单簿（部分档）
@dataclass
class OrderBook:
    symbol: str
    ts: float                        # 秒（time.time()）
    bids: List[Tuple[float, float]]  # [(price, qty_base)]
    asks: List[Tuple[float, float]]

# 标记价 / 近似中位价（spot 用 bookTicker -> mid）
@dataclass
class MarkPrice:
    symbol: str
    ts: float
    mark: float
    index: Optional[float] = None

# 资金费（可选）
@dataclass
class FundingRate:
    symbol: str
    ts: float
    rate: float

# 元信息（合约面值、小数精度等）
@dataclass
class Meta:
    symbol: str
    kind: Kind
    contract_size: Optional[float] = None   # COIN-M: USD/张
    price_tick: Optional[float] = None
    qty_step: Optional[float] = None

@dataclass
class Position:
    side: str                   # "POS" / "NEG"
    Q: float = 0.0              # 现货/UM 的 base 数量
    N: int = 0                  # COIN-M 的张数
    spot_symbol: str = ""
    coinm_symbol: str = ""
    trade_id: int = 0
    t0: float = field(default_factory=lambda: time.time())
