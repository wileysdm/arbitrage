# -*- coding: utf-8 -*-
# arbitrage/data/schemas.py
from __future__ import annotations
from dataclasses import dataclass
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
