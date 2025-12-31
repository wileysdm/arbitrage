# -*- coding: utf-8 -*-
# arbitrage/exchanges/legs.py
"""统一腿适配器（统一账户 / PAPI 版本）。

- 支持：Spot(=PM Cross Margin), Coin-M, UM（USDT-M/USDC-M）
- 行情：仅从 DataService.bus.latest(...) 读取（WS）；若缺失/过旧则直接报错，由策略跳过本轮
- 下单：
    - Spot/Margin：PAPI /papi/v1/margin/order
    - Coin-M：exec_binance_rest.py（PAPI /papi/v1/cm/order）
    - UM：直接使用 binance_rest.r_signed（PAPI /papi/v1/um/order）
"""
from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import Tuple, List, Optional
import time

# === data 层（只读） ===
from arbitrage.data.bus import Topic
from arbitrage.data.service import DataService
from arbitrage.data.adapters.binance import rest as rest_adp  # 兜底 REST
from arbitrage.data.schemas import OrderBook, MarkPrice, Meta

# === 你已有的执行封装 ===
from arbitrage.exchanges.exec_binance_rest import (
    place_coinm_market, place_coinm_limit,   get_coinm_order_status, cancel_coinm_order
)

# === UM 下单直连（沿用 HTTP 底座签名） ===
from arbitrage.exchanges.binance_rest import r_signed
from arbitrage.config import (
    PAPI_BASE,
    PAPI_KEY,
    PAPI_SECRET,
    MARGIN_SIDE_EFFECT_TYPE,
    ORDERBOOK_MAX_AGE_SEC,
    MARK_MAX_AGE_SEC,
)

OrderBookSide = List[Tuple[float, float]]
SVC = DataService.global_service()   # 需先由 app/main.py 注册并 start()

# ---------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------
def _get_ob(symbol: str) -> Optional[OrderBook]:
    return SVC.bus.latest(Topic.ORDERBOOK, symbol.upper())

def _get_mark(symbol: str) -> Optional[MarkPrice]:
    return SVC.bus.latest(Topic.MARK, symbol.upper())

def _get_meta(kind: str, symbol: str) -> Meta:
    mt = SVC.bus.latest(Topic.META, symbol.upper())
    if isinstance(mt, Meta):
        return mt
    # 兜底请求一次并发布
    m = rest_adp.get_meta(kind, symbol)
    SVC.bus.publish(Topic.META, symbol.upper(), m)
    return m

def _mid_from_ob(ob: OrderBook) -> float:
    return (ob.bids[0][0] + ob.asks[0][0]) / 2.0


def _is_fresh(ts: float, max_age_sec: float) -> bool:
    try:
        return (time.time() - float(ts)) <= float(max_age_sec)
    except Exception:
        return False


def _require_orderbook(symbol: str, limit: int) -> OrderBook:
    ob = _get_ob(symbol)
    if not ob:
        raise RuntimeError(f"orderbook not available for {symbol}")
    if not _is_fresh(getattr(ob, "ts", 0.0), ORDERBOOK_MAX_AGE_SEC):
        raise RuntimeError(f"orderbook stale for {symbol}: ts={getattr(ob, 'ts', None)}")
    if not getattr(ob, "bids", None) or not getattr(ob, "asks", None):
        raise RuntimeError(f"orderbook empty for {symbol}")
    if limit and (len(ob.bids) < 1 or len(ob.asks) < 1):
        raise RuntimeError(f"orderbook insufficient levels for {symbol}")
    return ob


def _require_mark(symbol: str) -> MarkPrice:
    mp = _get_mark(symbol)
    if not mp:
        raise RuntimeError(f"mark not available for {symbol}")
    if not _is_fresh(getattr(mp, "ts", 0.0), MARK_MAX_AGE_SEC):
        raise RuntimeError(f"mark stale for {symbol}: ts={getattr(mp, 'ts', None)}")
    return mp

# ---------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------
@dataclass
class LegSpec:
    kind: str      # 'coinm' | 'usdtm' | 'usdcm'
    symbol: str

class BaseLeg:
    symbol: str
    def get_books(self, limit=5) -> Tuple[OrderBookSide, OrderBookSide]: ...
    def ref_price(self) -> float: ...
    def qty_from_usd(self, V_usd: float) -> float: ...
    def place_market(self, side: str, qty: float, reduce_only: bool=False) -> dict: ...
    def place_limit_maker(self, side: str, qty: float, px: float) -> dict: ...
    def get_order_status(self, order_id: int) -> Tuple[str, float]: ...
    def cancel(self, order_id: int) -> dict: ...
    def is_perp(self) -> bool: return False
    def contract_size(self) -> Optional[float]: return None


# ---------------------------------------------------------------------
# SPOT（Portfolio Margin 下的 Cross Margin 现货/杠杆）
# ---------------------------------------------------------------------
class SpotLeg(BaseLeg):
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()

    def get_books(self, limit=5):
        ob = _require_orderbook(self.symbol, limit)
        return ob.bids[:limit], ob.asks[:limit]

    def ref_price(self) -> float:
        # spot: 优先 MARK（来自 bookTicker），否则退化为同一份 WS orderbook 的 mid
        try:
            mp = _require_mark(self.symbol)
            return float(mp.mark)
        except Exception:
            ob = _require_orderbook(self.symbol, 1)
            return float(_mid_from_ob(ob))

    def qty_from_usd(self, V_usd: float) -> float:
        return V_usd / max(1e-12, self.ref_price())

    def place_market(self, side: str, qty: float, reduce_only: bool=False):
        params = {
            "symbol": self.symbol,
            "side": "BUY" if side == "BUY" else "SELL",
            "type": "MARKET",
            "quantity": f"{float(qty):.8f}",
            "newOrderRespType": "FULL",
        }
        if MARGIN_SIDE_EFFECT_TYPE:
            params["sideEffectType"] = MARGIN_SIDE_EFFECT_TYPE
        logging.info("Placing margin market order: %s", params)
        return r_signed(PAPI_BASE, "/papi/v1/margin/order", "POST", params, PAPI_KEY, PAPI_SECRET)

    def place_limit_maker(self, side: str, qty: float, px: float):
        params = {
            "symbol": self.symbol,
            "side": "BUY" if side == "BUY" else "SELL",
            "type": "LIMIT_MAKER",
            "quantity": f"{float(qty):.8f}",
            "price": f"{float(px):.8f}",
            "newOrderRespType": "FULL",
        }
        if MARGIN_SIDE_EFFECT_TYPE:
            params["sideEffectType"] = MARGIN_SIDE_EFFECT_TYPE
        logging.info("Placing margin limit maker order: %s", params)
        return r_signed(PAPI_BASE, "/papi/v1/margin/order", "POST", params, PAPI_KEY, PAPI_SECRET)

    def get_order_status(self, order_id: int):
        r = r_signed(
            PAPI_BASE,
            "/papi/v1/margin/order",
            "GET",
            {"symbol": self.symbol, "orderId": int(order_id)},
            PAPI_KEY,
            PAPI_SECRET,
        )
        return r.get("status", ""), float(r.get("executedQty", 0.0) or 0.0)

    def cancel(self, order_id: int):
        return r_signed(
            PAPI_BASE,
            "/papi/v1/margin/order",
            "DELETE",
            {"symbol": self.symbol, "orderId": int(order_id)},
            PAPI_KEY,
            PAPI_SECRET,
        )

# ---------------------------------------------------------------------
# COIN-M
# ---------------------------------------------------------------------
class CoinMLeg(BaseLeg):
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self._C = None  # USD/张

    def _ensure_meta(self):
        if self._C is None:
            mt = _get_meta("coinm", self.symbol)
            self._C = float(mt.contract_size or 100.0)

    def get_books(self, limit=5):
        ob = _require_orderbook(self.symbol, limit)
        return ob.bids[:limit], ob.asks[:limit]

    def ref_price(self) -> float:
        mp = _require_mark(self.symbol)
        return float(mp.mark)

    def qty_from_usd(self, V_usd: float) -> float:
        self._ensure_meta()
        C = float(self._C or 0.0)
        return V_usd / max(1e-12, C)

    def place_market(self, side: str, qty: float, reduce_only: bool=False):
        n = int(qty)
        logging.info("Placing place_market order: side=%s, qty=%s, symbol=%s", side, n, self.symbol)
        return place_coinm_market("BUY" if side=="BUY" else "SELL", n, reduce_only=reduce_only, symbol=self.symbol)

    def place_limit_maker(self, side: str, qty: float, px: float):
        n = int(qty)
        logging.info("Placing place_limit_maker order: side=%s, qty=%s, px=%s, symbol=%s", side, n, px, self.symbol)
        return place_coinm_limit("BUY" if side=="BUY" else "SELL", n, float(px), post_only=True, symbol=self.symbol)

    def get_order_status(self, order_id: int):
        st, filled = get_coinm_order_status(order_id, symbol=self.symbol)
        return st, float(filled or 0.0)

    def cancel(self, order_id: int): return cancel_coinm_order(order_id, symbol=self.symbol)
    def is_perp(self) -> bool: return True
    def contract_size(self) -> Optional[float]: 
        self._ensure_meta(); return self._C

# ---------------------------------------------------------------------
# USDⓈ-M（USDT-M / USDC-M）
# ---------------------------------------------------------------------
class UMLeg(BaseLeg):
    def __init__(self, symbol: str): self.symbol = symbol.upper()

    def get_books(self, limit=5):
        ob = _require_orderbook(self.symbol, limit)
        return ob.bids[:limit], ob.asks[:limit]

    def ref_price(self) -> float:
        mp = _require_mark(self.symbol)
        return float(mp.mark)

    def qty_from_usd(self, V_usd: float) -> float:
        return V_usd / max(1e-12, self.ref_price())

    # 直接调用 FAPI 下单（与 exec_binance_rest 并行）
    def place_market(self, side: str, qty: float, reduce_only: bool=False):
        params = {
            "symbol": self.symbol,
            "side":   "BUY" if side=="BUY" else "SELL",
            "type":   "MARKET",
            "quantity": f"{float(qty):.8f}",
            "reduceOnly": "true" if reduce_only else "false",
            "newOrderRespType": "RESULT",
        }
        logging.info("Placing market order: %s", params)
        return r_signed(PAPI_BASE, "/papi/v1/um/order", "POST", params, PAPI_KEY, PAPI_SECRET)

    def place_limit_maker(self, side: str, qty: float, px: float):
        params = {
            "symbol": self.symbol,
            "side":   "BUY" if side=="BUY" else "SELL",
            "type":   "LIMIT",
            "timeInForce": "GTX",
            "quantity": f"{float(qty):.8f}",
            "price":    f"{float(px):.2f}",
            "newOrderRespType": "RESULT",
        }
        logging.info("Placing limit maker order: %s", params)
        return r_signed(PAPI_BASE, "/papi/v1/um/order", "POST", params, PAPI_KEY, PAPI_SECRET)

    def get_order_status(self, order_id: int):
        r = r_signed(PAPI_BASE, "/papi/v1/um/order", "GET", {"symbol": self.symbol, "orderId": int(order_id)}, PAPI_KEY, PAPI_SECRET)
        return r.get("status",""), float(r.get("executedQty", 0.0) or 0.0)

    def cancel(self, order_id: int):
        return r_signed(PAPI_BASE, "/papi/v1/um/order", "DELETE", {"symbol": self.symbol, "orderId": int(order_id)}, PAPI_KEY, PAPI_SECRET)

    def is_perp(self) -> bool: return True

# ---------------------------------------------------------------------
# 工厂
# ---------------------------------------------------------------------
def make_leg(kind: str, symbol: str) -> BaseLeg:
    k = (kind or "").lower()
    if k == "spot":
        return SpotLeg(symbol)
    if k == "coinm": return CoinMLeg(symbol)
    if k in ("usdtm","usdcm"): return UMLeg(symbol)
    raise ValueError(f"unsupported leg kind for PAPI-only build: {kind}")
