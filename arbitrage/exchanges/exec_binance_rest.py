# -*- coding: utf-8 -*-
# arbitrage/exchanges/exec_binance_rest.py
"""
现货 / 币本位合约 执行封装（轻量版）
- 现货：
    place_spot_market(side, qty, symbol)
    place_spot_limit_maker(side, qty, price, symbol)
    get_spot_order_status(order_id, symbol)
    cancel_spot_order(order_id, symbol)
- 币本位（COIN-M）：
    place_coinm_market(side, quantity_cntr, reduce_only, symbol)
    place_coinm_limit(side, quantity_cntr, price, post_only, symbol)
    get_coinm_order_status(order_id, symbol)
    cancel_coinm_order(order_id, symbol)
备注：
- 数量/价格未做步长对齐，若报最小下单量错误请在上游对齐或放大名义
"""

from __future__ import annotations
import logging
import os
from typing import Tuple, Any

from arbitrage.exchanges.binance_rest import r_signed
from arbitrage.config import (
    SPOT_BASE, DAPI_BASE, SPOT_KEY, SPOT_SECRET, DAPI_KEY, DAPI_SECRET
)

# =========================================================
# 现货 Spot
# =========================================================
def place_spot_market(side: str, qty: float, symbol: str) -> Any:
    """
    市价单（RESULT）
    side: "BUY"/"SELL"
    """
    params = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": "MARKET",
        "quantity": f"{float(qty):.5f}",
        "newOrderRespType": "RESULT",
    }
    r =  r_signed(SPOT_BASE, "/api/v3/order", "POST", params, SPOT_KEY, SPOT_SECRET)
    logging.info("Response: %s", r)
    return r

def place_spot_limit_maker(side: str, qty: float, price: float, symbol: str) -> Any:
    """
    限价 Post-Only：Binance 现货用 "LIMIT_MAKER"
    """
    params = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": "LIMIT_MAKER",
        "quantity": f"{float(qty):.5f}",
        "price": f"{float(price):.5f}",
        "newOrderRespType": "RESULT",
    }
    return r_signed(SPOT_BASE, "/api/v3/order", "POST", params, SPOT_KEY, SPOT_SECRET)

def get_spot_order_status(order_id: int, symbol: str) -> Tuple[str, float]:
    """
    返回 (status, executedQty)
    """
    params = {"symbol": symbol.upper(), "orderId": int(order_id)}
    j = r_signed(SPOT_BASE, "/api/v3/order", "GET", params, SPOT_KEY, SPOT_SECRET)
    return j.get("status", ""), float(j.get("executedQty", 0.0) or 0.0)

def cancel_spot_order(order_id: int, symbol: str) -> Any:
    params = {"symbol": symbol.upper(), "orderId": int(order_id)}
    return r_signed(SPOT_BASE, "/api/v3/order", "DELETE", params, SPOT_KEY, SPOT_SECRET)

# =========================================================
# 币本位合约 COIN-M (dapi)
# =========================================================
def place_coinm_market(side: str, quantity_cntr: int, reduce_only: bool, symbol: str) -> Any:
    """
    市价单（张为单位）
    - reduce_only: True/False
    """
    params = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": "MARKET",
        "quantity": int(quantity_cntr),
        "reduceOnly": "true" if reduce_only else "false",
        "newOrderRespType": "RESULT",
    }
    return r_signed(DAPI_BASE, "/dapi/v1/order", "POST", params, DAPI_KEY, DAPI_SECRET)

def place_coinm_limit(side: str, quantity_cntr: int, price: float, post_only: bool, symbol: str) -> Any:
    """
    限价单；post_only=True 时使用 GTX（Post Only）
    """
    params = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": "LIMIT",
        "timeInForce": "GTX" if post_only else "GTC",
        "quantity": int(quantity_cntr),
        "price": f"{float(price):.2f}",
        "newOrderRespType": "RESULT",
    }
    return r_signed(DAPI_BASE, "/dapi/v1/order", "POST", params, DAPI_KEY, DAPI_SECRET)

def get_coinm_order_status(order_id: int, symbol: str) -> Tuple[str, float]:
    params = {"symbol": symbol.upper(), "orderId": int(order_id)}
    j = r_signed(DAPI_BASE, "/dapi/v1/order", "GET", params, DAPI_KEY, DAPI_SECRET)
    # 币本位返回的 executedQty（张）；为统一这里仍返回 float
    return j.get("status", ""), float(j.get("executedQty", 0.0) or 0.0)

def cancel_coinm_order(order_id: int, symbol: str) -> Any:
    params = {"symbol": symbol.upper(), "orderId": int(order_id)}
    return r_signed(DAPI_BASE, "/dapi/v1/order", "DELETE", params, DAPI_KEY, DAPI_SECRET)
