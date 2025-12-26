# -*- coding: utf-8 -*-
# arbitrage/exchanges/exec_binance_rest.py
"""币本位合约（COIN-M）执行封装（统一账户 / PAPI 版本）。

仅保留 COIN-M 下单/撤单/查单：
- PAPI: /papi/v1/cm/order

备注：数量/价格未做步长对齐，若报最小下单量错误请在上游对齐或放大名义。
"""

from __future__ import annotations
import logging
import os
from typing import Tuple, Any

from arbitrage.exchanges.binance_rest import r_signed
from arbitrage.config import PAPI_BASE, PAPI_KEY, PAPI_SECRET

# =========================================================
# 币本位合约 COIN-M (PAPI)
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
    return r_signed(PAPI_BASE, "/papi/v1/cm/order", "POST", params, PAPI_KEY, PAPI_SECRET)

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
    return r_signed(PAPI_BASE, "/papi/v1/cm/order", "POST", params, PAPI_KEY, PAPI_SECRET)

def get_coinm_order_status(order_id: int, symbol: str) -> Tuple[str, float]:
    params = {"symbol": symbol.upper(), "orderId": int(order_id)}
    j = r_signed(PAPI_BASE, "/papi/v1/cm/order", "GET", params, PAPI_KEY, PAPI_SECRET)
    # 币本位返回的 executedQty（张）；为统一这里仍返回 float
    return j.get("status", ""), float(j.get("executedQty", 0.0) or 0.0)

def cancel_coinm_order(order_id: int, symbol: str) -> Any:
    params = {"symbol": symbol.upper(), "orderId": int(order_id)}
    return r_signed(PAPI_BASE, "/papi/v1/cm/order", "DELETE", params, PAPI_KEY, PAPI_SECRET)
