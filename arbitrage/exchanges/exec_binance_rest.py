from arbitrage.config import SPOT_SYMBOL, COINM_SYMBOL, DRY_RUN
from arbitrage.exchanges.binance_rest import spot_signed, dapi_signed
from arbitrage.exchanges.binance_rest import spot_get, dapi_get  # 可用于调试

def place_spot_limit_maker(side, qty, price, symbol: str | None = None):
    sym = symbol or SPOT_SYMBOL
    params = {"symbol": sym,"side": side,"type": "LIMIT_MAKER",
              "quantity": f"{qty:.8f}","price": f"{price:.2f}","newOrderRespType":"RESULT"}
    if DRY_RUN:
        print(f"[DRY] SPOT {sym} {side} {params['quantity']} @ {params['price']}")
        return {"orderId": None}
    return spot_signed("/api/v3/order","POST",params)

def place_spot_market(side, qty, symbol: str | None = None):
    sym = symbol or SPOT_SYMBOL
    params = {"symbol": sym,"side": side,"type": "MARKET","quantity": f"{qty:.8f}"}
    if DRY_RUN:
        print(f"[DRY] SPOT {sym} {side} MARKET {params['quantity']}")
        return {"orderId": None}
    return spot_signed("/api/v3/order","POST",params)

def place_coinm_limit(side, contracts, price, post_only=True, symbol: str | None = None):
    sym = symbol or COINM_SYMBOL
    params = {"symbol": sym,"side": side,"type": "LIMIT",
              "timeInForce": "GTX" if post_only else "GTC",
              "quantity": str(int(contracts)),"price": f"{price:.1f}",
              "newOrderRespType": "RESULT"}
    if DRY_RUN:
        print(f"[DRY] COIN-M {sym} {side} {params['quantity']} @ {params['price']} (postOnly={post_only})")
        return {"orderId": None}
    return dapi_signed("/dapi/v1/order","POST",params)

def place_coinm_market(side, contracts, reduce_only=True, symbol: str | None = None):
    sym = symbol or COINM_SYMBOL
    params = {"symbol": sym,"side": side,"type":"MARKET",
              "quantity": str(int(contracts)),"reduceOnly": "true" if reduce_only else "false",
              "newOrderRespType": "RESULT"}
    if DRY_RUN:
        print(f"[DRY] COIN-M {sym} {side} MARKET {params['quantity']} (reduceOnly={reduce_only})")
        return {"orderId": None}
    return dapi_signed("/dapi/v1/order","POST",params)

def get_spot_order_status(order_id: int, symbol: str | None = None):
    try:
        sym = symbol or SPOT_SYMBOL
        resp = spot_signed("/api/v3/order","GET",{"symbol": sym, "orderId": order_id})
        return resp.get("status",""), float(resp.get("executedQty",0.0) or 0.0)
    except Exception as e:
        print("⚠ 现货订单查询失败：", e); return "ERROR", 0.0

def get_coinm_order_status(order_id: int, symbol: str | None = None):
    try:
        sym = symbol or COINM_SYMBOL
        resp = dapi_signed("/dapi/v1/order","GET",{"symbol": sym, "orderId": order_id})
        exec_qty = float(resp.get("executedQty", resp.get("cumQty", 0.0)) or 0.0)
        return resp.get("status",""), exec_qty
    except Exception as e:
        print("⚠ 合约订单查询失败：", e); return "ERROR", 0.0

# 账户与成交明细（给风控与记账用）
def dapi_position_risk():
    return dapi_signed("/dapi/v1/positionRisk","GET",{})

def dapi_account():
    return dapi_signed("/dapi/v1/account","GET",{})

def spot_trades_by_order(order_id: int, symbol: str | None = None):
    sym = symbol or SPOT_SYMBOL
    return spot_signed("/api/v3/myTrades","GET",{"symbol": sym, "orderId": order_id})

def dapi_user_trades(order_id: int, symbol: str | None = None):
    sym = symbol or COINM_SYMBOL
    return dapi_signed("/dapi/v1/userTrades","GET",{"symbol": sym, "orderId": order_id})

def dapi_income_since(start_ms: int, symbol: str | None = None):
    sym = symbol or COINM_SYMBOL
    return dapi_signed("/dapi/v1/income","GET",{"symbol": sym, "startTime": start_ms})

def cancel_spot_order(order_id: int):
    try:
        return spot_signed("/api/v3/order","DELETE",{"symbol": SPOT_SYMBOL, "orderId": order_id})
    except Exception as e:
        print("⚠ 现货撤单失败：", e); return {}

def cancel_coinm_order(order_id: int):
    try:
        return dapi_signed("/dapi/v1/order","DELETE",{"symbol": COINM_SYMBOL, "orderId": order_id})
    except Exception as e:
        print("⚠ 合约撤单失败：", e); return {}