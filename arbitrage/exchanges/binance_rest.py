# -*- coding: utf-8 -*-
import requests
from typing import Dict, Any, Optional
from arbitrage.config import SPOT_BASE, DAPI_BASE, SPOT_KEY, SPOT_SECRET, DAPI_KEY, DAPI_SECRET
from arbitrage.utils import ts_ms, sign_query

_session = requests.Session()
_session.headers.update({"Content-Type": "application/x-www-form-urlencoded"})

def _hdr(key): 
    return {"X-MBX-APIKEY": key} if key else {}

def r_get(base, path, params=None, key=None):
    url = base + path
    r = _session.get(url, params=params or {}, headers=_hdr(key), timeout=10)
    r.raise_for_status()
    return r.json()

def r_post(base, path, params=None, key=None):
    url = base + path
    r = _session.post(url, params=params or {}, headers=_hdr(key), timeout=10)
    r.raise_for_status()
    return r.json()

def r_put(base, path, params=None, key=None):
    url = base + path
    r = _session.put(url, params=params or {}, headers=_hdr(key), timeout=10)
    r.raise_for_status()
    return r.json()

def r_signed(base, path, method, params, key, secret):
    payload = {**(params or {}), "timestamp": ts_ms(), "recvWindow": 5000}
    body_qs = sign_query(secret, payload)  # "k=v&...&signature=..."
    url = base + path
    m = method.upper()
    if m == "GET":
        r = _session.get(url + "?" + body_qs, headers=_hdr(key), timeout=10)
    elif m == "POST":
        r = _session.post(url, data=body_qs, headers=_hdr(key), timeout=10)
    elif m == "DELETE":
        r = _session.delete(url, data=body_qs, headers=_hdr(key), timeout=10)
    elif m == "PUT":
        r = _session.put(url, data=body_qs, headers=_hdr(key), timeout=10)
    else:
        raise ValueError(f"unsupported method: {method}")
    r.raise_for_status()
    return r.json()

# ===== 现货（SPOT）=====
def spot_get(path, params=None):
    return r_get(SPOT_BASE, path, params, key=SPOT_KEY)

def spot_post(path, params=None):
    return r_post(SPOT_BASE, path, params, key=SPOT_KEY)

def spot_put(path, params=None):
    return r_put(SPOT_BASE, path, params, key=SPOT_KEY)

def spot_signed(path, method, params):
    return r_signed(SPOT_BASE, path, method, params, SPOT_KEY, SPOT_SECRET)

# ===== 币本位（COIN-M / dapi）=====
def dapi_get(path, params=None):
    return r_get(DAPI_BASE, path, params, key=DAPI_KEY)

def dapi_post(path, params=None):
    return r_post(DAPI_BASE, path, params, key=DAPI_KEY)

def dapi_put(path, params=None):
    return r_put(DAPI_BASE, path, params, key=DAPI_KEY)

def dapi_signed(path, method, params):
    return r_signed(DAPI_BASE, path, method, params, DAPI_KEY, DAPI_SECRET)

# ===== 交易对元数据（tick/step/合约面值）=====
def _parse_filters(filters):
    out = {"price_tick": None, "qty_step": None, "min_qty": None, "min_notional": None}
    for f in filters or []:
        ft = f.get("filterType")
        if ft == "PRICE_FILTER":
            out["price_tick"] = float(f["tickSize"])
        elif ft == "LOT_SIZE":
            out["qty_step"] = float(f["stepSize"])
            out["min_qty"] = float(f.get("minQty", 0.0))
        elif ft in ("NOTIONAL", "MIN_NOTIONAL"):
            v = f.get("notional") or f.get("minNotional")
            if v is not None:
                out["min_notional"] = float(v)
    return out

def spot_symbol_meta(symbol: str) -> Dict[str, Any]:
    """现货：/api/v3/exchangeInfo?symbol=BTCUSDT"""
    j = spot_get("/api/v3/exchangeInfo", {"symbol": symbol.upper()})
    syms = j.get("symbols", [])
    if not syms:
        raise RuntimeError(f"spot exchangeInfo no symbol: {symbol}")
    s = syms[0]
    flt = _parse_filters(s.get("filters"))
    return {
        "symbol": s["symbol"],
        "price_tick": flt["price_tick"],
        "qty_step": flt["qty_step"],
        "min_qty": flt["min_qty"],
        "min_notional": flt["min_notional"],
        "price_precision": s.get("pricePrecision"),
        "quantity_precision": s.get("quantityPrecision"),
    }

def dapi_symbol_meta(symbol: str) -> Dict[str, Any]:
    """COIN-M：/dapi/v1/exchangeInfo?symbol=BTCUSD_PERP（含 contractSize=每张USD面值V）"""
    j = dapi_get("/dapi/v1/exchangeInfo", {"symbol": symbol.upper()})
    syms = j.get("symbols", [])
    if not syms:
        raise RuntimeError(f"dapi exchangeInfo no symbol: {symbol}")
    s = syms[0]
    flt = _parse_filters(s.get("filters"))
    contract_size = float(s.get("contractSize", 0)) if s.get("contractSize") is not None else None
    return {
        "symbol": s["symbol"],
        "price_tick": flt["price_tick"],
        "qty_step": flt["qty_step"],
        "min_qty": flt["min_qty"],
        "min_notional": flt["min_notional"],
        "price_precision": s.get("pricePrecision"),
        "quantity_precision": s.get("quantityPrecision"),
        "contract_size": contract_size,  # V（USD/张）
    }
