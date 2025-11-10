# -*- coding: utf-8 -*-
"""
工具函数：vwap、日志、ID 生成等
"""
from __future__ import annotations
import time
from typing import List, Tuple
from arbitrage.exchanges.binance_rest import spot_get, dapi_get

_trade_id_seed = int(time.time() * 1000)

def next_trade_id() -> int:
    global _trade_id_seed
    _trade_id_seed += 1
    return _trade_id_seed

def vwap_to_qty(levels: List[Tuple[float, float]], qty: float):
    """给定 [(px,qty)] 与目标数量，返回 (filled, vwap)"""
    remain = qty
    notional = 0.0
    filled = 0.0
    for px, q in levels:
        if remain <= 1e-12: break
        take = min(remain, q)
        notional += take * px
        filled += take
        remain -= take
    vwap = (notional / filled) if filled > 0 else None
    return filled, vwap

def append_trade_row(d: dict):
    """轻量日志：先简单打印，后续可落 CSV / DB"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    side = d.get("side","")
    evt  = d.get("event","")
    q    = d.get("Q_btc","")
    n    = d.get("N_cntr","")
    print(f"[TRADE] {ts} | {evt} {side} | Q={q} N={n} | {d}")


# ---- 规则获取（可以获取api调用格式） ----
def fetch_spot_rules(symbol: str):
    info = spot_get("/api/v3/exchangeInfo", {"symbol": symbol})
    sym = info["symbols"][0]
    price_tick = [f for f in sym["filters"] if f["filterType"]=="PRICE_FILTER"][0]["tickSize"]
    lot_step   = [f for f in sym["filters"] if f["filterType"]=="LOT_SIZE"][0]["stepSize"]
    return float(price_tick), float(lot_step)

def fetch_coinm_rules(symbol: str):
    info = dapi_get("/dapi/v1/exchangeInfo", {"symbol": symbol})
    sym = info["symbols"][0]
    contract_size = float(sym.get("contractSize", 100.0))
    price_tick = [f for f in sym["filters"] if f["filterType"]=="PRICE_FILTER"][0]["tickSize"]
    lot_step   = [f for f in sym["filters"] if f["filterType"]=="LOT_SIZE"][0]["stepSize"]
    return contract_size, float(price_tick), float(lot_step)