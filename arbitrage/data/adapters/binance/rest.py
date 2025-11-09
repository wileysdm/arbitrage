# -*- coding: utf-8 -*-
# arbitrage/data/adapters/binance/rest.py
"""
Binance REST 适配：统一拿 depth / mark / exchangeInfo（spot/coinm/um）
- 公开函数：
    get_depth(kind, symbol, limit=20) -> (bids, asks)
    get_mark(kind, symbol) -> float | None
    get_meta(kind, symbol) -> Meta(dict-like)
- 可选轮询发布：
    poll_once_orderbook(kind, symbol, bus)
    poll_once_mark(kind, symbol, bus)
    poll_loop_orderbook(..., interval=0.5)
    poll_loop_mark(..., interval=1.0)
"""
from __future__ import annotations
import os, time, json
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- 兜底：schemas / bus（若尚未创建 data 层模块） ----
try:
    from arbitrage.data.schemas import OrderBook, MarkPrice, Meta
except Exception:
    @dataclass
    class OrderBook:
        symbol: str
        ts: float
        bids: List[Tuple[float, float]]
        asks: List[Tuple[float, float]]

    @dataclass
    class MarkPrice:
        symbol: str
        ts: float
        mark: float
        index: Optional[float] = None

    @dataclass
    class Meta:
        symbol: str
        kind: str
        contract_size: Optional[float] = None
        price_tick: Optional[float] = None
        qty_step: Optional[float] = None

try:
    from arbitrage.data.bus import Bus, Topic
except Exception:
    class Topic:
        ORDERBOOK = "orderbook"
        MARK = "mark"
    class Bus:
        def publish(self, topic: str, key: str, value):  # noqa
            # 兜底：仅打印
            print(f"[NoopBus] {topic} {key} -> {value}")

# ---- 端点 & 会话 ----
def _def(v, default):  # 小工具
    return v if v else default

SPOT_BASE = _def(os.environ.get("SPOT_BASE"), "https://api.binance.com")
DAPI_BASE = _def(os.environ.get("DAPI_BASE"), "https://dapi.binance.com")
FAPI_BASE = _def(os.environ.get("FAPI_BASE"), "https://fapi.binance.com")

def _session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.2, status_forcelist=(429, 500, 502, 503, 504))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    # 显式代理（若 Clash 等设置了系统代理，也可不配）
    proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    s.headers.update({"User-Agent": "arb-data/1.0"})
    return s

def _get(base: str, path: str, params: Optional[Dict]=None) -> dict:
    url = base + path
    r = _session().get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

# ---- 工具：域名/路径选择 ----
def _depth_base(kind: str) -> str:
    k = kind.lower()
    if k == "spot":  return SPOT_BASE
    if k == "coinm": return DAPI_BASE
    if k in ("usdtm", "usdcm"): return FAPI_BASE
    raise ValueError(f"unknown kind: {kind}")

def _info_path(kind: str) -> str:
    return "/api/v3/exchangeInfo" if kind=="spot" else "/fapi/v1/exchangeInfo" if kind in ("usdtm","usdcm") else "/dapi/v1/exchangeInfo"

def _depth_path(kind: str) -> str:
    return "/api/v3/depth" if kind=="spot" else "/fapi/v1/depth" if kind in ("usdtm","usdcm") else "/dapi/v1/depth"

def _mark_path(kind: str) -> str:
    # 资金费标记价：UM/CM 用 premiumIndex；spot 无标记价，用 mid 近似（由 WS 或深度计算）
    if kind in ("usdtm","usdcm"): return "/fapi/v1/premiumIndex"
    if kind == "coinm":           return "/dapi/v1/premiumIndex"
    return ""  # spot

# ---- 核心 API ----
def get_depth(kind: str, symbol: str, limit: int = 20):
    """返回 [(px,qty)] 浮点列表（bids, asks）"""
    base = _depth_base(kind)
    path = _depth_path(kind)
    sym = symbol.upper()
    j = _get(base, path, {"symbol": sym, "limit": limit})
    # 兼容字段名 b/a & bids/asks
    bids = j.get("bids") or j.get("b") or []
    asks = j.get("asks") or j.get("a") or []
    bids = [(float(p), float(q)) for p, q in bids]
    asks = [(float(p), float(q)) for p, q in asks]
    return bids, asks

def get_mark(kind: str, symbol: str) -> Optional[float]:
    """UM/CM 返回标记价；spot 返回 None（请用 mid）"""
    if kind == "spot":
        return None
    base = _depth_base(kind)
    path = _mark_path(kind)
    sym = symbol.upper()
    j = _get(base, path, {"symbol": sym})
    if isinstance(j, dict) and j.get("symbol") == sym:
        return float(j["markPrice"])
    if isinstance(j, list):
        for it in j:
            if it.get("symbol") == sym:
                return float(it["markPrice"])
    raise RuntimeError(f"premiumIndex not found for {kind}:{symbol}")

def get_meta(kind: str, symbol: str) -> Meta:
    base = _depth_base(kind)
    path = _info_path(kind)
    sym = symbol.upper()
    j = _get(base, path, {"symbol": sym})
    arr = j.get("symbols", [])
    if not arr:  # spot 有时返回对象也含其它键
        arr = [j] if j.get("symbol") else []
    if not arr:
        return Meta(symbol=sym, kind=kind)

    s = arr[0]
    cs = None
    if kind == "coinm":
        cs = float(s.get("contractSize", 100.0))
    # 解析 filters
    price_tick = qty_step = min_qty = None
    for f in s.get("filters", []):
        ft = f.get("filterType")
        if ft == "PRICE_FILTER":
            price_tick = float(f.get("tickSize", 0.0))
        elif ft == "LOT_SIZE":
            qty_step = float(f.get("stepSize", 0.0))
            min_qty = float(f.get("minQty", 0.0))
    return Meta(symbol=sym, kind=kind, contract_size=cs, price_tick=price_tick, qty_step=qty_step)

# ---- 发布（可选） ----
def poll_once_orderbook(kind: str, symbol: str, bus: Bus):
    bids, asks = get_depth(kind, symbol, limit=20)
    ob = OrderBook(symbol=symbol.upper(), ts=time.time(), bids=bids, asks=asks)
    bus.publish(Topic.ORDERBOOK, ob.symbol, ob)
    return ob

def poll_once_mark(kind: str, symbol: str, bus: Bus):
    if kind == "spot":
        # 用 mid 近似
        bids, asks = get_depth(kind, symbol, limit=5)
        mid = (bids[0][0] + asks[0][0]) / 2.0
        mp = MarkPrice(symbol=symbol.upper(), ts=time.time(), mark=mid, index=None)
    else:
        mark = get_mark(kind, symbol)
        mp = MarkPrice(symbol=symbol.upper(), ts=time.time(), mark=mark, index=None)
    bus.publish(Topic.MARK, mp.symbol, mp)
    return mp

def poll_loop_orderbook(kind: str, symbol: str, bus: Bus, interval: float = 0.5):
    while True:
        try:
            poll_once_orderbook(kind, symbol, bus)
        except Exception as e:
            print(f"[REST depth] {kind}:{symbol} error: {e}")
        time.sleep(interval)

def poll_loop_mark(kind: str, symbol: str, bus: Bus, interval: float = 1.0):
    while True:
        try:
            poll_once_mark(kind, symbol, bus)
        except Exception as e:
            print(f"[REST mark] {kind}:{symbol} error: {e}")
        time.sleep(interval)
