# -*- coding: utf-8 -*-
# arbitrage/data/adapters/binance/ws_mark.py
"""
Binance WS - Mark/Index（UM/CM）；Spot 使用 bookTicker 近似中位价
- run_mark_ws(kind, symbol, bus)
"""
from __future__ import annotations
import os, ssl, json, time, asyncio
from dataclasses import dataclass
from typing import Optional

from websockets import client as websockets

# 兜底 schemas / bus
try:
    from arbitrage.data.schemas import MarkPrice
except Exception:
    @dataclass
    class MarkPrice:
        symbol: str
        ts: float
        mark: float
        index: Optional[float] = None

try:
    from arbitrage.data.bus import Bus, Topic
except Exception:
    class Topic:
        MARK = "mark"
    class Bus:
        def publish(self, topic, key, value):
            print(f"[NoopBus] {topic} {key} -> mark {value.mark}")

# ---- WS 端点 ----
def _ws_base(kind: str) -> str:
    k = kind.lower()
    if k == "spot":  return "wss://stream.binance.com:9443"
    if k == "coinm": return "wss://dstream.binance.com"
    if k in ("usdtm","usdcm"): return "wss://fstream.binance.com"
    raise ValueError(f"unknown kind: {kind}")

def _stream_name(kind: str, symbol: str) -> str:
    s = symbol.lower()
    if kind == "spot":
        # 用 bookTicker 估 mid
        return f"{s}@bookTicker"
    # UM/CM: 标记价事件（1s）
    return f"{s}@markPrice@1s"

# ---- 解析 ----
def _parse_mark(kind: str, msg: dict) -> float:
    d = msg.get("data") or msg
    if kind == "spot":
        # bookTicker: bestBid/bestAsk
        b = float(d.get("b") or d.get("bestBidPrice"))
        a = float(d.get("a") or d.get("bestAskPrice"))
        return (b + a) / 2.0
    # UM/CM: markPrice 事件
    # 字段可能为 "p" 或 "markPrice"
    if "p" in d:
        return float(d["p"])
    if "markPrice" in d:
        return float(d["markPrice"])
    raise KeyError("mark not found")

# ---- 主循环 ----
async def run_mark_ws(kind: str,
                      symbol: str,
                      bus: Bus,
                      reconnect_delay: float = 1.0,
                      max_delay: float = 30.0):
    base = _ws_base(kind)
    stream = _stream_name(kind, symbol)
    url = f"{base}/ws/{stream}"
    ssl_ctx = ssl.create_default_context()
    delay = reconnect_delay

    while True:
        try:
            async with websockets.connect(url, ssl=ssl_ctx, ping_interval=15, ping_timeout=10, max_queue=64) as ws:
                print(f"[WS mark] connected: {kind}:{symbol}")
                delay = reconnect_delay

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        mark = _parse_mark(kind, msg)
                        mp = MarkPrice(symbol=symbol.upper(), ts=time.time(), mark=mark, index=None)
                        bus.publish(Topic.MARK, mp.symbol, mp)
                    except Exception as ie:
                        print(f"[WS mark] parse err: {ie}")
        except Exception as e:
            print(f"[WS mark] {kind}:{symbol} disconnected: {e}. Reconnect in {delay:.1f}s")
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, max_delay)
