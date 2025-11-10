# -*- coding: utf-8 -*-
# arbitrage/data/adapters/binance/ws_orderbook.py
"""
Binance WS - Partial Book Depth 适配（spot/coinm/um）
- run_orderbook_ws(kind, symbol, bus, levels=10, speed_ms=100)
  每条消息发布一个 OrderBook 快照（bids/asks 皆为浮点元组）
"""
from __future__ import annotations
import ssl, json, time, asyncio

# websockets 库（async）
import websockets

from arbitrage.data.schemas import OrderBook

from arbitrage.data.bus import Bus, Topic

# ---- WS 端点 ----
def _ws_base(kind: str) -> str:
    k = kind.lower()
    if k == "spot":  return "wss://stream.binance.com:9443"
    if k == "coinm": return "wss://dstream.binance.com"
    if k in ("usdtm","usdcm"): return "wss://fstream.binance.com"
    raise ValueError(f"unknown kind: {kind}")

def _stream_name(kind: str, symbol: str, levels: int, speed_ms: int) -> str:
    # lowercase 要求
    return f"{symbol.lower()}@depth{int(levels)}@{int(speed_ms)}ms"

# ---- 解析工具 ----
def _extract_bids_asks(msg: dict):
    d = msg.get("data") or msg  # 兼容 combined stream 包裹
    bids = d.get("bids") or d.get("b") or []
    asks = d.get("asks") or d.get("a") or []
    bids = [(float(p), float(q)) for p, q in bids]
    asks = [(float(p), float(q)) for p, q in asks]
    return bids, asks

# ---- 主循环 ----
async def run_orderbook_ws(kind: str,
                           symbol: str,
                           bus: Bus,
                           levels: int = 10,
                           speed_ms: int = 100,
                           reconnect_delay: float = 1.0,
                           max_delay: float = 30.0):
    base = _ws_base(kind)
    stream = _stream_name(kind, symbol, levels, speed_ms)
    url = f"{base}/ws/{stream}"
    ssl_ctx = ssl.create_default_context()
    # 代理（若 Clash/系统代理生效通常无需设置；websockets 不直接支持 http(s) 代理透传）
    delay = reconnect_delay

    while True:
        try:
            async with websockets.connect(url, ssl=ssl_ctx, ping_interval=15, ping_timeout=10, max_queue=64) as ws:
                print(f"[WS depth] connected: {kind}:{symbol} {levels}@{speed_ms}ms")
                delay = reconnect_delay  # 成功后重置回退

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        bids, asks = _extract_bids_asks(msg)
                        if bids and asks:
                            ob = OrderBook(symbol=symbol.upper(), ts=time.time(), bids=bids, asks=asks)
                            bus.publish(Topic.ORDERBOOK, ob.symbol, ob)
                    except Exception as ie:
                        # 单条消息解析容错
                        print(f"[WS depth] parse err: {ie}")
        except Exception as e:
            print(f"[WS depth] {kind}:{symbol} disconnected: {e}. Reconnect in {delay:.1f}s")
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, max_delay)
