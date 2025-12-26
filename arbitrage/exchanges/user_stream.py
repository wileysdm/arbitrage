"""Binance Portfolio Margin (PAPI) User Data Stream.

统一账户只需要一条 UDS：
- REST: POST/PUT /papi/v1/listenKey (base=https://papi.binance.com)
- WS:   wss://fstream.binance.com/pm/ws/<listenKey>

消费 WS 事件并将成交事件发布到 Bus 的 Topic.EXEC_FILLS（key=um/cm）。
"""

from __future__ import annotations

import asyncio
import json
import ssl

import websockets
from typing import Optional, Dict, Any

from arbitrage.exchanges.binance_rest import r_post, r_put
from arbitrage.data.bus import Topic
from arbitrage.data.service import DataService
from arbitrage.config import PAPI_BASE, PAPI_KEY, WS_PM

KEEPALIVE_SEC = 30 * 60

BUS = DataService.global_service().bus

# -------------------------------------------------------------------
# listenKey / keepalive
# -------------------------------------------------------------------
async def _create_listen_key(base: str, path: str, key: str) -> str:
    j = r_post(base, path, {}, key=key)
    return j["listenKey"]

async def _keepalive_loop(base: str, path: str, key: str, listen_key: str):
    while True:
        await asyncio.sleep(KEEPALIVE_SEC - 30)
        try:
            _ = r_put(base, path, {"listenKey": listen_key}, key=key)
        except Exception as e:
            print(f"[UDS keepalive] {base} {e}")

# -------------------------------------------------------------------
# 解析与发布
# -------------------------------------------------------------------
def _publish_exec(market: str, payload: Dict[str, Any]):
    BUS.publish(Topic.EXEC_FILLS, market, payload)


def _infer_futures_market_from_symbol(symbol: Optional[str]) -> str:
    if not symbol:
        return "um"
    s = symbol.upper()
    # UM 常见：BTCUSDT/BTCUSDC；CM 常见：BTCUSD_PERP/BTCUSD_YYMMDD
    if s.endswith("USDT") or s.endswith("USDC"):
        return "um"
    return "cm"


def _parse_papi(msg: dict):
    # Portfolio Margin UDS：
    # - futures: ORDER_TRADE_UPDATE
    # - margin:  executionReport
    d = msg.get("data") or msg

    et = d.get("e")
    if et == "ORDER_TRADE_UPDATE":
        o = d.get("o", {})
        symbol = o.get("s")
        market = _infer_futures_market_from_symbol(symbol)
        payload = dict(
            market=market,
            event="execution",
            symbol=symbol,
            orderId=o.get("i"),
            side=o.get("S"),
            status=o.get("X"),
            lastPx=float(o.get("L", 0) or 0.0),
            lastQty=float(o.get("l", 0) or 0.0),
            execTs=float(d.get("E", 0) / 1000),
            cumQty=float(o.get("z", 0) or 0.0),
            cumQuote=float(o.get("Z", 0) or 0.0),
            reduceOnly=bool(o.get("R", False)),
        )
        _publish_exec(market, payload)
        return

    if et == "executionReport":
        # Margin order update
        symbol = d.get("s")
        market = "spot"
        payload = dict(
            market=market,
            event="execution",
            symbol=symbol,
            orderId=d.get("i"),
            side=d.get("S"),
            status=d.get("X"),
            lastPx=float(d.get("L", 0) or 0.0),
            lastQty=float(d.get("l", 0) or 0.0),
            execTs=float(d.get("E", 0) / 1000),
            cumQty=float(d.get("z", 0) or 0.0),
            cumQuote=float(d.get("Z", 0) or 0.0),
        )
        _publish_exec(market, payload)
        return

# -------------------------------------------------------------------
# WS 主循环
# -------------------------------------------------------------------
async def _run_ws(url: str, on_msg, name: str):
    ssl_ctx = ssl.create_default_context()
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(url, ssl=ssl_ctx, ping_interval=15, ping_timeout=10, max_queue=256) as ws:
                print(f"[UDS] connected: {name}")
                backoff = 1.0
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        on_msg(msg)
                    except Exception as ie:
                        print(f"[UDS] parse err: {ie}")
        except Exception as e:
            print(f"[UDS] {name} disconnected: {e}; reconnect in {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.7, 60.0)

# -------------------------------------------------------------------
# 对外启动器
# -------------------------------------------------------------------
async def run_user_stream():
    lk = await _create_listen_key(PAPI_BASE, "/papi/v1/listenKey", PAPI_KEY)
    url = f"{WS_PM}/ws/{lk}"
    asyncio.create_task(_keepalive_loop(PAPI_BASE, "/papi/v1/listenKey", PAPI_KEY, lk))
    await _run_ws(url, _parse_papi, "papi-uds")
