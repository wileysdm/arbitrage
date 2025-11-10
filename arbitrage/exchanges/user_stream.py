# -*- coding: utf-8 -*-
# arbitrage/exchanges/user_stream.py
"""
Binance User Data Stream（Spot / COIN-M / USDⓈ-M）
- 负责创建 listenKey、维持 keepalive、消费 WS 事件
- 将成交/账户事件发布到 data.bus 的 Topic.EXEC_FILLS（key=市场类型）
事件负载（示例）：
{
  "market": "um" | "cm" | "spot",
  "event": "execution",
  "symbol": "BTCUSDT",
  "orderId": 123456,
  "side": "BUY",
  "status": "FILLED",
  "lastPx":  62000.5,
  "lastQty": 0.001,
  "execTs":  1719999999.123,
  "cumQty":  0.005,
  "cumQuote": 310.0
}
"""
from __future__ import annotations
import os, ssl, json, time, asyncio
import websockets
from typing import Optional, Dict, Any

from arbitrage.exchanges.binance_rest import r_post, r_put, r_get  # 使用你已有 HTTP 底座（带代理/重试）
from arbitrage.data.bus import Bus, Topic
from arbitrage.data.service import DataService

# to-do 挪到 config.py
# 端点
SPOT_BASE = os.environ.get("SPOT_BASE", "https://api.binance.com")
DAPI_BASE = os.environ.get("DAPI_BASE", "https://dapi.binance.com")
FAPI_BASE = os.environ.get("FAPI_BASE", "https://fapi.binance.com")

WS_SPOT = "wss://stream.binance.com:9443"
WS_CM   = "wss://dstream.binance.com"
WS_UM   = "wss://fstream.binance.com"

SPOT_KEY = os.environ.get("SPOT_KEY", "")
SPOT_SECRET = os.environ.get("SPOT_SECRET", "")
DAPI_KEY = os.environ.get("DAPI_KEY", SPOT_KEY)
DAPI_SECRET = os.environ.get("DAPI_SECRET", SPOT_SECRET)
UM_KEY = os.environ.get("UM_KEY", SPOT_KEY)
UM_SECRET = os.environ.get("UM_SECRET", SPOT_SECRET)

KEEPALIVE_SEC = 30 * 60

BUS = DataService.global_service().bus  # 直接用全局数据总线

# -------------------------------------------------------------------
# listenKey / keepalive
# -------------------------------------------------------------------
async def _create_listen_key(base: str, path: str, key: str, secret: str) -> str:
    # 这些接口不需要签名，但你 binance_rest.r_post 会自动带 key header
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

def _parse_spot(msg: dict):
    # Spot: executionReport
    d = msg
    if d.get("e") != "executionReport":
        return
    payload = dict(
        market="spot", event="execution",
        symbol=d.get("s"), orderId=d.get("i"),
        side=d.get("S"), status=d.get("X"),
        lastPx=float(d.get("L", 0) or 0.0),
        lastQty=float(d.get("l", 0) or 0.0),
        execTs=float(d.get("E", 0) / 1000),
        cumQty=float(d.get("z", 0) or 0.0),
        cumQuote=float(d.get("Z", 0) or 0.0),
    )
    _publish_exec("spot", payload)

def _parse_um(msg: dict):
    # UM: ORDER_TRADE_UPDATE
    d = msg.get("data") or msg
    if d.get("e") != "ORDER_TRADE_UPDATE":
        return
    o = d.get("o", {})
    payload = dict(
        market="um", event="execution",
        symbol=o.get("s"), orderId=o.get("i"),
        side=o.get("S"), status=o.get("X"),
        lastPx=float(o.get("L", 0) or 0.0),
        lastQty=float(o.get("l", 0) or 0.0),
        execTs=float(d.get("E", 0) / 1000),
        cumQty=float(o.get("z", 0) or 0.0),
        cumQuote=float(o.get("Z", 0) or 0.0),
        reduceOnly=bool(o.get("R", False)),
    )
    _publish_exec("um", payload)

def _parse_cm(msg: dict):
    # CM: ORDER_TRADE_UPDATE
    d = msg.get("data") or msg
    if d.get("e") != "ORDER_TRADE_UPDATE":
        return
    o = d.get("o", {})
    payload = dict(
        market="cm", event="execution",
        symbol=o.get("s"), orderId=o.get("i"),
        side=o.get("S"), status=o.get("X"),
        lastPx=float(o.get("L", 0) or 0.0),
        lastQty=float(o.get("l", 0) or 0.0),
        execTs=float(d.get("E", 0) / 1000),
        cumQty=float(o.get("z", 0) or 0.0),
        cumQuote=float(o.get("Z", 0) or 0.0),
        reduceOnly=bool(o.get("R", False)),
    )
    _publish_exec("cm", payload)

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
async def run_spot_user_stream():
    lk = await _create_listen_key(SPOT_BASE, "/api/v3/userDataStream", SPOT_KEY, SPOT_SECRET)
    url = f"{WS_SPOT}/ws/{lk}"
    asyncio.create_task(_keepalive_loop(SPOT_BASE, "/api/v3/userDataStream", SPOT_KEY, lk))
    await _run_ws(url, _parse_spot, "spot-uds")

async def run_um_user_stream():
    lk = await _create_listen_key(FAPI_BASE, "/fapi/v1/listenKey", UM_KEY, UM_SECRET)
    url = f"{WS_UM}/ws/{lk}"
    asyncio.create_task(_keepalive_loop(FAPI_BASE, "/fapi/v1/listenKey", UM_KEY, lk))
    await _run_ws(url, _parse_um, "um-uds")

async def run_cm_user_stream():
    lk = await _create_listen_key(DAPI_BASE, "/dapi/v1/listenKey", DAPI_KEY, DAPI_SECRET)
    url = f"{WS_CM}/ws/{lk}"
    asyncio.create_task(_keepalive_loop(DAPI_BASE, "/dapi/v1/listenKey", DAPI_KEY, lk))
    await _run_ws(url, _parse_cm, "cm-uds")

async def run_all_user_streams(enable_spot=True, enable_um=True, enable_cm=True):
    tasks = []
    if enable_spot: tasks.append(asyncio.create_task(run_spot_user_stream()))
    if enable_um:   tasks.append(asyncio.create_task(run_um_user_stream()))
    if enable_cm:   tasks.append(asyncio.create_task(run_cm_user_stream()))
    if tasks:
        await asyncio.gather(*tasks)
