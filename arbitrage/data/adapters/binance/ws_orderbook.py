# -*- coding: utf-8 -*-
# arbitrage/data/adapters/binance/ws_orderbook.py
"""
Binance WS - 本地订单簿维护（REST 快照 + Diff Depth 增量）
- run_orderbook_ws(kind, symbol, bus, levels=10, speed_ms=100, snapshot_interval=30.0)

设计：
1）启动前，读取环境中的 WS / REST 端点（见 arbitrage.config）；
2）每次（重）连接时：
    - 先通过 REST depth 拿一份快照，构建本地 bids/asks；
    - 然后连接 `<symbol>@depth@{speed_ms}ms` diff depth 流；
    - 后续仅用 WS 的增量来维护本地簿；
3）每条增量应用后，把前 N 档封装成 OrderBook 发布到 Bus；
4）每 snapshot_interval 秒（默认 30s）或检测到 updateId 断档时，自动重新拉快照并重建本地簿。
"""
from __future__ import annotations

import os
import ssl
import json
import time
import asyncio
from typing import Dict, List, Tuple

import requests
import websockets

from arbitrage.data.schemas import OrderBook
from arbitrage.data.bus import Bus, Topic
from arbitrage.config import SPOT_BASE, DAPI_BASE, FAPI_BASE, WS_SPOT, WS_CM, WS_UM

# 默认参数
DEFAULT_LEVELS = 1
DEFAULT_SPEED_MS = 100
DEFAULT_SNAPSHOT_INTERVAL = float(os.environ.get("ORDERBOOK_SNAPSHOT_INTERVAL", "30"))  # 秒


# ---------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------
def _ws_base(kind: str) -> str:
    k = (kind or "").lower()
    if k == "spot":
        return WS_SPOT.rstrip("/")
    if k == "coinm":
        return WS_CM.rstrip("/")
    if k in ("usdtm", "usdcm"):
        return WS_UM.rstrip("/")
    raise ValueError(f"unknown kind for ws base: {kind!r}")


def _rest_base(kind: str) -> str:
    k = (kind or "").lower()
    if k == "spot":
        return SPOT_BASE.rstrip("/")
    if k == "coinm":
        return DAPI_BASE.rstrip("/")
    if k in ("usdtm", "usdcm"):
        return FAPI_BASE.rstrip("/")
    raise ValueError(f"unknown kind for rest base: {kind!r}")


def _fetch_depth_snapshot(kind: str, symbol: str, limit: int) -> Dict:
    """
    通过 REST /depth 拿一份快照。
    返回原始 JSON（包含 lastUpdateId / bids / asks）。
    """
    base = _rest_base(kind)
    k = (kind or "").lower()
    symbol = symbol.upper()

    if k == "spot":
        path = "/api/v3/depth"
    elif k in ("usdtm", "usdcm"):
        path = "/fapi/v1/depth"
    else:  # coinm
        path = "/dapi/v1/depth"

    url = base + path
    # requests 会自动读取 HTTP(S)_PROXY 环境变量；这里不额外处理代理
    resp = requests.get(url, params={"symbol": symbol, "limit": int(limit)}, timeout=3.0)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected depth snapshot for {kind}:{symbol}: {data!r}")
    return data


def _apply_updates(book: Dict[float, float], updates: List[List[str]]) -> None:
    """
    将 diff depth 的某一侧更新应用到本地簿：
    - qty > 0：upsert
    - qty == 0：删除该价位
    """
    if not updates:
        return
    for item in updates:
        if len(item) < 2:
            continue
        try:
            price = float(item[0])
            qty = float(item[1])
        except (TypeError, ValueError):
            continue
        if qty <= 0.0:
            book.pop(price, None)
        else:
            book[price] = qty


def _sorted_top(book: Dict[float, float], levels: int, reverse: bool) -> List[Tuple[float, float]]:
    """
    从本地簿中取出前 N 档：
    - bids: 价格降序
    - asks: 价格升序
    """
    if not book:
        return []
    items = [(p, q) for p, q in book.items() if q > 0]
    items.sort(key=lambda x: x[0], reverse=reverse)
    if levels and levels > 0:
        items = items[:levels]
    return items


# ---------------------------------------------------------------------
# 主协程
# ---------------------------------------------------------------------
async def run_orderbook_ws(
    kind: str,
    symbol: str,
    bus: Bus,
    levels: int = DEFAULT_LEVELS,
    speed_ms: int = DEFAULT_SPEED_MS,
    snapshot_interval: float | None = None,
):
    """
    维护本地订单簿并把前 N 档推到 Bus.Topic.ORDERBOOK。

    参数：
        kind: "spot" / "coinm" / "usdtm" / "usdcm"
        symbol: 交易对，例如 "BTCUSDT" / "BTCUSD_PERP"
        bus: 统一数据总线
        levels: 保存并发布的档数（默认 10）
        speed_ms: Diff Depth 更新频率（100 / 250 / 500）
        snapshot_interval: 多久强制重拉一次 REST 快照（秒）；为 None 时用环境变量
    """
    levels = int(levels or DEFAULT_LEVELS)
    speed_ms = int(speed_ms or DEFAULT_SPEED_MS)
    snapshot_interval = float(snapshot_interval or DEFAULT_SNAPSHOT_INTERVAL)

    k = (kind or "").lower()
    sym_u = symbol.upper()
    sym_l = symbol.lower()

    ws_base = _ws_base(k)
    # 使用 diff book depth 流：<symbol>@depth@{speed_ms}ms
    stream_name = f"{sym_l}@depth@{speed_ms}ms"
    url = f"{ws_base}/ws/{stream_name}"

    reconnect_delay = 1.0
    max_delay = 30.0

    ssl_ctx = ssl.create_default_context()

    while True:
        try:
            print(f"[WS depth] connecting {k}:{sym_u} url={url}")
            async with websockets.connect(
                url,
                ssl=ssl_ctx,
                ping_interval=15,
                ping_timeout=10,
                max_queue=128,
            ) as ws:
                print(f"[WS depth] connected {k}:{sym_u}")
                reconnect_delay = 1.0

                # ========== 初始化：REST depth 快照 ==========
                loop = asyncio.get_running_loop()
                try:
                    snap = await loop.run_in_executor(
                        None, _fetch_depth_snapshot, k, sym_u, max(levels, 50)
                    )
                except Exception as se:
                    print(f"[WS depth] snapshot err {k}:{sym_u}: {se}")
                    # 快照都拿不到，直接触发外层重连
                    raise

                last_u = snap.get("lastUpdateId")
                if isinstance(last_u, str):
                    try:
                        last_u = int(last_u)
                    except ValueError:
                        pass

                bids_book: Dict[float, float] = {}
                asks_book: Dict[float, float] = {}
                _apply_updates(bids_book, snap.get("bids", []))
                _apply_updates(asks_book, snap.get("asks", []))

                last_snapshot_ts = time.time()

                # 先发布一次快照
                bids = _sorted_top(bids_book, levels, reverse=True)
                asks = _sorted_top(asks_book, levels, reverse=False)
                if bids and asks:
                    ob = OrderBook(symbol=sym_u, ts=last_snapshot_ts, bids=bids, asks=asks)
                    bus.publish(Topic.ORDERBOOK, sym_u, ob)

                # ========== 主循环：消费 diff depth ==========
                async for raw in ws:
                    now = time.time()

                    # 定期强制重建一次本地簿：直接跳出 inner 循环，走外层重连 + 新快照
                    if snapshot_interval > 0 and (now - last_snapshot_ts) >= snapshot_interval:
                        print(f"[WS depth] snapshot interval reached for {sym_u}, resync...")
                        break

                    try:
                        msg = json.loads(raw)
                    except Exception as je:
                        print(f"[WS depth] json err {k}:{sym_u}: {je}")
                        continue

                    # depthUpdate 标准字段
                    u = msg.get("u")
                    if u is None:
                        # 不是 depthUpdate（极少见），忽略
                        continue
                    if isinstance(u, str):
                        try:
                            u = int(u)
                        except ValueError:
                            pass

                    U = msg.get("U")
                    pu = msg.get("pu")

                    # 跳过快照之前的历史更新
                    if isinstance(last_u, int) and isinstance(u, int) and u <= last_u:
                        continue

                    # 若 futures 提供了 pu，则可以用来检测丢包
                    if pu is not None and isinstance(last_u, int):
                        try:
                            pu_int = int(pu)
                        except ValueError:
                            pu_int = None
                        else:
                            if pu_int != last_u:
                                print(f"[WS depth] gap detected for {sym_u} (pu={pu_int}, last_u={last_u}), resync")
                                break

                    # 应用增量
                    _apply_updates(bids_book, msg.get("b", []))
                    _apply_updates(asks_book, msg.get("a", []))
                    last_u = u
                    last_snapshot_ts = now

                    # 发布前 N 档
                    bids = _sorted_top(bids_book, levels, reverse=True)
                    asks = _sorted_top(asks_book, levels, reverse=False)
                    if bids and asks:
                        ob = OrderBook(symbol=sym_u, ts=now, bids=bids, asks=asks)
                        bus.publish(Topic.ORDERBOOK, sym_u, ob)

            # 正常退出 inner ws（例如我们主动 break 做 resync），走重连流程
        except Exception as e:
            print(f"[WS depth] {k}:{sym_u} disconnected: {e}. Reconnect in {reconnect_delay:.1f}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.7, max_delay)
