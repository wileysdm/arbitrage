# -*- coding: utf-8 -*-
# arbitrage/data/service.py
"""
DataService：编排数据适配器（REST/WS）→ Bus → Cache → DataClient 只读接口
- register(kind, symbol): 注册要监听的标的
- start(): 启动 WS（订单簿 + 标记价），并启动 cache 泵
- DataClient：策略/legs 的只读门面（get_orderbook / get_mark / get_meta / subscribe_*）

注意：
- 若你尚未实现 funding/meta 的生产者，这两类返回可能为 None。
- Store 持久化可选（传入 Store(enable=True) 即可）。
"""
from __future__ import annotations
import asyncio
from typing import Dict, List, Optional, Tuple

from arbitrage.data.bus import Bus, Topic
from arbitrage.data.cache import Cache, pump_from_bus
from arbitrage.data.store import Store
from arbitrage.data.schemas import OrderBook, MarkPrice, FundingRate, Meta

# 适配器
from arbitrage.data.adapters.binance.ws_orderbook import run_orderbook_ws
from arbitrage.data.adapters.binance.ws_mark import run_mark_ws
from arbitrage.data.adapters.binance.rest import poll_loop_orderbook, poll_loop_mark, get_meta

class DataService:
    _global: "DataService" | None = None

    def __init__(self, use_ws: bool = True, store: Optional[Store] = None):
        self.bus   = Bus()
        self.cache = Cache()
        self.store = store or Store(enable=False)
        self.use_ws = use_ws
        self._regs: List[Tuple[str, str]] = []  # (kind, symbol)
        self._tasks: List[asyncio.Task] = []
        self._started = False

    # ---- 注册与启动 ----
    def register(self, kind: str, symbol: str):
        tup = (kind.lower(), symbol.upper())
        if tup not in self._regs:
            self._regs.append(tup)

    async def _start_symbol(self, kind: str, symbol: str):
        # 同步发布一次 meta
        try:
            mt = get_meta(kind, symbol)
            self.bus.publish(Topic.META, symbol, mt)
        except Exception as e:
            print(f"[DataService] meta fetch err {kind}:{symbol}: {e}")

        if self.use_ws:
            # WS 版（订单簿 + 标记价）
            self._tasks.append(asyncio.create_task(run_orderbook_ws(kind, symbol, self.bus, levels=10, speed_ms=100)))
            self._tasks.append(asyncio.create_task(run_mark_ws(kind, symbol, self.bus)))
        else:
            # REST 轮询兜底
            self._tasks.append(asyncio.create_task(poll_loop_orderbook(kind, symbol, self.bus, interval=0.5)))
            self._tasks.append(asyncio.create_task(poll_loop_mark(kind, symbol, self.bus, interval=1.0)))

    async def start(self):
        if self._started:
            return
        self._started = True
        # 启动 cache 泵（Bus -> Cache）
        self._tasks.append(asyncio.create_task(pump_from_bus(self.bus, self.cache)))

        # 启动每个注册标的
        for kind, symbol in self._regs:
            await self._start_symbol(kind, symbol)

        # 可选：持久化（监听 bus 并保存）
        if self.store and self.store.enable:
            self._tasks.append(asyncio.create_task(self._persist_loop()))

    async def _persist_loop(self):
        ob_sub = await self.bus.subscribe(Topic.ORDERBOOK, None)
        mk_sub = await self.bus.subscribe(Topic.MARK, None)
        fr_sub = await self.bus.subscribe(Topic.FUNDING, None)

        async def _loop_ob():
            async for sym, ob in ob_sub:
                self.store.save_orderbook(ob)

        async def _loop_mk():
            async for sym, mp in mk_sub:
                self.store.save_mark(mp)

        async def _loop_fr():
            async for sym, fr in fr_sub:
                self.store.save_funding(fr)

        await asyncio.gather(_loop_ob(), _loop_mk(), _loop_fr())

    @classmethod
    def global_service(cls) -> "DataService":
        if cls._global is None:
            cls._global = DataService()
        return cls._global

# ---- 对外只读门面 ----
class DataClient:
    """策略/legs 侧使用的只读接口"""
    def __init__(self, service: Optional[DataService] = None):
        self.svc = service or DataService.global_service()

    # 最新快照
    async def aget_orderbook(self, symbol: str) -> Optional[OrderBook]:
        return await self.svc.cache.get_orderbook(symbol.upper())

    async def aget_mark(self, symbol: str) -> Optional[MarkPrice]:
        return await self.svc.cache.get_mark(symbol.upper())

    async def aget_funding(self, symbol: str) -> Optional[FundingRate]:
        return await self.svc.cache.get_funding(symbol.upper())

    async def aget_meta(self, symbol: str) -> Optional[Meta]:
        return await self.svc.cache.get_meta(symbol.upper())

    # 同步友好包装（在同步环境下用 loop.run_until_complete）
    def get_orderbook(self, symbol: str) -> Optional[OrderBook]:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # 允许在协程内被调用（注意：阻塞当前任务直到返回）
            return loop.run_until_complete(self.aget_orderbook(symbol))
        else:
            return loop.run_until_complete(self.aget_orderbook(symbol))

    def get_mark(self, symbol: str) -> Optional[MarkPrice]:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return loop.run_until_complete(self.aget_mark(symbol))
        else:
            return loop.run_until_complete(self.aget_mark(symbol))

    def get_meta(self, symbol: str) -> Optional[Meta]:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return loop.run_until_complete(self.aget_meta(symbol))
        else:
            return loop.run_until_complete(self.aget_meta(symbol))

    # 订阅异步流
    async def subscribe_orderbook(self, symbol: str):
        return await self.svc.bus.subscribe(Topic.ORDERBOOK, symbol.upper())

    async def subscribe_mark(self, symbol: str):
        return await self.svc.bus.subscribe(Topic.MARK, symbol.upper())

# ---- 便捷启动 ----
async def boot_and_start(regs: list[tuple[str, str]], use_ws: bool = True, persist: bool = False):
    """
    快速启动数据服务：
        regs = [("usdtm","BTCUSDT"), ("coinm","BTCUSD_PERP"), ("spot","BTCUSDT")]
    """
    svc = DataService.global_service()
    svc.use_ws = use_ws
    svc.store.enable = persist
    for k, s in regs:
        svc.register(k, s)
    await svc.start()
    return svc
