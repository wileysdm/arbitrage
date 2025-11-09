# -*- coding: utf-8 -*-
# arbitrage/data/cache.py
"""
内存快照层：保存最新的 BBO/OrderBook、Mark、Funding、Meta。
提供线程安全（asyncio 环境）的 get/set。
另提供一个 “泵” 协程：监听 Bus，将消息写入 Cache。
"""
from __future__ import annotations
import asyncio
from typing import Dict, Optional, Tuple

from arbitrage.data.schemas import OrderBook, MarkPrice, FundingRate, Meta
from arbitrage.data.bus import Bus, Topic

class Cache:
    def __init__(self):
        self._ob: Dict[str, OrderBook] = {}
        self._mk: Dict[str, MarkPrice] = {}
        self._fr: Dict[str, FundingRate] = {}
        self._mt: Dict[str, Meta] = {}
        self._lock = asyncio.Lock()

    async def set_orderbook(self, ob: OrderBook):
        async with self._lock:
            self._ob[ob.symbol] = ob

    async def set_mark(self, mp: MarkPrice):
        async with self._lock:
            self._mk[mp.symbol] = mp

    async def set_funding(self, fr: FundingRate):
        async with self._lock:
            self._fr[fr.symbol] = fr

    async def set_meta(self, mt: Meta):
        async with self._lock:
            self._mt[mt.symbol] = mt

    async def get_orderbook(self, symbol: str) -> Optional[OrderBook]:
        async with self._lock:
            return self._ob.get(symbol)

    async def get_mark(self, symbol: str) -> Optional[MarkPrice]:
        async with self._lock:
            return self._mk.get(symbol)

    async def get_funding(self, symbol: str) -> Optional[FundingRate]:
        async with self._lock:
            return self._fr.get(symbol)

    async def get_meta(self, symbol: str) -> Optional[Meta]:
        async with self._lock:
            return self._mt.get(symbol)

async def pump_from_bus(bus: Bus, cache: Cache):
    """
    统一监听 bus 上的几个 Topic，写入 cache。
    - 使用通配订阅（key=None），收到 (key, value)
    """
    ob_sub = await bus.subscribe(Topic.ORDERBOOK, None)
    mk_sub = await bus.subscribe(Topic.MARK, None)
    fr_sub = await bus.subscribe(Topic.FUNDING, None)
    mt_sub = await bus.subscribe(Topic.META, None)

    async def _loop_ob():
        async for key, ob in ob_sub:
            await cache.set_orderbook(ob)

    async def _loop_mk():
        async for key, mp in mk_sub:
            await cache.set_mark(mp)

    async def _loop_fr():
        async for key, fr in fr_sub:
            await cache.set_funding(fr)

    async def _loop_mt():
        async for key, mt in mt_sub:
            await cache.set_meta(mt)

    await asyncio.gather(_loop_ob(), _loop_mk(), _loop_fr(), _loop_mt())
