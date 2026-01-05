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
        # key 统一使用 bus key："{kind}:{SYMBOL}"（例如 "spot:BTCUSDT"）
        # 为兼容旧调用，也允许纯 "SYMBOL" 作为 key。
        self._ob: Dict[str, OrderBook] = {}
        self._mk: Dict[str, MarkPrice] = {}
        self._fr: Dict[str, FundingRate] = {}
        self._mt: Dict[str, Meta] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _normalize_key(key: str) -> str:
        s = (key or "").strip()
        if ":" in s:
            k, sym = s.split(":", 1)
            return f"{k.lower()}:{sym.upper()}"
        return s.upper()

    async def set_orderbook(self, ob: OrderBook, key: str | None = None):
        async with self._lock:
            k = self._normalize_key(key or ob.symbol)
            self._ob[k] = ob

    async def set_mark(self, mp: MarkPrice, key: str | None = None):
        async with self._lock:
            k = self._normalize_key(key or mp.symbol)
            self._mk[k] = mp

    async def set_funding(self, fr: FundingRate, key: str | None = None):
        async with self._lock:
            k = self._normalize_key(key or fr.symbol)
            self._fr[k] = fr

    async def set_meta(self, mt: Meta, key: str | None = None):
        async with self._lock:
            k = self._normalize_key(key or mt.symbol)
            self._mt[k] = mt

    async def get_orderbook(self, symbol: str) -> Optional[OrderBook]:
        async with self._lock:
            k = self._normalize_key(symbol)
            return self._ob.get(k)

    async def get_mark(self, symbol: str) -> Optional[MarkPrice]:
        async with self._lock:
            k = self._normalize_key(symbol)
            return self._mk.get(k)

    async def get_funding(self, symbol: str) -> Optional[FundingRate]:
        async with self._lock:
            k = self._normalize_key(symbol)
            return self._fr.get(k)

    async def get_meta(self, symbol: str) -> Optional[Meta]:
        async with self._lock:
            k = self._normalize_key(symbol)
            return self._mt.get(k)

async def pump_from_bus(bus: Bus, cache: Cache):
    """
    统一监听 bus 上的几个 Topic，写入 cache。
    - 使用通配订阅（key=None），收到 (key, value)
    """
    ob_sub = bus.subscribe(Topic.ORDERBOOK, None)
    mk_sub = bus.subscribe(Topic.MARK, None)
    fr_sub = bus.subscribe(Topic.FUNDING, None)
    mt_sub = bus.subscribe(Topic.META, None)

    async def _loop_ob():
        async for key, ob in ob_sub:
            await cache.set_orderbook(ob, key=key)

    async def _loop_mk():
        async for key, mp in mk_sub:
            await cache.set_mark(mp, key=key)

    async def _loop_fr():
        async for key, fr in fr_sub:
            await cache.set_funding(fr, key=key)

    async def _loop_mt():
        async for key, mt in mt_sub:
            await cache.set_meta(mt, key=key)

    await asyncio.gather(_loop_ob(), _loop_mk(), _loop_fr(), _loop_mt())
