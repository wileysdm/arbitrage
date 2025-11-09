# -*- coding: utf-8 -*-
# arbitrage/data/bus.py
"""
轻量发布/订阅（本地 asyncio 队列）。
- publish(topic, key, value)
- latest(topic, key) -> 最近一次的 value（没有则 None）
- subscribe(topic, key=None) -> 返回 AsyncIterator（key=None 为“通配”，收到 (key, value)）
"""
from __future__ import annotations
import asyncio
from collections import defaultdict
from typing import Any, Dict, Tuple, AsyncIterator, Optional

class Topic:
    ORDERBOOK = "orderbook"
    MARK      = "mark"
    FUNDING   = "funding"
    META      = "meta"
    EXEC_FILLS = "exec_fills"  # 来自 user_stream 的成交/余额等（可选）

class Bus:
    def __init__(self, max_queue: int = 1000):
        self._latest: Dict[Tuple[str, str], Any] = {}
        self._sub_exact: Dict[Tuple[str, str], list[asyncio.Queue]] = defaultdict(list)
        self._sub_wild : Dict[str, list[asyncio.Queue]] = defaultdict(list)
        self._max_queue = max_queue
        self._lock = asyncio.Lock()

    async def _put(self, q: asyncio.Queue, item):
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            # 丢弃最旧，保留最新
            try:
                _ = q.get_nowait()
            except Exception:
                pass
            q.put_nowait(item)

    async def publish_async(self, topic: str, key: str, value: Any):
        async with self._lock:
            self._latest[(topic, key)] = value
            for q in self._sub_exact.get((topic, key), []):
                await self._put(q, value)
            # wildcard 订阅收到 (key, value)
            for q in self._sub_wild.get(topic, []):
                await self._put(q, (key, value))

    def publish(self, topic: str, key: str, value: Any):
        # 允许在非 async 环境中调用
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(self.publish_async(topic, key, value))
        else:
            loop.run_until_complete(self.publish_async(topic, key, value))

    def latest(self, topic: str, key: str) -> Optional[Any]:
        return self._latest.get((topic, key))

    async def subscribe(self, topic: str, key: Optional[str] = None) -> AsyncIterator[Any]:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._max_queue)
        if key is None:
            self._sub_wild[topic].append(q)
            while True:
                item = await q.get()
                yield item  # (key, value)
        else:
            self._sub_exact[(topic, key)].append(q)
            # 先把 latest 推一次（快启动）
            latest = self.latest(topic, key)
            if latest is not None:
                await self._put(q, latest)
            while True:
                item = await q.get()
                yield item
