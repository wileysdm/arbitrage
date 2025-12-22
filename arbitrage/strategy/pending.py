# -*- coding: utf-8 -*-
# arbitrage/strategy/pending.py
"""
全局挂单管理器
- 用于 Hybrid 模式下，注册 maker 订单，并等待来自 user_stream 的成交信号
- 使用 asyncio.Event 实现异步等待/通知
"""
from __future__ import annotations
import asyncio
from typing import Dict, Any, Optional

class PendingHybridOrder:
    """代表一个待处理的 hybrid 模式下的 maker 订单"""
    def __init__(self, order_id: Any, maker_leg_symbol: str, taker_leg_symbol: str):
        self.order_id = order_id
        self.maker_leg_symbol = maker_leg_symbol
        self.taker_leg_symbol = taker_leg_symbol
        self.fill_event = asyncio.Event()
        self.fill_data: Optional[Dict[str, Any]] = None

    def set_filled(self, fill_data: Dict[str, Any]):
        """当订单被填充时，设置填充数据并触发事件"""
        self.fill_data = fill_data
        self.fill_event.set()

    async def wait_for_fill(self, timeout: float) -> Optional[Dict[str, Any]]:
        """等待订单成交信号，带超时"""
        try:
            await asyncio.wait_for(self.fill_event.wait(), timeout=timeout)
            return self.fill_data
        except asyncio.TimeoutError:
            return None

class PendingManager:
    """管理所有待处理的 maker 订单"""
    def __init__(self):
        self._orders: Dict[Any, PendingHybridOrder] = {}
        self._lock = asyncio.Lock()

    async def register(self, order_id: Any, maker_leg_symbol: str, taker_leg_symbol: str, timeout: float) -> Optional[Dict[str, Any]]:
        """注册一个新订单并等待其成交"""
        if order_id in self._orders:
            return None  # 避免重复注册

        order = PendingHybridOrder(order_id, maker_leg_symbol, taker_leg_symbol)
        async with self._lock:
            self._orders[order_id] = order

        fill_data = await order.wait_for_fill(timeout)

        # 等待结束后，无论是否成交，都清理掉
        await self.remove(order_id)
        return fill_data

    async def on_fill(self, fill_data: Dict[str, Any]):
        """从 user_stream 收到成交事件时的回调"""
        order_id = fill_data.get("orderId")
        if not order_id:
            return

        async with self._lock:
            order = self._orders.get(order_id)
            if order:
                # 检查成交状态，只有当订单部分成交或完全成交时才触发
                status = fill_data.get("status")
                if status in ("PARTIALLY_FILLED", "FILLED"):
                    order.set_filled(fill_data)

    async def remove(self, order_id: Any):
        """移除一个订单"""
        async with self._lock:
            if order_id in self._orders:
                del self._orders[order_id]

# 创建一个全局单例
GlobalPendingManager = PendingManager()
