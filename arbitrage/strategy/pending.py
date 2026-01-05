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
    def __init__(
        self,
        order_id: Any,
        maker_leg_symbol: str,
        taker_leg_symbol: str,
        maker_market: Optional[str] = None,
        taker_market: Optional[str] = None,
    ):
        self.order_id = order_id
        self.maker_leg_symbol = maker_leg_symbol
        self.taker_leg_symbol = taker_leg_symbol
        self.maker_market = maker_market
        self.taker_market = taker_market
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
        # key: (symbol, orderId)；兼容旧行为会在缺 symbol 时退回 orderId
        self._orders: Dict[Any, PendingHybridOrder] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _make_key(order_id: Any, symbol: Optional[str], market: Optional[str] = None) -> Any:
        mk = (market or "").lower().strip()
        sym = (symbol or "").upper().strip()
        if mk and sym:
            return (mk, sym, order_id)
        if sym:
            return (sym, order_id)
        return order_id

    async def register(
        self,
        order_id: Any,
        maker_leg_symbol: str,
        taker_leg_symbol: str,
        timeout: float,
        maker_market: Optional[str] = None,
        taker_market: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """注册一个新订单并等待其成交"""
        key = self._make_key(order_id, maker_leg_symbol, maker_market)
        if key in self._orders:
            return None  # 避免重复注册

        order = PendingHybridOrder(
            order_id,
            maker_leg_symbol,
            taker_leg_symbol,
            maker_market=maker_market,
            taker_market=taker_market,
        )
        async with self._lock:
            self._orders[key] = order

        fill_data = await order.wait_for_fill(timeout)

        # 等待结束后，无论是否成交，都清理掉
        await self.remove(order_id)
        return fill_data

    async def on_fill(self, fill_data: Dict[str, Any], market: Optional[str] = None):
        """从 user_stream 收到成交事件时的回调"""
        order_id = fill_data.get("orderId")
        if not order_id:
            return

        market = market or fill_data.get("market")
        symbol = fill_data.get("symbol")
        key = self._make_key(order_id, symbol, market)
        key_sym_only = self._make_key(order_id, symbol)

        async with self._lock:
            order = self._orders.get(key) or self._orders.get(key_sym_only) or self._orders.get(order_id)
            if order:
                # 检查成交状态，只有当订单部分成交或完全成交时才触发
                status = fill_data.get("status")
                if status in ("PARTIALLY_FILLED", "FILLED"):
                    order.set_filled(fill_data)

    async def remove(self, order_id: Any):
        """移除一个订单"""
        async with self._lock:
            # 兼容旧：按 order_id 移除所有匹配项
            keys_to_del = [k for k, v in self._orders.items() if v.order_id == order_id]
            for k in keys_to_del:
                del self._orders[k]

# 创建一个全局单例
GlobalPendingManager = PendingManager()
