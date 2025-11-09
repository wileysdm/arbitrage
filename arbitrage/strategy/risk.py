# -*- coding: utf-8 -*-
"""
极简风险模块（可逐步增强）：
- check_slippage_ok(bps, threshold)
- allow_open(): 全局是否允许开仓（占用、风控阈值等入口）
- allow_close(): 总是允许
后续可接入：
- 从 EXEC_FILLS/账户WS 获取保证金、仓位、ADL 队列、维护保证金率等 → 决策
"""
from __future__ import annotations

from arbitrage.config import MAX_SLIPPAGE_BPS_SPOT, MAX_SLIPPAGE_BPS_COINM

def check_slippage_ok(bps: float, is_perp: bool) -> bool:
    th = MAX_SLIPPAGE_BPS_COINM if is_perp else MAX_SLIPPAGE_BPS_SPOT
    return bps <= th

def allow_open() -> bool:
    # 这里可接账户利用率/风控阈值
    return True

def allow_close() -> bool:
    return True
