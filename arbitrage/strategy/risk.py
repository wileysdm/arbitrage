"""风险模块（统一账户 / PAPI 版本）。

该项目的核心风控逻辑目前主要在策略层实现；这里保留最小接口，避免依赖旧的 spot 执行封装。
"""

from __future__ import annotations

from arbitrage.config import MAX_SLIPPAGE_BPS_COINM, MAX_SLIPPAGE_BPS_SPOT


def check_slippage_ok(bps: float, is_perp: bool) -> bool:
    th = MAX_SLIPPAGE_BPS_COINM if is_perp else MAX_SLIPPAGE_BPS_SPOT
    return bps <= th


def allow_open() -> bool:
    return True


def allow_close() -> bool:
    return True

