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

import time
from arbitrage.config import (
    PAIR_TIMEOUT_SEC, PAIR_POLL_INTERVAL, DRY_RUN,
    MAX_SLIPPAGE_BPS_SPOT, MAX_SLIPPAGE_BPS_COINM
)
from arbitrage.exchanges.exec_binance_rest import (
    get_spot_order_status, get_coinm_order_status,
    place_spot_market, place_coinm_market,
)


def check_slippage_ok(bps: float, is_perp: bool) -> bool:
    th = MAX_SLIPPAGE_BPS_COINM if is_perp else MAX_SLIPPAGE_BPS_SPOT
    return bps <= th

def allow_open() -> bool:
    # 这里可接账户利用率/风控阈值
    return True

def allow_close() -> bool:
    return True


def monitor_and_rescue_single_leg(side: str, spot_order: dict, cm_order: dict,
                                  expect_Q: float, expect_N: int,
                                  spot_symbol: str, coinm_symbol: str,
                                  timeout_sec: float = PAIR_TIMEOUT_SEC,
                                  poll_interval: float = PAIR_POLL_INTERVAL):
    """
    经典 maker-maker 或 taker-taker 的成对下单护航逻辑：
      - 若两腿都有成交 → 返回
      - 超时且仅一腿成交 → 在该腿上对冲（reduceOnly 或反向）以消灭单腿风险
    """
    if DRY_RUN:
        print("[DRY] 单腿监控跳过")
        return

    soid = (spot_order or {}).get("orderId")
    coid = (cm_order or {}).get("orderId")
    if not soid or not coid:
        print("⚠ 订单ID缺失，跳过监控");  return

    deadline = time.time() + timeout_sec
    spot_filled = cm_filled = 0.0

    while time.time() < deadline:
        _, s_exec = get_spot_order_status(soid, symbol=spot_symbol)
        _, c_exec = get_coinm_order_status(coid, symbol=coinm_symbol)
        spot_filled = max(spot_filled, float(s_exec or 0.0))
        cm_filled   = max(cm_filled,   float(c_exec or 0.0))
        if (spot_filled > 0.0) and (cm_filled > 0.0):
            return
        time.sleep(poll_interval)

    # —— 超时：单腿处理 ——
    if (spot_filled > 0.0) and (cm_filled == 0.0):
        qty = spot_filled if spot_filled > 0 else expect_Q
        print(f"‼ 单腿：仅 {spot_symbol} 成交 {qty:.6f} → 市价对冲")
        place_spot_market("SELL" if side=="POS" else "BUY", qty, symbol=spot_symbol)
    elif (cm_filled > 0.0) and (spot_filled == 0.0):
        n = int(cm_filled if cm_filled > 0 else expect_N)
        print(f"‼ 单腿：仅 {coinm_symbol} 成交 {n} 张 → reduceOnly 市价对冲")
        if side == "POS":
            place_coinm_market("BUY",  n, reduce_only=True, symbol=coinm_symbol)
        else:
            place_coinm_market("SELL", n, reduce_only=True, symbol=coinm_symbol)

