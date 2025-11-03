# -*- coding: utf-8 -*-
import time, math
from arbitrage.config import (
    PAIR_TIMEOUT_SEC, PAIR_POLL_INTERVAL, DRY_RUN,
    HYBRID_WAIT_SEC, HYBRID_MIN_FILL_RATIO
)
from arbitrage.exchanges.exec_binance_rest import (
    get_spot_order_status, get_coinm_order_status,
    place_spot_market, place_coinm_market,
    place_spot_limit_maker, place_coinm_limit,
    cancel_spot_order, cancel_coinm_order
)

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


# ====== 新增：Hybrid（先Maker一腿 → 成交后另一腿Taker） ======

def _round_down(x, step):
    if not step: return x
    return math.floor((float(x) + 1e-12) / float(step)) * float(step)

def hybrid_maker_then_taker(side: str, maker_leg: str,
                            Q_target: float, N_target: int,
                            spot_bids, spot_asks, cm_bids, cm_asks,
                            spot_step: float, cm_step: float, contract_size: float,
                            spot_symbol: str, coinm_symbol: str):
    """
    side: "POS"=买现货/卖合约；"NEG"=卖现货/买合约
    maker_leg: "spot" | "coinm"
    返回: (ok, Q_filled, N_filled, spot_order_id, cm_order_id)
    过程：
      1) 先在 maker_leg 挂 LIMIT_MAKER；
      2) 等待 HYBRID_WAIT_SEC；若部分成交≥比例阈值，立即在另一腿用 MARKET 对冲匹配的规模；
         若 0 成交则撤单放弃。
    """
    if DRY_RUN:
        print(f"[DRY][HYB] side={side} maker_leg={maker_leg} Q={Q_target:.6f} N={N_target}")
        return True, Q_target, int(N_target), None, None

    soid = coid = None
    t0 = time.time()

    # 1) 先下 Maker（post-only）
    if maker_leg == "spot":
        if side == "POS":
            price = spot_bids[0][0] if spot_bids else None
            o = place_spot_limit_maker("BUY",  Q_target, price, symbol=spot_symbol); soid = (o or {}).get("orderId")
        else:
            price = spot_asks[0][0] if spot_asks else None
            o = place_spot_limit_maker("SELL", Q_target, price, symbol=spot_symbol); soid = (o or {}).get("orderId")
    else:  # maker_leg == "coinm"
        if side == "POS":
            price = cm_asks[0][0] if cm_asks else None
            o = place_coinm_limit("SELL", N_target, price, post_only=True, symbol=coinm_symbol); coid = (o or {}).get("orderId")
        else:
            price = cm_bids[0][0] if cm_bids else None
            o = place_coinm_limit("BUY",  N_target, price, post_only=True, symbol=coinm_symbol); coid = (o or {}).get("orderId")

    # 2) 轮询等待 Maker 成交或部分成交
    Q_done = 0.0; N_done = 0
    while time.time() - t0 < HYBRID_WAIT_SEC:
        if maker_leg == "spot":
            _, q_exec = get_spot_order_status(soid, symbol=spot_symbol)
            Q_done = max(Q_done, float(q_exec or 0.0))
            cond_ok = (Q_done >= max(1e-12, HYBRID_MIN_FILL_RATIO * Q_target))
        else:
            _, n_exec = get_coinm_order_status(coid, symbol=coinm_symbol)
            N_done = max(N_done, int(n_exec or 0))
            cond_ok = (N_done >= max(1, int(math.ceil(HYBRID_MIN_FILL_RATIO * N_target))))
        if cond_ok:
            break
        time.sleep(PAIR_POLL_INTERVAL)

    # 3) 超时处理：完全没成交则撤单并放弃；部分成交则立刻对冲另一腿
    if maker_leg == "spot":
        if Q_done <= 0.0:
            try: cancel_spot_order(soid, symbol=spot_symbol)
            except Exception: pass
            print("[HYB] Maker(spot)未成交，撤单放弃。")
            return (False, 0.0, 0, soid, coid)

        ratio = min(1.0, Q_done / max(Q_target, 1e-12))
        n_need = int(max(1, _round_down(N_target * ratio, cm_step)))
        if side == "POS":
            o2 = place_coinm_market("SELL", n_need, reduce_only=False, symbol=coinm_symbol); coid = (o2 or {}).get("orderId")
        else:
            o2 = place_coinm_market("BUY",  n_need, reduce_only=False, symbol=coinm_symbol); coid = (o2 or {}).get("orderId")
        return (True, Q_done, n_need, soid, coid)

    else:  # maker_leg == "coinm"
        if N_done <= 0:
            try: cancel_coinm_order(coid, symbol=coinm_symbol)
            except Exception: pass
            print("[HYB] Maker(coinm)未成交，撤单放弃。")
            return (False, 0.0, 0, soid, coid)

        ratio = min(1.0, N_done / max(N_target, 1e-12))
        Q_need = max(_round_down(Q_target * ratio, spot_step), spot_step)
        if side == "POS":
            o2 = place_spot_market("BUY",  Q_need, symbol=spot_symbol); soid = (o2 or {}).get("orderId")
        else:
            o2 = place_spot_market("SELL", Q_need, symbol=spot_symbol); soid = (o2 or {}).get("orderId")
        return (True, Q_need, N_done, soid, coid)
