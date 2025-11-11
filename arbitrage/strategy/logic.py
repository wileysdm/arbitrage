# -*- coding: utf-8 -*-
"""
统一腿架构的策略逻辑（quote/hedge 并列 & 合并同类项）
- 支持腿类型：spot / coinm / usdtm / usdcm
- 入口：
    * try_enter_unified()             —— 直接从配置取两腿，统一判定并执行
- 退出：
    * try_exit_unified(position)      —— 统一的判定与市价平仓
"""
from __future__ import annotations

import time, math
import logging
from typing import Tuple, List

from arbitrage.data.schemas import Position
from arbitrage.utils import vwap_to_qty, append_trade_row, next_trade_id

# ===== 配置常量 =====
from arbitrage.config import (
    ONLY_POSITIVE_CARRY, ENTER_BPS, EXIT_BPS, STOP_BPS, MAX_HOLD_SEC,
    V_USD, MAX_SLIPPAGE_BPS_SPOT, MAX_SLIPPAGE_BPS_COINM,
    EXECUTION_MODE, HEDGE_KIND, QUOTE_KIND, HEDGE_SYMBOL, QUOTE_SYMBOL
)
from arbitrage.config import (
    HYBRID_MAKER_LEG, HYBRID_WAIT_SEC, HYBRID_MIN_FILL_RATIO,
    PAIR_POLL_INTERVAL, DRY_RUN, EXECUTION_MODE,  
)
# 统一腿适配器
from arbitrage.exchanges.legs import make_leg

# 兼容frontier仅 coinm↔spot 使用
from arbitrage.strategy.frontier import (
    collect_frontier_candidates, print_per_level_book_edge, place_entry_from_row
)

# 老执行（仅用于 coinm↔spot 的兼容入口）
from arbitrage.exchanges.exec_binance_rest import (
    place_spot_limit_maker, place_coinm_limit,
    place_spot_market, place_coinm_market
)

OrderBookSide = List[Tuple[float, float]]

# ---------------------------------------------------------
# 公用工具
# ---------------------------------------------------------
def _slip_bps(levels: OrderBookSide, qty: float) -> float:
    """用 vwap 估算滑点（相对最优档），单位 bps。levels 为 (px, qty)。"""
    if qty <= 0 or not levels:
        return 1e9
    filled, vwap = vwap_to_qty(levels, qty)
    if not vwap or filled <= 0:
        return 1e9
    best = levels[0][0]
    return abs(vwap - best) / best * 10000.0

def _slip_threshold(kind: str) -> float:
    """不同腿类型对应不同默认阈值：合约用 MAX_SLIPPAGE_BPS_COINM，现货/UM 用 MAX_SLIPPAGE_BPS_SPOT。"""
    return MAX_SLIPPAGE_BPS_COINM if kind == "coinm" else MAX_SLIPPAGE_BPS_SPOT

def _spread_bps(quote_px: float, hedge_px: float) -> float:
    return (quote_px - hedge_px) / max(1e-12, hedge_px) * 10000.0

# ===== 新增：逐档候选收集（统一 legs）=====
def _collect_unified_candidates(
    quote, hedge,
    q_bids, q_asks, h_bids, h_asks,
    max_levels: int = 10,
    min_bp: float = None,
    min_vusd: float = None,
    only_positive_carry: bool = True,
):
    """
    统一逐档筛选候选：
      - 返回 (rows_pos, rows_neg)，元素:
        { side, px_quote, px_hedge, q_qty, h_qty, edge_bps, level }
      - q_qty/h_qty 已按名义与该档可成交量取 min
    """
    min_bp  = ENTER_BPS if min_bp  is None else float(min_bp)
    min_vus = V_USD     if min_vusd is None else float(min_vusd)

    # 名义目标（每条腿独立）
    q_target = quote.qty_from_usd(V_USD)
    h_target = hedge.qty_from_usd(V_USD)

    rows_pos, rows_neg = [], []

    L = min(max_levels, len(q_bids), len(q_asks), len(h_bids), len(h_asks))
    for i in range(L):
        # ---- 正向：买 hedge(ask[i]) / 卖 quote(bid[i]) ----
        px_h_pos = h_asks[i][0]
        px_q_pos = q_bids[i][0]
        edge_pos = _spread_bps(px_q_pos, px_h_pos)  # = (q - h)/h * 1e4

        # 可成交数量（逐档）
        h_cap_pos = float(h_asks[i][1])
        q_cap_pos = float(q_bids[i][1])
        # 最终数量：名义与该档余量的 min
        h_qty_pos = min(h_target, h_cap_pos)
        q_qty_pos = min(q_target, q_cap_pos)

        if edge_pos >= min_bp and (h_qty_pos * px_h_pos) >= min_vus and (q_qty_pos * px_q_pos) >= min_vus:
            rows_pos.append(dict(
                side="POS", level=i+1,
                px_quote=px_q_pos, px_hedge=px_h_pos,
                q_qty=q_qty_pos, h_qty=h_qty_pos,
                edge_bps=edge_pos
            ))

        # ---- 反向：卖 hedge(bid[i]) / 买 quote(ask[i]) ----
        if not only_positive_carry:
            px_h_neg = h_bids[i][0]
            px_q_neg = q_asks[i][0]
            edge_neg = _spread_bps(px_q_neg, px_h_neg)  # NEG 目标：edge <= -min_bp

            h_cap_neg = float(h_bids[i][1])
            q_cap_neg = float(q_asks[i][1])
            h_qty_neg = min(h_target, h_cap_neg)
            q_qty_neg = min(q_target, q_cap_neg)

            if edge_neg <= -min_bp and (h_qty_neg * px_h_neg) >= min_vus and (q_qty_neg * px_q_neg) >= min_vus:
                rows_neg.append(dict(
                    side="NEG", level=i+1,
                    px_quote=px_q_neg, px_hedge=px_h_neg,
                    q_qty=q_qty_neg, h_qty=h_qty_neg,
                    edge_bps=edge_neg
                ))

    return rows_pos, rows_neg
def _hybrid_maker_then_taker(
    side: str,                 # "POS" 或 "NEG"
    maker_leg, taker_leg,      # 传入已经构造好的两个 Leg 实例（quote/hedge 其一做 maker）
    maker_qty: float,          # maker 腿目标数量（base 或 张）
    taker_qty: float,          # taker 腿目标数量（base 或 张）
    maker_bids, maker_asks,    # maker 腿盘口（用于取 post-only 价：BUY→bid，SELL→ask）
    taker_bids, taker_asks,    # taker 腿盘口（这里只用于日志或你后续要做风控）
) -> bool:
    """
    先在 maker_leg 下 LIMIT_MAKER；在 HYBRID_WAIT_SEC 内若累计成交比例 ≥ HYBRID_MIN_FILL_RATIO，
    立即在另一腿（taker_leg）用 MARKET 对冲匹配规模。否则撤单放弃。
    """
    if DRY_RUN:
        print(f"[DRY][HYB] side={side} maker={getattr(maker_leg,'symbol','?')} taker={getattr(taker_leg,'symbol','?')} "
              f"maker_qty={maker_qty:.8f} taker_qty={taker_qty:.8f}")
        return True

    # —— 1) 决定 maker 腿的买卖方向（根据 side 和 maker_leg 属于哪条腿）——
    # 规则：
    #   POS：hedge BUY, quote SELL
    #   NEG：hedge SELL, quote BUY
    if maker_leg is taker_leg:
        raise ValueError("maker_leg 与 taker_leg 不能相同")

    # maker 是 hedge 还是 quote？
    maker_is_hedge = (maker_leg.symbol.upper() == getattr(maker_leg, "symbol", "").upper()) and \
                     (maker_leg is not None)  # 只是避免 IDE 报告；真正判断用调用处传参语义

    # 更明确的方向映射：
    # 如果 maker 是 hedge：POS→BUY，NEG→SELL
    # 如果 maker 是 quote：POS→SELL，NEG→BUY
    if maker_is_hedge:
        maker_side = "BUY" if side == "POS" else "SELL"
    else:
        maker_side = "SELL" if side == "POS" else "BUY"

    # 选 post-only 价：BUY→bid，SELL→ask
    if maker_side == "BUY":
        maker_px = maker_bids[0][0] if maker_bids else None
    else:
        maker_px = maker_asks[0][0] if maker_asks else None
    if maker_px is None:
        print("[HYB] maker 侧无可用价位")
        return False

    # —— 2) 下 maker 限价（LIMIT_MAKER / GTX）——
    try:
        o = maker_leg.place_limit_maker(maker_side, maker_qty, maker_px)
        maker_oid = (o or {}).get("orderId")
    except Exception as e:
        print(f"[HYB] maker 下单失败: {e}")
        return False

    # —— 3) 轮询等待部分成交 —— 
    filled = 0.0
    t0 = time.time()
    while time.time() - t0 < HYBRID_WAIT_SEC:
        try:
            st, cum = maker_leg.get_order_status(maker_oid, maker_leg.symbol)
            filled = max(filled, float(cum or 0.0))
        except Exception as e:
            print(f"[HYB] 查单失败: {e}")
        ratio = filled / max(1e-12, maker_qty)
        if ratio >= HYBRID_MIN_FILL_RATIO:
            break
        time.sleep(PAIR_POLL_INTERVAL)

    if filled <= 0.0:
        # 未成交 → 撤单放弃
        try:
            maker_leg.cancel(maker_oid)
        except Exception:
            pass
        print("[HYB] maker 未成交，撤单放弃")
        return False

    # —— 4) 按实际已成比例，对冲另一腿的市价规模 —— 
    scale = min(1.0, filled / max(1e-12, maker_qty))
    taker_filled_qty = taker_qty * scale

    # taker 方向与 side、腿别的映射：
    #   对 hedge 腿：POS→SELL，NEG→BUY
    #   对 quote 腿：POS→BUY， NEG→SELL
    taker_is_hedge = not maker_is_hedge
    if taker_is_hedge:
        taker_side = "SELL" if side == "POS" else "BUY"
    else:
        taker_side = "BUY" if side == "POS" else "SELL"

    try:
        taker_leg.place_market(taker_side, taker_filled_qty, reduce_only=taker_leg.is_perp())
    except Exception as e:
        print(f"[HYB] taker 对冲失败: {e}")
        return False

    return True


def try_enter_unified():
    """
    两腿（quote & hedge）统一处理（内含逐档候选/价值加权逻辑）：
      - ref_price：perp 用 mark，spot 用 mid
      - qty：通过各自 qty_from_usd(V_USD) 计算
      - 候选：按前N档（默认10）逐档计算 edge_bps 与可成交名义，筛出最优档
      - 执行：taker / maker / hybrid 由 EXECUTION_MODE 决定（默认 taker）
    返回: (ok, Position|None)
    """
    logging.info("try_enter_unified called")
    quote = make_leg(QUOTE_KIND, QUOTE_SYMBOL)
    hedge = make_leg(HEDGE_KIND, HEDGE_SYMBOL)

    # 取盘口与参考价
    q_bids, q_asks = quote.get_books(limit=10)
    h_bids, h_asks = hedge.get_books(limit=10)
    q_ref = quote.ref_price()
    h_ref = hedge.ref_price()
    sp_bps_ref = _spread_bps(q_ref, h_ref)

    # 先做参考价级别的“是否值得继续”快速拦截
    if ONLY_POSITIVE_CARRY and sp_bps_ref < 0:
        return (False, None)

    # 逐档搜集候选（代替原先 try_enter_from_frontier）
    rows_pos, rows_neg = _collect_unified_candidates(
        quote, hedge, q_bids, q_asks, h_bids, h_asks,
        max_levels=10, min_bp=ENTER_BPS, min_vusd=V_USD,
        only_positive_carry=ONLY_POSITIVE_CARRY
    )

    # 选最佳候选
    cand = None
    if rows_pos:
        cand = max(rows_pos, key=lambda r: r["edge_bps"])
    if (not ONLY_POSITIVE_CARRY) and rows_neg:
        neg_best = min(rows_neg, key=lambda r: r["edge_bps"])  # 更负更好
        # 如果没有正向或负向更优，则选负向
        if (cand is None) or (neg_best["edge_bps"] < cand["edge_bps"]):
            cand = neg_best

    if not cand:
        return (False, None)

    side      = cand["side"]
    px_quote  = float(cand["px_quote"])
    px_hedge  = float(cand["px_hedge"])
    q_qty     = float(cand["q_qty"])
    h_qty     = float(cand["h_qty"])
    edge_bps  = float(cand["edge_bps"])
    lvl       = cand["level"]

    # 再做一次滑点阈值估算（按方向选择买看 ask、卖看 bid）
    if side == "POS":
        h_sl = _slip_bps(h_asks, h_qty)   # 买 hedge → 看 ask
        q_sl = _slip_bps(q_bids, q_qty)   # 卖 quote → 看 bid
    else:
        h_sl = _slip_bps(h_bids, h_qty)   # 卖 hedge → 看 bid
        q_sl = _slip_bps(q_asks, q_qty)   # 买 quote → 看 ask
    if h_sl > _slip_threshold(HEDGE_KIND) or q_sl > _slip_threshold(QUOTE_KIND):
        print(f"❌ 入场滑点超限(level {lvl}) | hedge {h_sl:.2f}bp | quote {q_sl:.2f}bp")
        return (False, None)

    mode = (EXECUTION_MODE or "").lower()
    logging.info("Chosen level=%s side=%s edge=%.2fbp | q_px=%.2f h_px=%.2f | q_qty=%.6f h_qty=%.6f | mode=%s",
                 lvl, side, edge_bps, px_quote, px_hedge, q_qty, h_qty, mode)

    # === 下单（仅 hybrid / taker）===
    if mode == "hybrid":
        # 由 HYBRID_MAKER_LEG=quote|hedge 决定先 maker 的腿
        maker_first = (HYBRID_MAKER_LEG == "quote")
        maker_leg   = quote if maker_first else hedge
        taker_leg   = hedge if maker_first else quote
        maker_bids, maker_asks = (q_bids, q_asks) if maker_first else (h_bids, h_asks)
        taker_bids, taker_asks = (h_bids, h_asks) if maker_first else (q_bids, q_asks)

        ok = _hybrid_maker_then_taker(
            side,
            maker_leg, taker_leg,
            q_qty if maker_first else h_qty,
            h_qty if maker_first else q_qty,
            maker_bids, maker_asks, taker_bids, taker_asks
        )
        if not ok:
            return (False, None)
        o_h = o_q = {}  # hybrid 内已下单，这里占位记录

    else:  # taker
        if side == "POS":
            o_h = hedge.place_market("BUY",  h_qty)
            o_q = quote.place_market("SELL", q_qty)
        else:
            o_h = hedge.place_market("SELL", h_qty)
            o_q = quote.place_market("BUY",  q_qty)

    # === 记录与返回 Position ===
    tid = next_trade_id()
    append_trade_row({
        "event": "OPEN", "trade_id": tid, "side": side,
        "level": lvl, "edge_bps": f"{edge_bps:.2f}",
        "Q_btc": f"{(h_qty if HEDGE_KIND!='coinm' else 0.0):.8f}",
        "N_cntr": int(q_qty) if QUOTE_KIND == "coinm" else 0,
        "spot_orderId": str(o_h.get("orderId","")) if isinstance(o_h, dict) else "",
        "cm_orderId":   str(o_q.get("orderId","")) if isinstance(o_q, dict) else "",
    })

    pos = Position(
        side=side,
        Q=(h_qty if HEDGE_KIND!="coinm" else 0.0),
        N=(int(q_qty) if QUOTE_KIND=="coinm" else 0),
        spot_symbol=HEDGE_SYMBOL if HEDGE_KIND=="spot" else "",
        coinm_symbol=QUOTE_SYMBOL if QUOTE_KIND=="coinm" else "",
    )
    return (True, pos)

# ---------------------------------------------------------
# 统一退出/平仓
# ---------------------------------------------------------
def try_exit_unified(position: Position) -> Tuple[bool, str]:
    """
    统一判定 + 市价离场（内联平仓逻辑）：
      - POS：spread_bps <= EXIT_BPS 达标；>= STOP_BPS 止损；
      - NEG：spread_bps >= -EXIT_BPS 达标；<= -STOP_BPS 止损；
      - 或者超过 MAX_HOLD_SEC 强制离场。
    返回: (是否触发, 原因字符串)
    """
    quote = make_leg(QUOTE_KIND, QUOTE_SYMBOL)
    hedge = make_leg(HEDGE_KIND, HEDGE_SYMBOL)
    q_ref, h_ref = quote.ref_price(), hedge.ref_price()
    sp_bps = _spread_bps(q_ref, h_ref)

    held_sec = time.time() - float(getattr(position, "t0", time.time()))
    reason = ""
    if position.side == "POS":
        if sp_bps <= EXIT_BPS: reason = f"spread {sp_bps:.2f}bp <= EXIT {EXIT_BPS}"
        elif sp_bps >= STOP_BPS: reason = f"STOP {sp_bps:.2f}bp >= {STOP_BPS}"
    else:
        if sp_bps >= -EXIT_BPS: reason = f"spread {sp_bps:.2f}bp >= -EXIT {-EXIT_BPS}"
        elif sp_bps <= -STOP_BPS: reason = f"STOP {sp_bps:.2f}bp <= {-STOP_BPS}"
    if not reason and held_sec >= MAX_HOLD_SEC:
        reason = f"HOLD {held_sec:.1f}s >= {MAX_HOLD_SEC}s"
    if not reason:
        return (False, "")

    # ===== 触发 → 直接在此处执行平仓（原 try_close 内联） =====
    # 计算两腿需要对冲的数量：
    # - 对于 coinm：优先用 position.N（张数）；其他腿（spot/UM）用 position.Q（base数量）；
    # - 若 position 里没记录可用数量（如 UM 腿），按名义 V_USD 估算数量；
    if HEDGE_KIND == "coinm":
        h_qty = float(position.N or 0)
    else:
        h_qty = float(position.Q or 0.0)
        if h_qty <= 0:
            h_qty = hedge.qty_from_usd(V_USD)

    if QUOTE_KIND == "coinm":
        q_qty = float(position.N or 0)
    else:
        q_qty = quote.qty_from_usd(V_USD)

    # 按仓位方向发送对冲单（perp 使用 reduceOnly 防止反向开仓）
    if position.side == "POS":
        hedge.place_market("SELL", h_qty, reduce_only=hedge.is_perp())
        quote.place_market("BUY",  q_qty, reduce_only=quote.is_perp())
    else:
        hedge.place_market("BUY",  h_qty, reduce_only=hedge.is_perp())
        quote.place_market("SELL", q_qty, reduce_only=quote.is_perp())

    append_trade_row({
        "event": "CLOSE",
        "trade_id": getattr(position, "trade_id", ""),
        "side": position.side,
        "Q_btc": f"{float(position.Q or 0.0):.8f}",
        "N_cntr": int(position.N or 0),
    })
    return (True, reason)

