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

import asyncio
import time, math
import logging
from typing import Any, Dict, Tuple, List, Optional
from arbitrage.data.adapters.binance import rest
from arbitrage.data.schemas import Position
from arbitrage.utils import vwap_to_qty, append_trade_row, next_trade_id

# ===== 配置常量 =====
from arbitrage.config import (
    ONLY_POSITIVE_CARRY, ENTER_BPS, EXIT_BPS, STOP_BPS, MAX_HOLD_SEC,
    V_USD, MAX_SLIPPAGE_BPS_SPOT, MAX_SLIPPAGE_BPS_COINM,
    EXECUTION_MODE, HEDGE_KIND, QUOTE_KIND, HEDGE_SYMBOL, QUOTE_SYMBOL,EXECUTION_MODE,
    HYBRID_MAKER_LEG, HYBRID_WAIT_SEC, HYBRID_MIN_FILL_RATIO,
    PAIR_POLL_INTERVAL, DRY_RUN
)

# 统一腿适配器
from arbitrage.exchanges.legs import make_leg

# 导入新的挂单管理器
from arbitrage.strategy.pending import GlobalPendingManager

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
    return (quote_px - hedge_px) / hedge_px * 10000.0

# ===== 新增：逐档候选收集（统一 legs）=====
def _collect_unified_candidates(
    quote, hedge,
    q_bids, q_asks, h_bids, h_asks,
    max_levels: int = 10,
    min_bp: Optional[float] = None,
    min_vusd: Optional[float] = None,
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

def _get_candidate(
    q_bids: List[Tuple[float, float]], q_asks: List[Tuple[float, float]],
    h_bids: List[Tuple[float, float]], h_asks: List[Tuple[float, float]],
    min_bp: float,
    only_positive_carry: bool = True
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    【简化版】只基于 Level 1 价格，筛选潜在的套利候选机会。

    :param q_bids/q_asks/h_bids/h_asks: 订单簿深度数据（只需要第一个元素）
    :param min_bp: 最小入场价差（基点）
    :param min_vusd: 最小名义成交额（USD）
    :param only_positive_carry: 是否只考虑正向套利
    :return: (rows_pos, rows_neg)，只包含 Level 1 且满足条件的候选
    """
    
    # 检查是否有 Level 1 数据
    if not (q_bids and q_asks and h_bids and h_asks):
        # 订单簿不完整，无法进行 Level 1 匹配
        return [], []
    rows_pos, rows_neg = [], []

    # --- 1. 正向：买 hedge(ask[0]) / 卖 quote(ask[0]) ---
    # Level 1 价格
    px_h_pos = h_asks[0][0]
    px_q_pos = q_asks[0][0]
    aa_spread = _spread_bps(px_q_pos, px_h_pos) 

    # Level 1 数量
    h_cap_pos = float(h_asks[0][1])
    q_cap_pos = float(q_asks[0][1])
    
    if aa_spread >= min_bp:
        rows_pos.append(dict(
            side="POS", level=1, # Level=1
            px_quote=px_q_pos, px_hedge=px_h_pos,
            q_qty=q_cap_pos, h_qty=h_cap_pos,
            edge_bps=aa_spread
        ))

    # --- 2. 反向：卖 hedge(bid[0]) / 买 quote(ask[0]) ---
    if not only_positive_carry:
        px_h_neg = h_bids[0][0]
        px_q_neg = q_bids[0][0]
        bb_spread = _spread_bps(px_q_neg, px_h_neg)  # NEG 目标：edge <= -min_bp

        h_cap_neg = float(h_bids[0][1])
        q_cap_neg = float(q_bids[0][1])
        
        if bb_spread <= -min_bp:
            rows_neg.append(dict(
                side="NEG", level=1, # Level=1
                px_quote=px_q_neg, px_hedge=px_h_neg,
                q_qty=q_cap_neg, h_qty=h_cap_neg,
                edge_bps=bb_spread
            ))

    return rows_pos, rows_neg

async def _hybrid_maker_then_taker(
    side: str,                 # "POS" 或 "NEG"
    maker_leg, taker_leg,      # 传入已经构造好的两个 Leg 实例（quote/hedge 其一做 maker）
    maker_qty: float,          # maker 腿目标数量（base 或 张）
    taker_qty: float,          # taker 腿目标数量（base 或 张）
    maker_bids, maker_asks,    # maker 腿盘口（用于取 post-only 价：BUY→bid，SELL→ask）
    taker_bids, taker_asks,    # taker 腿盘口（这里只用于日志或你后续要做风控）
) -> bool:
    """
    事件驱动版本：先在 maker_leg 下 LIMIT_MAKER，然后等待来自 User Stream 的成交事件。
    一旦收到成交回报，立即在另一腿（taker_leg）用 MARKET 对冲。
    """
    if DRY_RUN:
        print(f"[DRY][HYB] side={side} maker={getattr(maker_leg,'symbol','?')} taker={getattr(taker_leg,'symbol','?')} "
              f"maker_qty={maker_qty:.8f} taker_qty={taker_qty:.8f}")
        return True

    # —— 1) 决定 maker 腿的买卖方向和价格 ——
    if maker_leg is taker_leg:
        raise ValueError("maker_leg 与 taker_leg 不能相同")

    maker_is_hedge = (
        str(getattr(maker_leg, "kind", "")).lower() == str(HEDGE_KIND).lower()
        and str(getattr(maker_leg, "symbol", "")).upper() == str(HEDGE_SYMBOL).upper()
    )

    def _uds_market_for_kind(kind: str) -> str:
        k = (kind or "").lower()
        if k == "spot":
            return "spot"
        if k == "coinm":
            return "cm"
        if k in ("usdtm", "usdcm"):
            return "um"
        # 兜底：unknown kind 仍尝试按 futures 处理
        return "um"

    if maker_is_hedge:
        maker_side = "BUY" if side == "POS" else "SELL"
    else: # maker is quote
        maker_side = "SELL" if side == "POS" else "BUY"

    if maker_side == "BUY":
        maker_px = maker_bids[0][0] if maker_bids else None
    else:
        maker_px = maker_asks[0][0] if maker_asks else None
    
    if maker_px is None:
        logging.warning("[HYB] maker 侧无可用价位")
        return False

    # —— 2) 下 maker 限价单（LIMIT_MAKER / GTX）——
    try:
        o = maker_leg.place_limit_maker(maker_side, maker_qty, maker_px)
        maker_oid = (o or {}).get("orderId")
        if not maker_oid:
            raise ValueError("下单后未能获取 orderId")
    except Exception as e:
        logging.error(f"[HYB] maker 下单失败: {e}")
        return False

    logging.info(f"[HYB] Maker order {maker_oid} placed. Waiting for fill...")

    # —— 3) 等待成交事件 ——
    fill_data = await GlobalPendingManager.register(
        maker_oid,
        maker_leg.symbol,
        taker_leg.symbol,
        maker_market=_uds_market_for_kind(getattr(maker_leg, "kind", "")),
        taker_market=_uds_market_for_kind(getattr(taker_leg, "kind", "")),
        timeout=HYBRID_WAIT_SEC
    )

    if not fill_data:
        # 超时未收到成交事件：仍可能已部分成交（取消/事件丢失/延迟）。
        filled_qty_timeout = 0.0
        status_timeout = ""
        try:
            status_timeout, filled_qty_timeout = maker_leg.get_order_status(maker_oid)
            filled_qty_timeout = float(filled_qty_timeout or 0.0)
        except Exception:
            filled_qty_timeout = 0.0

        logging.warning(
            "[HYB] Maker order %s 未在 %.2fs 内收到成交事件；status=%s executed=%.8f。尝试撤单",
            maker_oid,
            HYBRID_WAIT_SEC,
            status_timeout,
            filled_qty_timeout,
        )
        try:
            maker_leg.cancel(maker_oid)
        except Exception as e:
            logging.error(f"[HYB] 撤销 maker order {maker_oid} 失败: {e}")

        # 如果确实有成交（哪怕没收到事件），也要做一次补救对冲。
        if filled_qty_timeout > 0:
            scale = filled_qty_timeout / max(1e-12, maker_qty)
            taker_filled_qty = taker_qty * scale

            taker_is_hedge = not maker_is_hedge
            if taker_is_hedge:
                taker_side = "SELL" if side == "POS" else "BUY"
            else:
                taker_side = "BUY" if side == "POS" else "SELL"

            logging.info(
                "[HYB] Timeout recovery hedging: taker %s %.8f of %s",
                taker_side,
                taker_filled_qty,
                getattr(taker_leg, "symbol", "?"),
            )
            try:
                # 入场对冲不能用 reduceOnly，否则可能直接被拒单
                taker_leg.place_market(taker_side, taker_filled_qty, reduce_only=False)
                return True
            except Exception as e:
                logging.error(f"[HYB] timeout recovery taker 对冲失败: {e}")
        return False

    # —— 4) 收到成交事件，计算并执行 Taker 对冲 ——
    # 收到部分成交事件后立刻撤掉剩余挂单，避免继续成交导致裸露
    try:
        maker_leg.cancel(maker_oid)
    except Exception:
        pass

    last_qty = float(fill_data.get("lastQty", 0.0) or 0.0)
    cum_qty = float(fill_data.get("cumQty", 0.0) or 0.0)
    filled_qty = max(last_qty, cum_qty)
    # 再用 REST 查一次 executedQty 做兜底（事件可能丢/延迟）
    try:
        _st, executed = maker_leg.get_order_status(maker_oid)
        filled_qty = max(filled_qty, float(executed or 0.0))
    except Exception:
        pass

    if filled_qty <= 0:
        logging.warning(f"[HYB] 成交数量为 0，不执行对冲. Data: {fill_data}")
        return False

    # 按实际已成比例，对冲另一腿的市价规模
    scale = filled_qty / max(1e-12, maker_qty)
    taker_filled_qty = taker_qty * scale

    # 决定 taker 腿的方向
    taker_is_hedge = not maker_is_hedge
    if taker_is_hedge:
        taker_side = "SELL" if side == "POS" else "BUY"
    else: # taker is quote
        taker_side = "BUY" if side == "POS" else "SELL"

    logging.info(f"[HYB] Maker order {maker_oid} filled {filled_qty}. Executing taker leg: {taker_side} {taker_filled_qty} of {taker_leg.symbol}")

    try:
        # 入场对冲不能用 reduceOnly，否则 perp 可能直接拒单
        taker_leg.place_market(taker_side, taker_filled_qty, reduce_only=False)
    except Exception as e:
        logging.error(f"[HYB] taker 对冲失败: {e}")
        # 这里需要考虑补偿逻辑，例如立即市价平掉 maker 腿的仓位
        return False

    return True


async def try_enter_unified():
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

    # 取盘口与参考价（仅 WS；若缺失/过旧则直接跳过本轮）
    try:
        q_bids, q_asks = quote.get_books(limit=1)
        h_bids, h_asks = hedge.get_books(limit=1)
        q_ref = quote.ref_price()
        h_ref = hedge.ref_price()
    except Exception as e:
        logging.info("skip cycle: market data not ready/stale: %s", e)
        return (False, None)
    sp_bps_ref = _spread_bps(q_ref, h_ref)
    logging.info("Ref prices: quote=%.4f hedge=%.4f | spread=%.2fbps", q_ref, h_ref, sp_bps_ref)

    # 先做参考价级别的“是否值得继续”快速拦截
    #if ONLY_POSITIVE_CARRY and sp_bps_ref < 0:
    #    logging.info("❌ 仅正向套利，参考价 spread=%.2fbps < 0，放弃", sp_bps_ref)
    #    return (False, None)

    # 获取两腿上一分钟的成交额（REST；放到线程池避免阻塞事件循环）
    try:
        logging.info("quote.symbol=%s hedge.symbol=%s", quote.symbol, hedge.symbol)
        if not hasattr(quote, "symbol") or quote.symbol is None:
            raise AttributeError("The 'quote' object does not have a valid 'symbol' attribute.")
        loop = asyncio.get_running_loop()
        last_minute_volume = await loop.run_in_executor(None, rest.get_last_minute_volume, quote.symbol, QUOTE_KIND)
        if last_minute_volume is None:
            raise ValueError(f"Failed to fetch last minute volume for symbol: {quote.symbol}")
        V_q_last_min = float(last_minute_volume)
        h_volume = await loop.run_in_executor(None, rest.get_last_minute_volume, getattr(hedge, "symbol", ""), HEDGE_KIND)
        if h_volume is None:
            raise ValueError(f"Failed to fetch last minute volume for hedge symbol: {getattr(hedge, 'symbol', '')}")
        V_h_last_min = float(h_volume)
    except Exception as e:
        logging.error("获取上一分钟成交额失败: %s", e)
        return (False, None)

    # 计算 1% 的流动性上限
    logging.info("上一分钟成交额：quote=%.2f USD hedge=%.2f USD", V_q_last_min, V_h_last_min)
    V_q_cap_1pct = V_q_last_min * 0.01
    V_h_cap_1pct = V_h_last_min * 0.01
    
    # 比较：计划投入资金 V_USD (名义目标) 是否小于上限
    V_trade = V_USD # V_USD 是您计划投入的名义目标

    # 需要满足两腿同时小于各自的 1% 上限
    if V_trade > V_q_cap_1pct or V_trade > V_h_cap_1pct:
        logging.info(
            "❌ 流动性不足。V_USD=%.2f超限。Q上限:%.2f (1%% of %.2f), H上限:%.2f (1%% of %.2f)",
            V_trade, V_q_cap_1pct, V_q_last_min, V_h_cap_1pct, V_h_last_min
        )
        return (False, None)
    
    logging.info("✅ 流动性检查通过。投入资金 V_USD=%.2f 在两腿 1%% 范围内。", V_trade)

    # 改成直接按照卖一价候选 
    rows_pos, rows_neg = _get_candidate(
        q_bids, q_asks, h_bids, h_asks,
        min_bp=ENTER_BPS, 
        only_positive_carry=ONLY_POSITIVE_CARRY
    )
    logging.info("Collected %d POS candidates, %d NEG candidates", len(rows_pos), len(rows_neg))

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
        logging.debug("No valid arbitrage candidate found in this cycle.")
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
        logging.info("Executing HYBRID maker_then_taker...")
        # 由 HYBRID_MAKER_LEG=quote|hedge 决定先 maker 的腿
        maker_first = (HYBRID_MAKER_LEG == "quote")
        maker_leg   = quote if maker_first else hedge
        taker_leg   = hedge if maker_first else quote
        maker_bids, maker_asks = (q_bids, q_asks) if maker_first else (h_bids, h_asks)
        taker_bids, taker_asks = (h_bids, h_asks) if maker_first else (q_bids, q_asks)

        ok = await _hybrid_maker_then_taker(
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
        if DRY_RUN:
            logging.info(
                "[DRY] taker mode would trade: side=%s q=%s qty=%.6f h=%s qty=%.6f | skip placing orders",
                side,
                getattr(quote, "symbol", "?"),
                q_qty,
                getattr(hedge, "symbol", "?"),
                h_qty,
            )
            o_h = o_q = {}
        elif side == "POS":
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
        logging.debug(
            "Exit condition not met. Side: %s, Current Spread: %.2f bps, Held: %.1f s",
            position.side, sp_bps, held_sec
        )
        return (False, "")

    # ===== 触发 → 直接在此处执行平仓（原 try_close 内联） =====
    logging.info("Exit triggered! Reason: %s. Closing position: %s", reason, position)
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

    if DRY_RUN:
        logging.info(
            "[DRY] would close position side=%s (hedge=%s qty=%s, quote=%s qty=%s); skip placing orders",
            position.side,
            getattr(hedge, "symbol", "?"),
            h_qty,
            getattr(quote, "symbol", "?"),
            q_qty,
        )
        return (True, reason)

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

