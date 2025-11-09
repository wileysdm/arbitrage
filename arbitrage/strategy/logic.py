# -*- coding: utf-8 -*-
"""
统一腿架构的策略逻辑（quote/hedge 并列 & 合并同类项）
- 支持腿类型：spot / coinm / usdtm / usdcm
- 入口：
    * try_enter_unified()             —— 直接从配置取两腿，统一判定并执行
    * try_enter(...)                  —— 向后兼容 coinm↔spot 的老签名；若非该组合则路由到 unified
    * try_enter_from_frontier(...)    —— 仅当 quote=coinm 且 hedge=spot 时使用；否则路由到 unified
- 退出：
    * try_exit_unified(position)      —— 统一的判定与市价平仓
    * try_close(position)             —— 直接市价对冲离场（不判定）
"""
from __future__ import annotations

import time
from typing import Tuple, List, Optional

from arbitrage.models import Position
from arbitrage.utils import vwap_to_qty, append_trade_row, next_trade_id

# ===== 配置常量 =====
from arbitrage.config import (
    ONLY_POSITIVE_CARRY, ENTER_BPS, EXIT_BPS, STOP_BPS, MAX_HOLD_SEC,
    V_USD, MAX_SLIPPAGE_BPS_SPOT, MAX_SLIPPAGE_BPS_COINM,
    EXECUTION_MODE, HEDGE_KIND, QUOTE_KIND, HEDGE_SYMBOL, QUOTE_SYMBOL
)

# 统一腿适配器
from arbitrage.exchanges.legs import make_leg

# 兼容：frontier（仅 coinm↔spot 使用）
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

# ---------------------------------------------------------
# 统一入场（两腿并列）
# ---------------------------------------------------------
def try_enter_unified():
    """
    两腿（quote & hedge）统一处理：
      - ref_price：perp 用 mark，spot 用 mid
      - qty：通过各自 qty_from_usd(V_USD) 计算
      - 执行：taker / maker 由 EXECUTION_MODE 决定（默认 taker）
    返回: (ok, Position|None)
    """
    quote = make_leg(QUOTE_KIND, QUOTE_SYMBOL)
    hedge = make_leg(HEDGE_KIND, HEDGE_SYMBOL)

    # 取盘口与参考价
    q_bids, q_asks = quote.get_books(limit=5)
    h_bids, h_asks = hedge.get_books(limit=5)
    q_ref = quote.ref_price()
    h_ref = hedge.ref_price()

    sp_bps = _spread_bps(q_ref, h_ref)
    if ONLY_POSITIVE_CARRY and sp_bps < 0:
        return (False, None)
    if abs(sp_bps) < ENTER_BPS:
        return (False, None)

    # 名义 → 数量
    q_qty = quote.qty_from_usd(V_USD)
    h_qty = hedge.qty_from_usd(V_USD)

    mode = (EXECUTION_MODE or "").lower()
    if sp_bps > 0:
        # 正向：买 hedge / 卖 quote
        h_sl = _slip_bps(h_asks, h_qty)   # 买看 ask
        q_sl = _slip_bps(q_bids, q_qty)   # 卖看 bid
        if h_sl > _slip_threshold(HEDGE_KIND) or q_sl > _slip_threshold(QUOTE_KIND):
            print(f"❌ 入场滑点超限 | hedge {h_sl:.2f}bp | quote {q_sl:.2f}bp")
            return (False, None)

        if mode == "maker":
            px_h = h_bids[0][0]  # 买 hedge → bid
            px_q = q_asks[0][0]  # 卖 quote → ask
            o_h = hedge.place_limit_maker("BUY",  h_qty, px_h)
            o_q = quote.place_limit_maker("SELL", q_qty, px_q)
        else:
            o_h = hedge.place_market("BUY",  h_qty)
            o_q = quote.place_market("SELL", q_qty)

        tid = next_trade_id()
        append_trade_row({
            "event": "OPEN", "trade_id": tid, "side": "POS",
            "Q_btc": f"{(h_qty if HEDGE_KIND!='coinm' else 0.0):.8f}",
            "N_cntr": int(q_qty) if QUOTE_KIND == "coinm" else 0,
            "spot_orderId": str(o_h.get("orderId","")) if isinstance(o_h, dict) else "",
            "cm_orderId":   str(o_q.get("orderId","")) if isinstance(o_q, dict) else "",
        })

        pos = Position(
            side="POS",
            Q=(h_qty if HEDGE_KIND!="coinm" else 0.0),
            N=(int(q_qty) if QUOTE_KIND=="coinm" else 0),
            spot_symbol=HEDGE_SYMBOL if HEDGE_KIND=="spot" else "",
            coinm_symbol=QUOTE_SYMBOL if QUOTE_KIND=="coinm" else "",
        )
        return (True, pos)

    else:
        # 反向：卖 hedge / 买 quote
        h_sl = _slip_bps(h_bids, h_qty)   # 卖看 bid
        q_sl = _slip_bps(q_asks, q_qty)   # 买看 ask
        if h_sl > _slip_threshold(HEDGE_KIND) or q_sl > _slip_threshold(QUOTE_KIND):
            print(f"❌ 入场滑点超限(反向) | hedge {h_sl:.2f}bp | quote {q_sl:.2f}bp")
            return (False, None)

        if mode == "maker":
            px_h = h_asks[0][0]  # 卖 hedge → ask
            px_q = q_bids[0][0]  # 买 quote → bid
            o_h = hedge.place_limit_maker("SELL", h_qty, px_h)
            o_q = quote.place_limit_maker("BUY",  q_qty, px_q)
        else:
            o_h = hedge.place_market("SELL", h_qty)
            o_q = quote.place_market("BUY",  q_qty)

        tid = next_trade_id()
        append_trade_row({
            "event": "OPEN", "trade_id": tid, "side": "NEG",
            "Q_btc": f"{(h_qty if HEDGE_KIND!='coinm' else 0.0):.8f}",
            "N_cntr": int(q_qty) if QUOTE_KIND == "coinm" else 0,
            "spot_orderId": str(o_h.get("orderId","")) if isinstance(o_h, dict) else "",
            "cm_orderId":   str(o_q.get("orderId","")) if isinstance(o_q, dict) else "",
        })

        pos = Position(
            side="NEG",
            Q=(h_qty if HEDGE_KIND!="coinm" else 0.0),
            N=(int(q_qty) if QUOTE_KIND=="coinm" else 0),
            spot_symbol=HEDGE_SYMBOL if HEDGE_KIND=="spot" else "",
            coinm_symbol=QUOTE_SYMBOL if QUOTE_KIND=="coinm" else "",
        )
        return (True, pos)

# ---------------------------------------------------------
# 兼容入口：老 coinm↔spot 形状
# ---------------------------------------------------------
def vwap_slippage_bps(levels: OrderBookSide, qty_or_cntr: float):
    filled, vwap = vwap_to_qty(levels, qty_or_cntr)
    if not vwap or filled <= 0:
        return 0.0, None, None
    best = levels[0][0]
    bps = abs(vwap - best)/best*10000.0
    return filled, vwap, bps

def try_enter(spread_bps: float, spot_mid: float,
              spot_bids: OrderBookSide, spot_asks: OrderBookSide,
              cm_bids: OrderBookSide, cm_asks: OrderBookSide,
              contract_size: float, spot_step: float, cm_step: float,
              spot_symbol: str, coinm_symbol: str):
    """
    老接口：coinm↔spot 的执行入口。
    若当前组合不是 (quote=coinm, hedge=spot)，则自动路由到 try_enter_unified()。
    """
    if not (QUOTE_KIND == "coinm" and HEDGE_KIND == "spot"):
        return try_enter_unified()

    if ONLY_POSITIVE_CARRY and spread_bps < 0:
        return (False, None)
    if abs(spread_bps) < ENTER_BPS:
        return (False, None)

    V = float(V_USD)
    Q = V / max(1e-12, spot_mid)  # 现货数量
    N = int(V / max(1e-12, contract_size))  # 合约张数

    mode = (EXECUTION_MODE or "").lower()
    if spread_bps > 0:
        # 正向：买现货 / 卖合约
        filled_q_buy, _, bps_spot = vwap_slippage_bps(spot_asks, Q)
        filled_n_sell, _, bps_cm  = vwap_slippage_bps(cm_bids,  N)
        if filled_q_buy < Q or (bps_spot and bps_spot > MAX_SLIPPAGE_BPS_SPOT) or (bps_cm and bps_cm > MAX_SLIPPAGE_BPS_COINM):
            print(f"❌ 深度/滑点不满足: spot {bps_spot:.2f}bp, coinm {bps_cm:.2f}bp");  return (False, None)

        if mode == "taker":
            place_spot_market("BUY",  Q, symbol=spot_symbol)
            place_coinm_market("SELL", N, reduce_only=False, symbol=coinm_symbol)
        else:
            px_spot_bid = spot_bids[0][0]
            px_cm_ask   = cm_asks[0][0]
            place_spot_limit_maker("BUY",  Q, px_spot_bid, symbol=spot_symbol)
            place_coinm_limit("SELL", N, px_cm_ask, post_only=True, symbol=coinm_symbol)

        tid = next_trade_id()
        append_trade_row({
            "event": "OPEN", "trade_id": tid, "side": "POS",
            "Q_btc": f"{Q:.8f}", "N_cntr": int(N),
            "spot_orderId": "", "cm_orderId": "",
        })
        return (True, Position(side="POS", Q=Q, N=N,
                               spot_symbol=spot_symbol, coinm_symbol=coinm_symbol))

    else:
        # 反向：卖现货 / 买合约
        filled_q_sell, _, bps_spot = vwap_slippage_bps(spot_bids, Q)
        filled_n_buy,  _, bps_cm   = vwap_slippage_bps(cm_asks,  N)
        if filled_q_sell < Q or (bps_spot and bps_spot > MAX_SLIPPAGE_BPS_SPOT) or (bps_cm and bps_cm > MAX_SLIPPAGE_BPS_COINM):
            print(f"❌ 深度/滑点不满足(反向): spot {bps_spot:.2f}bp, coinm {bps_cm:.2f}bp");  return (False, None)

        if mode == "taker":
            place_spot_market("SELL", Q, symbol=spot_symbol)
            place_coinm_market("BUY",  N, reduce_only=False, symbol=coinm_symbol)
        else:
            px_spot_ask = spot_asks[0][0]
            px_cm_bid   = cm_bids[0][0]
            place_spot_limit_maker("SELL", Q, px_spot_ask, symbol=spot_symbol)
            place_coinm_limit("BUY",  N, px_cm_bid,   post_only=True, symbol=coinm_symbol)

        tid = next_trade_id()
        append_trade_row({
            "event": "OPEN", "trade_id": tid, "side": "NEG",
            "Q_btc": f"{Q:.8f}", "N_cntr": int(N),
            "spot_orderId": "", "cm_orderId": "",
        })
        return (True, Position(side="NEG", Q=Q, N=N,
                               spot_symbol=spot_symbol, coinm_symbol=coinm_symbol))

def try_enter_from_frontier(spot_bids, spot_asks, cm_bids, cm_asks,
                            contract_size, spot_step, cm_step,
                            spot_symbol: str, coinm_symbol: str):
    """
    仅适用 coinm↔spot。否则路由到 try_enter_unified。
    """
    if not (QUOTE_KIND == "coinm" and HEDGE_KIND == "spot"):
        return try_enter_unified()

    rows_fwd, rows_rev = collect_frontier_candidates(
        spot_bids, spot_asks, cm_bids, cm_asks,
        contract_size_usd=contract_size, max_levels=50,
        min_bp=ENTER_BPS, min_vusd=V_USD,
        only_positive_carry=ONLY_POSITIVE_CARRY
    )
    cand = rows_fwd if ONLY_POSITIVE_CARRY else (rows_fwd + rows_rev)
    if not cand: return (False, None)

    row = sorted(cand, key=lambda r: -r["edge_bps"])[0]
    print_per_level_book_edge(spot_bids, spot_asks, cm_bids, cm_asks,
                              contract_size_usd=contract_size, max_levels=10)
    ok, pos = place_entry_from_row(row, spot_symbol, coinm_symbol, execution_mode=EXECUTION_MODE)
    return ok, pos

# ---------------------------------------------------------
# 统一退出/平仓
# ---------------------------------------------------------
def try_exit_unified(position: Position) -> Tuple[bool, str]:
    """
    统一判定 + 市价离场：
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

    # 触发 -> 平仓
    try_close(position)
    return (True, reason)

def try_close(position: Position):
    """
    统一平仓：直接按当前配置的两腿对冲离场（市价）。
    - coinm 使用 position.N；非 coinm（spot/UM）使用 position.Q；
    - 若某腿为 UM 但 position 没有数量记录，则按名义 V_USD 估算数量平仓（perp 使用 reduceOnly 防止反向开仓）。
    """
    hedge = make_leg(HEDGE_KIND, HEDGE_SYMBOL)
    quote = make_leg(QUOTE_KIND, QUOTE_SYMBOL)

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
