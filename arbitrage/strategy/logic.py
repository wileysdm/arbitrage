# -*- coding: utf-8 -*-
import time
from arbitrage.models import Position
from arbitrage.config import (
    ONLY_POSITIVE_CARRY, ENTER_BPS, EXIT_BPS, STOP_BPS, MAX_HOLD_SEC,
    V_USD, MAX_SLIPPAGE_BPS_SPOT, MAX_SLIPPAGE_BPS_COINM,
    EXECUTION_MODE
)
from arbitrage.utils import vwap_to_qty, append_trade_row
from arbitrage.strategy.frontier import (
    collect_frontier_candidates, print_per_level_book_edge, place_entry_from_row
)
from arbitrage.exchanges.exec_binance_rest import (
    place_spot_limit_maker, place_coinm_limit,
    place_spot_market, place_coinm_market
)

def _base_from_spot_symbol(sym: str) -> str:
    s = sym.upper()
    for suf in ("USDT","USDC","BUSD","FDUSD","TUSD","USD"):
        if s.endswith(suf):
            return s[: -len(suf)]
    return s

def vwap_slippage_bps(levels, qty):
    if not levels: return 0.0, None, None
    best = levels[0][0]
    filled, vwap = vwap_to_qty(levels, qty)
    if not vwap: return 0.0, None, None
    bps = abs(vwap - best)/best*10000.0
    return filled, vwap, bps

def print_levels_if_needed(PRINT_LEVELS, position, AUTO_FROM_FRONTIER,
                           spot_bids, spot_asks, cm_bids, cm_asks, contract_size, LEVELS_TO_PRINT):
    if PRINT_LEVELS and position is None and not AUTO_FROM_FRONTIER:
        print_per_level_book_edge(
            spot_bids, spot_asks, cm_bids, cm_asks,
            contract_size_usd=contract_size, max_levels=LEVELS_TO_PRINT
        )

def try_enter_from_frontier(spot_bids, spot_asks, cm_bids, cm_asks,
                            contract_size, spot_step, cm_step,
                            spot_symbol: str, coinm_symbol: str):
    rows_fwd, rows_rev = collect_frontier_candidates(
        spot_bids, spot_asks, cm_bids, cm_asks,
        contract_size_usd=contract_size, max_levels=50,
        min_bp=ENTER_BPS, min_vusd=5000.0,
        only_positive_carry=ONLY_POSITIVE_CARRY
    )
    cand = rows_fwd if ONLY_POSITIVE_CARRY else (rows_fwd + rows_rev)
    if not cand: return False, None
    best = max(cand, key=lambda r: r[6])
    side = "POS" if best in rows_fwd else "NEG"

    # —— 关键修改：把“当前WS簿”继续传给 place_entry_from_row（下单也用WS）——
    ok, Q_used, N_used, tid = place_entry_from_row(
        side, best, spot_step, cm_step, contract_size,
        spot_symbol, coinm_symbol,
        spot_bids, spot_asks, cm_bids, cm_asks
    )
    if ok:
        return True, Position(
            side=side, Q=Q_used, N=N_used, trade_id=tid,
            spot_symbol=spot_symbol, coinm_symbol=coinm_symbol
        )
    return False, None

def try_enter(spread_bps, spot_mid, spot_bids, spot_asks, cm_bids, cm_asks,
              contract_size, spot_step, cm_step,
              spot_symbol: str, coinm_symbol: str):
    """
    简单阈值入场（非 frontier）。支持 EXECUTION_MODE:
      - "taker": 两腿走市价
      - 其他（"maker"）: 两腿挂 LIMIT_MAKER（post-only）
    这里的报价也直接使用传入的 WS 簿顶档。
    """
    if ONLY_POSITIVE_CARRY and spread_bps <= ENTER_BPS: return (False, None)
    if abs(spread_bps) < ENTER_BPS: return (False, None)

    Q = V_USD / max(1e-12, float(spot_mid))
    N = V_USD / max(1e-12, float(contract_size))
    base = _base_from_spot_symbol(spot_symbol)
    mode = (EXECUTION_MODE or "").lower()

    if spread_bps > 0:
        # 正向：买现货 / 卖合约
        filled_q_buy, _, bps_spot = vwap_slippage_bps(spot_asks, Q)
        filled_n_sell, _, bps_cm  = vwap_slippage_bps(cm_bids,  N)
        if filled_q_buy < Q or (bps_spot and bps_spot > MAX_SLIPPAGE_BPS_SPOT) or (bps_cm and bps_cm > MAX_SLIPPAGE_BPS_COINM):
            print(f"❌ 深度/滑点不满足: spot {bps_spot:.2f}bp, coinm {bps_cm:.2f}bp");  return (False, None)

        if mode == "taker":
            print(f"✔ 入场正向[Taker]：Q≈{Q:.6f} {base}, N≈{N:.0f} 张")
            place_spot_market("BUY",  Q, symbol=spot_symbol)
            place_coinm_market("SELL", N, reduce_only=False, symbol=coinm_symbol)
        else:
            px_spot_bid = spot_bids[0][0]
            px_cm_ask   = cm_asks[0][0]
            print(f"✔ 入场正向[Maker]：Q≈{Q:.6f} {base}, N≈{N:.0f} 张 | spot_bid={px_spot_bid:.2f}, cm_ask={px_cm_ask:.1f}")
            place_spot_limit_maker("BUY",  Q, px_spot_bid, symbol=spot_symbol)
            place_coinm_limit     ("SELL", N, px_cm_ask, post_only=True, symbol=coinm_symbol)
        return True, Position(side="POS", Q=Q, N=int(N), spot_symbol=spot_symbol, coinm_symbol=coinm_symbol)

    else:
        # 反向：卖现货 / 买合约
        filled_q_sell, _, bps_spot = vwap_slippage_bps(spot_bids, Q)
        filled_n_buy,  _, bps_cm   = vwap_slippage_bps(cm_asks,  N)
        if filled_q_sell < Q or (bps_spot and bps_spot > MAX_SLIPPAGE_BPS_SPOT) or (bps_cm and bps_cm > MAX_SLIPPAGE_BPS_COINM):
            print(f"❌ 深度/滑点不满足(反向): spot {bps_spot:.2f}bp, coinm {bps_cm:.2f}bp");  return (False, None)

        if mode == "taker":
            print(f"✔ 入场反向[Taker]：Q≈{Q:.6f} {base}, N≈{N:.0f} 张")
            place_spot_market("SELL", Q, symbol=spot_symbol)
            place_coinm_market("BUY",  N, reduce_only=False, symbol=coinm_symbol)
        else:
            px_spot_ask = spot_asks[0][0]
            px_cm_bid   = cm_bids[0][0]
            print(f"✔ 入场反向[Maker]：Q≈{Q:.6f} {base}, N≈{N:.0f} 张 | spot_ask={px_spot_ask:.2f}, cm_bid={px_cm_bid:.1f}")
            place_spot_limit_maker("SELL", Q, px_spot_ask, symbol=spot_symbol)
            place_coinm_limit     ("BUY",  N, px_cm_bid, post_only=True, symbol=coinm_symbol)
        return True, Position(side="NEG", Q=Q, N=int(N), spot_symbol=spot_symbol, coinm_symbol=coinm_symbol)

def need_exit(spread_bps, position):
    if position is None:
        return False, "NO_POS"
    age = time.time() - position.t0
    if abs(spread_bps) <= EXIT_BPS: return True, "REVERT"
    if abs(spread_bps) >= STOP_BPS: return True, "STOP"
    if age >= MAX_HOLD_SEC:         return True, "TIME"
    return False, ""

def do_exit(position: Position):
    print(f"→ 平仓触发：side={position.side}, Q={position.Q}, N={position.N}（MARKET 平仓）")
    if position.side == "POS":
        so = place_spot_market("SELL", position.Q, symbol=position.spot_symbol)
        co = place_coinm_market("BUY",  position.N, reduce_only=True, symbol=position.coinm_symbol)
    else:
        so = place_spot_market("BUY",  position.Q, symbol=position.spot_symbol)
        co = place_coinm_market("SELL", position.N, reduce_only=True, symbol=position.coinm_symbol)

    append_trade_row({
        "event": "CLOSE",
        "trade_id": position.trade_id,
        "side": position.side,
        "Q_btc": f"{position.Q:.8f}",
        "N_cntr": int(position.N),
        "spot_orderId": (so.get("orderId","") if isinstance(so, dict) else ""),
        "cm_orderId":   (co.get("orderId","") if isinstance(co, dict) else ""),
    })
