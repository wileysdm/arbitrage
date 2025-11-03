# -*- coding: utf-8 -*-
from arbitrage.utils import round_step, next_trade_id, append_trade_row
from arbitrage.config import (
    ENABLE_FUNDING_INFO, FUNDING_BUFFER_SEC, MAX_HOLD_SEC,
    ENABLE_CM_RISK_CHECK, EXECUTION_MODE, MAX_Q_BTC_FRONTIER,
    HYBRID_MAKER_LEG, HYBRID_WAIT_SEC, HYBRID_MIN_FILL_RATIO
)
from arbitrage.exchanges.md_binance_rest import (
    get_spot_depth, get_coinm_depth, get_coinm_funding
)
from arbitrage.exchanges.exec_binance_rest import (
    place_spot_limit_maker, place_spot_market,
    place_coinm_limit, place_coinm_market
)
from arbitrage.exchanges.execution import (
    monitor_and_rescue_single_leg, hybrid_maker_then_taker
)
from arbitrage.strategy.risk import will_cross_next_funding, check_cm_margin_ok

# ===== 工具函数 =====
def _cum_usd_spot(levels):
    out, s = [], 0.0
    for px, q in levels: s += px*q; out.append(s)
    return out

def _cum_usd_cm(levels, C):
    out, n = [], 0.0
    for _, cn in levels: n += cn; out.append(n*C)
    return out

def _vwap_spot_for_usd(levels, V):
    spend, qty = 0.0, 0.0
    for px, avail in levels:
        if spend >= V - 1e-12: break
        cap = px*avail
        if spend + cap <= V: spend += cap; qty += avail
        else:
            need = V - spend; take = need/px
            qty += take; spend = V; break
    return qty, (spend/qty) if qty>0 else None

def _vwap_cm_for_usd(levels, V, C):
    N_target = V / C
    n, px_sum = 0.0, 0.0
    for px, cn in levels:
        if n >= N_target - 1e-12: break
        take = min(cn, N_target - n)
        px_sum += take*px; n += take
    return n, (px_sum/n) if n>0 else None

def print_per_level_book_edge(spot_bids, spot_asks, cm_bids, cm_asks,
                              contract_size_usd, max_levels=10, show_losers=False):
    def cross_print(tag, spot_side_levels, cm_side_levels, mode):
        V_spots = _cum_usd_spot(spot_side_levels)
        V_cms   = _cum_usd_cm(cm_side_levels, contract_size_usd)
        i = j = printed = 0; lines = []
        while i < len(V_spots) and j < len(V_cms) and printed < max_levels:
            V_match = V_spots[i] if V_spots[i] <= V_cms[j] else V_cms[j]
            Q_need, spot_v = _vwap_spot_for_usd(spot_side_levels[:i+1], V_match)
            N_need, perp_v = _vwap_cm_for_usd(cm_side_levels[:j+1], V_match, contract_size_usd)
            if spot_v and perp_v:
                arb_ratio = (perp_v/spot_v - 1.0) if mode=="forward" else (spot_v/perp_v - 1.0)
                if show_losers or arb_ratio > 0:
                    arb_bp   = arb_ratio * 10000.0
                    edge_usd = V_match * arb_ratio
                    lines.append(f"{i+1:>2}/{j+1:<2} | {V_match:>12.2f} | {spot_v:>9.2f} | {perp_v:>9.2f} | {arb_bp:>6.2f} | {edge_usd:>8.2f} | {Q_need:>6.4f} | {N_need:>6.2f}")
                    printed += 1
            if V_spots[i] < V_cms[j]: i += 1
            elif V_spots[i] > V_cms[j]: j += 1
            else: i += 1; j += 1
        if lines:
            print(f"\n—— 逐档评估（{tag}）——")
            print("i/j | V_match(USD) | spot_vwap | perp_vwap | arb_bp | edge_usd | Q_btc | N_cntr")
            for ln in lines: print(ln)

    cross_print("正向：买现货 / 卖合约", spot_asks, cm_bids, "forward")
    cross_print("反向：卖现货 / 买合约", spot_bids, cm_asks, "reverse")

def collect_frontier_candidates(spot_bids, spot_asks, cm_bids, cm_asks,
                                contract_size_usd, max_levels, min_bp, min_vusd,
                                only_positive_carry):
    def collect(spot_side_levels, cm_side_levels, mode):
        V_spots, V_cms = _cum_usd_spot(spot_side_levels), _cum_usd_cm(cm_side_levels, contract_size_usd)
        i = j = 0; rows = []
        while i < len(V_spots) and j < len(V_cms) and (i < max_levels and j < max_levels):
            V_match = V_spots[i] if V_spots[i] <= V_cms[j] else V_cms[j]
            Q_need, spot_v = _vwap_spot_for_usd(spot_side_levels[:i+1], V_match)
            N_need, perp_v = _vwap_cm_for_usd(cm_side_levels[:j+1], V_match, contract_size_usd)
            if spot_v and perp_v:
                arb_ratio = (perp_v/spot_v - 1.0) if mode=="fwd" else (spot_v/perp_v - 1.0)
                arb_bp, edge_usd = arb_ratio*10000.0, V_match*arb_ratio
                if (arb_bp >= min_bp) and (V_match >= min_vusd):
                    rows.append((i+1, j+1, V_match, spot_v, perp_v, arb_bp, edge_usd, Q_need, N_need))
            if V_spots[i] < V_cms[j]: i += 1
            elif V_spots[i] > V_cms[j]: j += 1
            else: i += 1; j += 1
        rows.sort(key=lambda r: r[6], reverse=True)
        return rows
    rows_fwd = collect(spot_asks, cm_bids, "fwd")
    rows_rev = [] if only_positive_carry else collect(spot_bids, cm_asks, "rev")
    return rows_fwd, rows_rev

# —— hybrid 决策用：估算“若作为 taker”的滑点 —— 
def _vwap_slippage_bps(levels, qty):
    if not levels or qty <= 0: return 1e9
    best = levels[0][0]
    remain, notional = qty, 0.0
    for px, q in levels:
        if remain <= 1e-12: break
        take = min(remain, q); notional += take*px; remain -= take
    filled = (qty - remain)
    if filled <= 0: return 1e9
    vwap = notional / filled
    return abs(vwap - best)/best*10000.0

# ===== 入场执行（支持 taker / maker / hybrid） =====
def place_entry_from_row(side, row, spot_step, cm_step, contract_size,
                         spot_symbol: str, coinm_symbol: str,
                         spot_bids=None, spot_asks=None, cm_bids=None, cm_asks=None):
    """
    新增了 4 个可选参数：spot_bids/spot_asks/cm_bids/cm_asks。
    若传入则优先使用（即 WS 维护的簿）；若未传或为空，退回 REST 获取 5 档。
    """
    i_s, j_c, V_row, sv, pv, bp, edge_row, Q_need, N_need = row
    Q0 = round_step(Q_need, spot_step, mode="floor")
    N0 = int(round_step(N_need, cm_step, mode="floor"))
    if Q0 <= 0 or N0 <= 0:
        print("规模经取整后无效，放弃。"); return (False, 0.0, 0, "")

    Q, N = Q0, N0; capped = False
    if MAX_Q_BTC_FRONTIER and Q0 > MAX_Q_BTC_FRONTIER:
        scale = MAX_Q_BTC_FRONTIER / Q0
        Q = round_step(MAX_Q_BTC_FRONTIER, spot_step, mode="floor")
        N_scaled = int(round_step(max(1.0, N0 * scale), cm_step, mode="floor"))
        N = min(N_scaled, N0); capped = True

    V_eff = min(Q * sv, N * contract_size)
    print(f"[CAND] side={side} | V_row≈{V_row:,.2f} USD | Q_row≈{Q0:.6f} | N_row≈{N0} | sv≈{sv:.2f} | pv≈{pv:.2f} | bp≈{bp:.2f}")
    if capped:
        print(f"[CAP ] MAX_Q_BTC_FRONTIER={MAX_Q_BTC_FRONTIER:.6f} → Q_used≈{Q:.6f}, N_used≈{N}")
    print(f"[EXEC] V_eff≈{V_eff:,.2f} USD | bp≈{bp:.2f} | Q≈{Q:.6f} | N≈{N}")

    # Funding 信息（仅打印）
    if ENABLE_FUNDING_INFO:
        fr_bp, nxt = get_coinm_funding(coinm_symbol)
        will_cross, eta_sec = will_cross_next_funding(nxt, MAX_HOLD_SEC, FUNDING_BUFFER_SEC)
        cross_txt = f"将跨结算(~{eta_sec:.0f}s)" if will_cross else "不跨结算"
        print(f"[Funding] fr={fr_bp:.2f}bp | {cross_txt}")

    # 保证金风控（入场前）
    if ENABLE_CM_RISK_CHECK:
        ok, info = check_cm_margin_ok(coinm_symbol)
        if not ok:
            print(f"❌ 保证金风控未通过：{info}；拒绝本次入场")
            return (False, 0.0, 0, "")
        else:
            print(f"✔ 保证金风控通过：{info}")

    exec_mode = (EXECUTION_MODE or "").lower()

    # ===== HYBRID：先在“深度更好”的一腿挂 maker，成交后另一腿 taker =====
    if exec_mode == "hybrid":
        # 若未传入簿 或 列表为空 → 退回 REST 5 档
        if not (spot_bids and spot_asks):
            spot_bids, spot_asks = get_spot_depth(5, symbol=spot_symbol)
        if not (cm_bids and cm_asks):
            cm_bids, cm_asks     = get_coinm_depth(5, symbol=coinm_symbol)

        maker_leg = (HYBRID_MAKER_LEG or "auto").lower()
        if maker_leg == "auto":
            if side == "POS":
                sl_spot = _vwap_slippage_bps(spot_asks, Q)   # 买现货作为 taker 的滑点
                sl_cm   = _vwap_slippage_bps(cm_bids,  N)   # 卖合约作为 taker 的滑点
            else:
                sl_spot = _vwap_slippage_bps(spot_bids, Q)  # 卖现货
                sl_cm   = _vwap_slippage_bps(cm_asks,  N)  # 买合约
            maker_leg = "spot" if sl_spot <= sl_cm else "coinm"

        print(f"[HYB ] maker_leg={maker_leg} | wait={HYBRID_WAIT_SEC:.2f}s | min_fill={HYBRID_MIN_FILL_RATIO:.2f}")

        ok, Q_used, N_used, soid, coid = hybrid_maker_then_taker(
            side, maker_leg, Q, N,
            spot_bids, spot_asks, cm_bids, cm_asks,
            spot_step, cm_step, contract_size,
            spot_symbol, coinm_symbol
        )
        if not ok:
            return (False, 0.0, 0, "")

        trade_id = next_trade_id()
        try:
            append_trade_row({
                "event": "OPEN", "trade_id": trade_id, "side": side,
                "Q_btc": f"{Q_used:.8f}", "N_cntr": int(N_used),
                "spot_vwap": f"{sv:.2f}", "perp_vwap": f"{pv:.2f}",
                "spot_orderId": soid or "", "cm_orderId": coid or "",
                "fee_btc_spot": "", "fee_btc_cm": "", "income_btc": "", "delta_btc": ""
            })
        except Exception as e:
            print("⚠ 写 OPEN 行失败：", e)

        return (True, Q_used, int(N_used), trade_id)

    # ===== TAKER / MAKER =====
    if exec_mode == "taker":
        if side == "POS":
            spot_o = place_spot_market("BUY", Q, symbol=spot_symbol)
            cm_o   = place_coinm_market("SELL", N, reduce_only=False, symbol=coinm_symbol)
        else:
            spot_o = place_spot_market("SELL", Q, symbol=spot_symbol)
            cm_o   = place_coinm_market("BUY",  N, reduce_only=False, symbol=coinm_symbol)
    else:
        # —— 这里优先用“传入的WS簿”取顶档；若为空再退回 REST 5 档 —— 
        if not (spot_bids and spot_asks):
            spot_bids, spot_asks = get_spot_depth(5, symbol=spot_symbol)
        if not (cm_bids and cm_asks):
            cm_bids, cm_asks     = get_coinm_depth(5, symbol=coinm_symbol)

        if side == "POS":
            px_spot = spot_bids[0][0]   # 买现货 → bid
            px_cm   = cm_asks[0][0]     # 卖合约 → ask
            spot_o = place_spot_limit_maker("BUY",  Q, px_spot, symbol=spot_symbol)
            cm_o   = place_coinm_limit     ("SELL", N, px_cm,   post_only=True, symbol=coinm_symbol)
        else:
            px_spot = spot_asks[0][0]   # 卖现货 → ask
            px_cm   = cm_bids[0][0]     # 买合约 → bid
            spot_o = place_spot_limit_maker("SELL", Q, px_spot, symbol=spot_symbol)
            cm_o   = place_coinm_limit     ("BUY",  N, px_cm,   post_only=True, symbol=coinm_symbol)

    trade_id = next_trade_id()

    # maker-maker / taker-taker 两腿并发：做单腿护航
    try:
        monitor_and_rescue_single_leg(side, spot_o, cm_o, Q, N, spot_symbol, coinm_symbol)
    except Exception as e:
        print("单腿监控异常（忽略继续）：", e)

    # 记账 OPEN
    try:
        append_trade_row({
            "event": "OPEN", "trade_id": trade_id, "side": side,
            "Q_btc": f"{Q:.8f}", "N_cntr": int(N),
            "spot_vwap": f"{sv:.2f}", "perp_vwap": f"{pv:.2f}",
            "spot_orderId": (spot_o.get("orderId","") if isinstance(spot_o, dict) else ""),
            "cm_orderId":   (cm_o.get("orderId","")   if isinstance(cm_o, dict)   else ""),
            "fee_btc_spot": "", "fee_btc_cm": "", "income_btc": "", "delta_btc": ""
        })
    except Exception as e:
        print("⚠ 写 OPEN 行失败：", e)

    return (True, Q, int(N), trade_id)
