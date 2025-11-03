from arbitrage.exchanges.exec_binance_rest import dapi_position_risk, dapi_account
from arbitrage.exchanges.md_binance_rest import get_coinm_mark
from arbitrage.config import LIQ_DIST_MIN_PCT, MARGIN_RATIO_MAX

def check_cm_margin_ok(symbol=None):
    try:
        risks = dapi_position_risk()
        rp = None
        for it in risks if isinstance(risks, list) else [risks]:
            if symbol is None or it.get("symbol") == symbol:
                rp = it; break

        mark = get_coinm_mark(symbol)
        liq  = float(rp.get("liquidationPrice", 0.0) if rp else 0.0)
        if liq and liq > 0: dist = abs((mark - liq) / mark)
        else: dist = 1.0

        acct = dapi_account()
        total_maint = float(acct.get("totalMaintMargin", 0.0) or 0.0)
        wallet_bal  = float(acct.get("totalWalletBalance", 0.0) or 0.0)
        maint_ratio = (total_maint / wallet_bal) if wallet_bal > 0 else 0.0

        if dist < LIQ_DIST_MIN_PCT:
            return False, f"距强平仅 {dist*100:.2f}% < {LIQ_DIST_MIN_PCT*100:.2f}%"
        if maint_ratio > MARGIN_RATIO_MAX:
            return False, f"维护保证金比 {maint_ratio:.2f} > {MARGIN_RATIO_MAX:.2f}"
        return True, f"OK | dist={dist*100:.2f}% | maint={maint_ratio:.2f}"
    except Exception as e:
        return False, f"风险检查异常：{e}"

def will_cross_next_funding(next_ts_ms, hold_sec, buffer_sec):
    if not next_ts_ms: return (False, None)
    import time
    now_ms = int(time.time()*1000)
    eta_sec = max(0, (next_ts_ms - now_ms) / 1000.0)
    return (hold_sec >= max(0.0, eta_sec - buffer_sec)), eta_sec
