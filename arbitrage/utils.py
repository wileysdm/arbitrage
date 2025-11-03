import time, hmac, hashlib, urllib.parse, os, csv
from datetime import datetime
from math import floor, ceil
from arbitrage.config import TRADES_CSV

def ts_ms(): return int(time.time() * 1000)

def sign_query(secret: str, params: dict) -> str:
    q = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + "&signature=" + sig

def round_step(x, step, mode="floor"):
    step = float(step)
    n = x/step
    if mode == "ceil": n = ceil(n-1e-12)
    elif mode == "round": n = round(n)
    else: n = floor(n+1e-12)
    return max(step, n*step)

def vwap_to_qty(levels, target_qty):
    if target_qty <= 0 or not levels: return 0.0, None
    remain, notional, filled = target_qty, 0.0, 0.0
    for px, q in levels:
        if remain <= 1e-12: break
        take = min(remain, q)
        notional += take*px
        filled   += take
        remain   -= take
    if filled <= 0: return 0.0, None
    return filled, (notional/filled)

# —— 交易日志（CSV） ——
TRADE_FIELDS = [
    "ts_ms","ts_iso","event","trade_id","side",
    "Q_btc","N_cntr","spot_vwap","perp_vwap",
    "spot_orderId","cm_orderId",
    "fee_btc_spot","fee_btc_cm",
    "income_btc","delta_btc"
]
_SEQ = 0
def next_trade_id():
    global _SEQ
    _SEQ += 1
    return f"T{int(time.time())}-{_SEQ}"

def append_trade_row(row: dict):
    row = dict(row)
    if "ts_ms" not in row: row["ts_ms"] = ts_ms()
    row["ts_iso"] = datetime.utcfromtimestamp(row["ts_ms"]/1000).strftime("%Y-%m-%d %H:%M:%S")
    for k in TRADE_FIELDS:
        row.setdefault(k, "")
    need_header = not os.path.exists(TRADES_CSV) or os.path.getsize(TRADES_CSV) == 0
    with open(TRADES_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_FIELDS)
        if need_header: w.writeheader()
        w.writerow({k: row.get(k, "") for k in TRADE_FIELDS})
