from arbitrage.config import SPOT_SYMBOL, COINM_SYMBOL
from arbitrage.exchanges.binance_rest import spot_get, dapi_get
import time

def get_spot_depth(limit=100, symbol: str | None = None):
    sym = symbol or SPOT_SYMBOL
    d = spot_get("/api/v3/depth", {"symbol": sym, "limit": limit})
    bids = [(float(p), float(q)) for p,q in d["bids"]]
    asks = [(float(p), float(q)) for p,q in d["asks"]]
    return bids, asks

def get_coinm_depth(limit=100, symbol: str | None = None):
    sym = symbol or COINM_SYMBOL
    d = dapi_get("/dapi/v1/depth", {"symbol": sym, "limit": limit})
    bids = [(float(p), float(q)) for p,q in d["bids"]]
    asks = [(float(p), float(q)) for p,q in d["asks"]]
    return bids, asks

def get_coinm_mark(symbol: str | None = None):
    sym = symbol or COINM_SYMBOL
    rows = dapi_get("/dapi/v1/premiumIndex", {"symbol": sym})
    if isinstance(rows, list):
        for it in rows:
            if it.get("symbol") == sym:
                return float(it["markPrice"])
    else:
        if rows.get("symbol") == sym:
            return float(rows["markPrice"])
    raise RuntimeError("premiumIndex: symbol not found")

def get_coinm_funding(symbol: str | None = None):
    sym = symbol or COINM_SYMBOL
    rows = dapi_get("/dapi/v1/premiumIndex", {"symbol": sym})
    if isinstance(rows, list):
        for it in rows:
            if it.get("symbol") == sym:
                fr = float(it.get("lastFundingRate", 0.0) or 0.0) * 10000.0
                nxt = it.get("nextFundingTime")
                nxt = int(nxt) if nxt not in (None, "") else None
                return fr, nxt
    else:
        if rows.get("symbol") == sym:
            fr = float(rows.get("lastFundingRate", 0.0) or 0.0) * 10000.0
            nxt = rows.get("nextFundingTime")
            nxt = int(nxt) if nxt not in (None, "") else None
            return fr, nxt
    return 0.0, None

# with timestamp
def get_spot_depth_with_ts(limit=100, symbol: str | None = None):
    t0 = time.time()
    bids, asks = get_spot_depth(limit, symbol)
    return bids, asks, t0

def get_coinm_depth_with_ts(limit=100, symbol: str | None = None):
    t0 = time.time()
    bids, asks = get_coinm_depth(limit, symbol)
    return bids, asks, t0
