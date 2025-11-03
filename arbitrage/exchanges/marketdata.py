# arbitrage/exchanges/marketdata.py
from typing import Protocol, Tuple
from arbitrage.models import OrderBook

class MarketData(Protocol):
    def get_spot_depth(limit=100):
        d = r_get(SPOT_BASE, "/api/v3/depth", {"symbol": SPOT_SYMBOL, "limit": limit})
        bids = [(float(p), float(q)) for p,q in d["bids"]]
        asks = [(float(p), float(q)) for p,q in d["asks"]]
        return bids, asks

    def get_coinm_depth(limit=100):
        d = r_get(DAPI_BASE, "/dapi/v1/depth", {"symbol": COINM_SYMBOL, "limit": limit})
        bids = [(float(p), float(q)) for p,q in d["bids"]]  # qty = 合约张数
        asks = [(float(p), float(q)) for p,q in d["asks"]]
        return bids, asks

    def get_coinm_mark():
        rows = r_get(DAPI_BASE, "/dapi/v1/premiumIndex", {"symbol": COINM_SYMBOL})
        if not isinstance(rows, list):
            raise TypeError(f"Expect list from premiumIndex, got {type(rows).__name__}")
        for it in rows:
            if it.get("symbol") == COINM_SYMBOL:
                return float(it["markPrice"])
        raise RuntimeError(f"premiumIndex: {COINM_SYMBOL} not found")
