from arbitrage.exchanges.binance_rest import spot_get, dapi_get

def fetch_spot_rules(symbol: str):
    info = spot_get("/api/v3/exchangeInfo", {"symbol": symbol})
    sym = info["symbols"][0]
    price_tick = [f for f in sym["filters"] if f["filterType"]=="PRICE_FILTER"][0]["tickSize"]
    lot_step   = [f for f in sym["filters"] if f["filterType"]=="LOT_SIZE"][0]["stepSize"]
    return float(price_tick), float(lot_step)

def fetch_coinm_rules(symbol: str):
    info = dapi_get("/dapi/v1/exchangeInfo", {"symbol": symbol})
    sym = info["symbols"][0]
    contract_size = float(sym.get("contractSize", 100.0))
    price_tick = [f for f in sym["filters"] if f["filterType"]=="PRICE_FILTER"][0]["tickSize"]
    lot_step   = [f for f in sym["filters"] if f["filterType"]=="LOT_SIZE"][0]["stepSize"]
    return contract_size, float(price_tick), float(lot_step)
