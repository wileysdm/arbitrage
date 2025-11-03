import requests
from arbitrage.config import SPOT_BASE, DAPI_BASE, SPOT_KEY, SPOT_SECRET, DAPI_KEY, DAPI_SECRET
from arbitrage.utils import ts_ms, sign_query

_session = requests.Session()
_session.headers.update({"Content-Type": "application/x-www-form-urlencoded"})

def r_get(base, path, params=None, key=None):
    url = base + path
    headers = {"X-MBX-APIKEY": key} if key else {}
    r = _session.get(url, params=params or {}, headers=headers, timeout=10)
    r.raise_for_status();  return r.json()

def r_signed(base, path, method, params, key, secret):
    params = {**params, "timestamp": ts_ms(), "recvWindow": 5000}
    body = sign_query(secret, params)
    headers = {"X-MBX-APIKEY": key}
    url = base + path
    if method == "POST":
        r = _session.post(url, data=body, headers=headers, timeout=10)
    elif method == "DELETE":
        r = _session.delete(url, data=body, headers=headers, timeout=10)
    else:
        r = _session.get(url, params=body, headers=headers, timeout=10)
    r.raise_for_status();  return r.json()

# 方便外部调用时不重复传 key/secret
def spot_signed(path, method, params):
    return r_signed(SPOT_BASE, path, method, params, SPOT_KEY, SPOT_SECRET)

def dapi_signed(path, method, params):
    return r_signed(DAPI_BASE, path, method, params, DAPI_KEY, DAPI_SECRET)

def spot_get(path, params=None):
    return r_get(SPOT_BASE, path, params, key=SPOT_KEY)

def dapi_get(path, params=None):
    return r_get(DAPI_BASE, path, params, key=DAPI_KEY)
