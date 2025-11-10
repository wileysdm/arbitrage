# -*- coding: utf-8 -*-
# arbitrage/exchanges/binance_rest.py
"""
Binance REST HTTP 底座（自带签名与重试、代理）
- r_get / r_post / r_put / r_delete      （无需签名；listenKey 等用）
- r_signed(base, path, method, params, key, secret, recv_window=5000)
  （带 HMAC-SHA256 签名；期货/现货下单、查单等用）
"""

from __future__ import annotations
import logging
import os, time, hmac, hashlib, urllib.parse
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- 内置工具（去除对 utils 依赖） ----------------
def ts_ms() -> int:
    return int(time.time() * 1000)

def _urlencode(params: Dict[str, Any]) -> str:
    return urllib.parse.urlencode(params, doseq=True)

def _sign_query(secret: str, params: Dict[str, Any]) -> str:
    q = _urlencode(params)
    sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
    return q + "&signature=" + sig

# ---------------- 全局会话（重试、代理） ----------------
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.2, status_forcelist=(429, 500, 502, 503, 504))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    # 代理（显式支持 HTTP_PROXY/HTTPS_PROXY）
    hp = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    sp = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    proxies = {}
    if hp: proxies["http"] = hp
    if sp: proxies["https"] = sp
    if proxies:
        s.proxies.update(proxies)
    s.headers.update({"User-Agent": "arb-exec/1.0"})
    return s

_S = _make_session()

def _headers(api_key: Optional[str] = None) -> Dict[str, str]:
    h = {}
    if api_key:
        h["X-MBX-APIKEY"] = api_key
    return h

def _abs_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}{path}"

# ---------------- 无签名请求 ----------------
def r_get(base: str, path: str, params: Optional[Dict[str, Any]] = None, key: Optional[str] = None) -> Any:
    url = _abs_url(base, path)
    r = _S.get(url, params=params or {}, headers=_headers(key), timeout=10)
    r.raise_for_status()
    return r.json()

def r_post(base: str, path: str, params: Optional[Dict[str, Any]] = None, key: Optional[str] = None) -> Any:
    url = _abs_url(base, path)
    # Binance 对无签名 POST 一般要求 x-www-form-urlencoded
    r = _S.post(url, data=params or {}, headers=_headers(key), timeout=10)
    r.raise_for_status()
    return r.json()

def r_put(base: str, path: str, params: Optional[Dict[str, Any]] = None, key: Optional[str] = None) -> Any:
    url = _abs_url(base, path)
    r = _S.put(url, data=params or {}, headers=_headers(key), timeout=10)
    r.raise_for_status()
    return r.json()

def r_delete(base: str, path: str, params: Optional[Dict[str, Any]] = None, key: Optional[str] = None) -> Any:
    url = _abs_url(base, path)
    r = _S.delete(url, data=params or {}, headers=_headers(key), timeout=10)
    r.raise_for_status()
    return r.json()

# ---------------- 带签名请求（现货/合约统一） ----------------
def r_signed(base: str,
             path: str,
             method: str,
             params: Optional[Dict[str, Any]],
             key: str,
             secret: str,
             recv_window: int = 5000) -> Any:
    """
    - 自动追加 recvWindow / timestamp
    - 对 params 做 urlencode 并签名（signature 放最后）
    - GET：放在 query；POST/PUT/DELETE：放在 body（x-www-form-urlencoded）
    """
    url = _abs_url(base, path)
    p = dict(params or {})
    if "recvWindow" not in p:
        p["recvWindow"] = int(recv_window)
    if "timestamp" not in p:
        p["timestamp"] = ts_ms()

    body_or_query = _sign_query(secret, p)

    method = method.upper()
    headers = _headers(key)

    if method == "GET":
        # query string 方式
        full_url = url + "?" + body_or_query
        logging.info("r_signed GET: %s", full_url + str(headers) + "10")
        r = _S.get(full_url, headers=headers, timeout=10)
    elif method == "POST":
        logging.info("r_signed POST: %s", url + str(body_or_query) + str(headers) + "10")
        r = _S.post(url, data=body_or_query, headers=headers, timeout=10)
    elif method == "PUT":
        logging.info("r_signed PUT: %s", url + str(body_or_query) + str(headers) + "10")
        r = _S.put(url, data=body_or_query, headers=headers, timeout=10)
    elif method == "DELETE":
        logging.info("r_signed DELETE: %s", url + str(body_or_query) + str(headers) + "10")
        r = _S.delete(url, data=body_or_query, headers=headers, timeout=10)
    else:
        raise ValueError(f"unsupported method: {method}")

    if r.status_code != 200:
        logging.error("Binance API Error Response (%s): %s", r.status_code, r.text)

    r.raise_for_status()
    return r.json()
