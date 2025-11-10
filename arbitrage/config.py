# -*- coding: utf-8 -*-
"""
全局配置（环境变量为主）
"""
from __future__ import annotations
import os
from dotenv import load_dotenv, find_dotenv

_dotenv_path = find_dotenv(usecwd=True)
if _dotenv_path:
    load_dotenv(_dotenv_path)

def _get_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None: return default
    return v.lower() in ("1","true","yes","y","on")

# —— 交易端点（保留你原有命名）——
SPOT_BASE = os.environ.get("SPOT_BASE", "https://api.binance.com")
DAPI_BASE = os.environ.get("DAPI_BASE", "https://dapi.binance.com")
FAPI_BASE = os.environ.get("FAPI_BASE", "https://fapi.binance.com")

WS_SPOT = os.environ.get("WS_SPOT", "wss://stream.binance.com:9443")
WS_CM   = os.environ.get("WS_CM", "wss://dstream.binance.com")
WS_UM   = os.environ.get("WS_UM", "wss://fstream.binance.com")

# —— API Keys ——（默认回落：UM/DAPI 沿用 SPOT 的 KEY/SECRET，便于本地调试）
SPOT_KEY    = os.environ.get("SPOT_KEY"   , "")
SPOT_SECRET = os.environ.get("SPOT_SECRET", "")
DAPI_KEY    = os.environ.get("DAPI_KEY"   , SPOT_KEY)
DAPI_SECRET = os.environ.get("DAPI_SECRET", SPOT_SECRET)
UM_KEY      = os.environ.get("UM_KEY"     , SPOT_KEY)
UM_SECRET   = os.environ.get("UM_SECRET"  , SPOT_SECRET)

# —— 标的与腿类型 ——（并列）
PAIR = os.environ.get("PAIR", "BTC")
SPOT_SYMBOL   = os.environ.get("SPOT_SYMBOL",   f"{PAIR}USDT")
COINM_SYMBOL  = os.environ.get("COINM_SYMBOL",  f"{PAIR}USD_PERP")

# 腿类型：'spot' | 'coinm' | 'usdtm' | 'usdcm'
HEDGE_KIND = os.environ.get("HEDGE_KIND", "spot").lower()
QUOTE_KIND = os.environ.get("QUOTE_KIND", "coinm").lower()

HEDGE_SYMBOL = os.environ.get(
    "HEDGE_SYMBOL",
    SPOT_SYMBOL if HEDGE_KIND == "spot"
    else (f"{PAIR}USDT" if HEDGE_KIND in ("usdtm","usdcm") else COINM_SYMBOL)
)
QUOTE_SYMBOL = os.environ.get(
    "QUOTE_SYMBOL",
    COINM_SYMBOL if QUOTE_KIND == "coinm" else f"{PAIR}USDT"
)

# —— 策略参数 ——
ONLY_POSITIVE_CARRY      = _get_bool("ONLY_POSITIVE_CARRY", False)
ENTER_BPS                = float(os.environ.get("ENTER_BPS", "2.0"))    # 触发开仓阈值（bps）
EXIT_BPS                 = float(os.environ.get("EXIT_BPS",  "0.5"))    # 盈利退出 bps
STOP_BPS                 = float(os.environ.get("STOP_BPS",  "10.0"))   # 止损 bps
MAX_HOLD_SEC             = int(os.environ.get("MAX_HOLD_SEC", "1800"))  # 强制离场秒数（默认30min）

V_USD                    = float(os.environ.get("V_USD", "1000"))       # 单次名义本金（USD）

MAX_SLIPPAGE_BPS_SPOT    = float(os.environ.get("MAX_SLIPPAGE_BPS_SPOT", "5.0"))
MAX_SLIPPAGE_BPS_COINM   = float(os.environ.get("MAX_SLIPPAGE_BPS_COINM","5.0"))

EXECUTION_MODE           = os.environ.get("EXECUTION_MODE", "taker")    # taker/maker
