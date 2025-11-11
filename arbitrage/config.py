# -*- coding: utf-8 -*-
"""
全局配置（环境变量为主）
"""
from __future__ import annotations
import os

# —— 标的与腿类型 ——（并列）
PAIR = os.environ.get("PAIR", "BTC")
SPOT_SYMBOL   = os.environ.get("SPOT_SYMBOL",   f"{PAIR}USDT")
COINM_SYMBOL  = os.environ.get("COINM_SYMBOL",  f"{PAIR}USD_PERP")

# 腿类型：'spot' | 'coinm' | 'usdtm' | 'usdcm'
HEDGE_KIND = os.environ.get("HEDGE_KIND", "spot")
QUOTE_KIND = os.environ.get("QUOTE_KIND", "coinm")

HEDGE_SYMBOL = os.environ.get(
    "HEDGE_SYMBOL",
    SPOT_SYMBOL if HEDGE_KIND == "spot"
    else (f"{PAIR}USDC" if HEDGE_KIND == "usdcm" else f"{PAIR}USDT" if HEDGE_KIND == "usdtm" else COINM_SYMBOL)
)
QUOTE_SYMBOL = os.environ.get(
    "QUOTE_SYMBOL",
    SPOT_SYMBOL if QUOTE_KIND == "spot"
    else (f"{PAIR}USDC" if QUOTE_KIND == "usdcm" else f"{PAIR}USDT" if QUOTE_KIND == "usdtm" else COINM_SYMBOL)
)
