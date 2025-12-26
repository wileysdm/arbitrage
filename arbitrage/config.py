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

# —— 公共行情端点（REST/WS；不需要签名）——
# 说明：统一账户仅影响“私有域”（下单/资金/UDS）；公共行情仍按现货/合约各自域名获取。
SPOT_BASE = os.environ.get("SPOT_BASE", "https://api.binance.com")
FAPI_BASE = os.environ.get("FAPI_BASE", "https://fapi.binance.com")
DAPI_BASE = os.environ.get("DAPI_BASE", "https://dapi.binance.com")

WS_SPOT = os.environ.get("WS_SPOT", "wss://stream.binance.com:9443")
WS_UM = os.environ.get("WS_UM", "wss://fstream.binance.com")
WS_CM = os.environ.get("WS_CM", "wss://dstream.binance.com")

# —— 统一账户 / Portfolio Margin (PAPI) ——
# 文档：Base https://papi.binance.com；UDS WS：wss://fstream.binance.com/pm
PAPI_BASE = os.environ.get("PAPI_BASE", "https://papi.binance.com")
WS_PM = os.environ.get("WS_PM", "wss://fstream.binance.com/pm")

# —— API Keys ——
PAPI_KEY    = os.environ.get("PAPI_KEY"   , "")
PAPI_SECRET = os.environ.get("PAPI_SECRET", "")

# —— 账户/资金日志展示 ——
ACCOUNT_LOG_INTERVAL_SEC = float(os.environ.get("ACCOUNT_LOG_INTERVAL_SEC", "60"))

# —— 标的与腿类型 ——（并列）
PAIR = os.environ.get("PAIR", "BTC")
SPOT_SYMBOL   = os.environ.get("SPOT_SYMBOL",   f"{PAIR}USDT")
COINM_SYMBOL  = os.environ.get("COINM_SYMBOL",  f"{PAIR}USD_PERP")

# 腿类型：'spot' | 'coinm' | 'usdtm' | 'usdcm'
# 统一账户下 spot 腿走 PAPI Cross Margin：/papi/v1/margin/order
HEDGE_KIND = os.environ.get("HEDGE_KIND", "coinm").lower()
QUOTE_KIND = os.environ.get("QUOTE_KIND", "usdtm").lower()

# spot/margin 下单 sideEffectType：
# - NO_SIDE_EFFECT: 只用现有余额
# - MARGIN_BUY / AUTO_REPAY / AUTO_BORROW_REPAY 等：允许自动借款/借还（按需开启）
MARGIN_SIDE_EFFECT_TYPE = os.environ.get("MARGIN_SIDE_EFFECT_TYPE", "").strip()

HEDGE_SYMBOL = os.environ.get(
    "HEDGE_SYMBOL",
    # 币本位：用 COINM_SYMBOL
    COINM_SYMBOL if HEDGE_KIND == "coinm"
    # USDC 本位：PAIRUSDC
    else f"{PAIR}USDC" if HEDGE_KIND in "usdcm"
    # 其它线性合约默认按 USDT 本位处理：PAIRUSDT
    else f"{PAIR}USDT"
)

QUOTE_SYMBOL = os.environ.get(
    "QUOTE_SYMBOL",
    # 币本位：用 COINM_SYMBOL
    COINM_SYMBOL if QUOTE_KIND == "coinm"
    # USDC 本位报价腿：PAIRUSDC
    else f"{PAIR}USDC" if QUOTE_KIND in "usdcm"
    # 其余：PAIRUSDT
    else f"{PAIR}USDT"
)

# —— 策略参数 ——
ONLY_POSITIVE_CARRY      = os.environ.get("ONLY_POSITIVE_CARRY", "False").lower() in ("1","true","yes","on")
ENTER_BPS                = float(os.environ.get("ENTER_BPS", "2.0"))    # 触发开仓阈值（bps）
EXIT_BPS                 = float(os.environ.get("EXIT_BPS",  "0.5"))    # 盈利退出 bps
STOP_BPS                 = float(os.environ.get("STOP_BPS",  "100.0"))   # 止损 bps
MAX_HOLD_SEC             = int(os.environ.get("MAX_HOLD_SEC", "1800"))  # 强制离场秒数（默认30min）

V_USD                    = float(os.environ.get("V_USD", "1000"))       # 单次名义本金（USD）

MAX_SLIPPAGE_BPS_SPOT    = float(os.environ.get("MAX_SLIPPAGE_BPS_SPOT", "5.0"))
MAX_SLIPPAGE_BPS_COINM   = float(os.environ.get("MAX_SLIPPAGE_BPS_COINM","5.0"))

EXECUTION_MODE           = os.environ.get("EXECUTION_MODE", "taker")    # taker/maker

# Hybrid 相关参数
HYBRID_MAKER_LEG      = os.environ.get("HYBRID_MAKER_LEG", "quote").lower()  # "quote" 或 "hedge"
HYBRID_WAIT_SEC       = float(os.environ.get("HYBRID_WAIT_SEC", "0.5"))      # 等待 maker 成交通知窗口
HYBRID_MIN_FILL_RATIO = float(os.environ.get("HYBRID_MIN_FILL_RATIO", "0.2"))# 部分成交比例阈值
PAIR_POLL_INTERVAL    = float(os.environ.get("PAIR_POLL_INTERVAL", "0.15"))  # 轮询间隔
DRY_RUN               = os.environ.get("DRY_RUN", "0").lower() in ("1","true","yes","on")

#os.environ['http_proxy'] = 'http://localhost:7890'
#os.environ['https_proxy'] = 'https://localhost:7890'