# -*- coding: utf-8 -*-
"""PAPI 资金/账户信息查询与日志摘要。

说明：
- 统一账户下，账户/资金/交易等私有接口应走 PAPI（https://papi.binance.com）。
- 公共行情（kline、trades、orderbook、mark 等）仍走各自现货/合约公开域名，本模块不涉及。

本模块尽量“稳健”：不同账户/权限下可用的 PAPI endpoint 可能不同，
因此默认会尝试少量常见 endpoint，并允许通过环境变量覆盖/补充。
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, List, Optional

from arbitrage.exchanges.binance_rest import r_signed
from arbitrage.config import PAPI_BASE, PAPI_KEY, PAPI_SECRET


def _split_csv(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def is_papi_configured() -> bool:
    return bool(PAPI_KEY and PAPI_SECRET)


def _try_signed_get(path: str) -> Optional[Any]:
    try:
        return r_signed(PAPI_BASE, path, "GET", {}, PAPI_KEY, PAPI_SECRET)
    except Exception as e:
        # 不让账户日志影响主策略：这里吞掉异常，只打 warning
        logging.warning("[ACCT] PAPI GET %s failed: %s", path, e)
        return None


def fetch_papi_account_snapshot() -> Dict[str, Any]:
    """抓取一份账户快照。

    返回结构：
    {
      "balance": <json|None>,
      "account": <json|None>,
      "extra": { path: json|None }
    }
    """
    if not is_papi_configured():
        return {"balance": None, "account": None, "extra": {}}

    # 可通过环境变量自定义要查询的 endpoint（逗号分隔）
    # 例：PAPI_ACCOUNT_ENDPOINTS=/papi/v1/balance,/papi/v1/account
    endpoints = _split_csv(os.environ.get("PAPI_ACCOUNT_ENDPOINTS", ""))
    if not endpoints:
        endpoints = [
            "/papi/v1/balance",
            "/papi/v1/account",
        ]

    extra: Dict[str, Any] = {}

    balance = None
    account = None

    for path in endpoints:
        data = _try_signed_get(path)
        extra[path] = data
        if path.endswith("/balance") and balance is None:
            balance = data
        if path.endswith("/account") and account is None:
            account = data

    return {"balance": balance, "account": account, "extra": extra}


def _pick(d: Dict[str, Any], keys: Iterable[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in keys:
        if k in d:
            out[k] = d[k]
    return out


def _asset_balance_map(balance_payload: Any) -> Dict[str, Dict[str, Any]]:
    """把 /balance 的列表形式归一为 asset -> dict。"""
    if not isinstance(balance_payload, list):
        return {}
    m: Dict[str, Dict[str, Any]] = {}
    for it in balance_payload:
        if not isinstance(it, dict):
            continue
        asset = str(it.get("asset") or it.get("a") or "").upper()
        if not asset:
            continue
        m[asset] = it
    return m


def format_papi_account_snapshot(snapshot: Dict[str, Any]) -> str:
    """把快照格式化成一行日志文本（尽量短、可读）。"""
    if not is_papi_configured():
        return "PAPI_KEY/PAPI_SECRET not set"

    parts: List[str] = []

    # 1) account 概览（尽量挑通用字段；不存在就跳过）
    acct = snapshot.get("account")
    if isinstance(acct, dict):
        fields = _pick(
            acct,
            [
                "totalWalletBalance",
                "totalMarginBalance",
                "availableBalance",
                "totalUnrealizedProfit",
                "totalInitialMargin",
                "totalMaintMargin",
            ],
        )
        if fields:
            parts.append("acct=" + str(fields))

    # 2) balance 资产摘要：默认展示常用资产；可通过 env 覆盖
    balance = snapshot.get("balance")
    bm = _asset_balance_map(balance)
    if bm:
        assets = _split_csv(os.environ.get("ACCOUNT_LOG_ASSETS", "USDT,USDC,BTC,ETH"))
        picked: Dict[str, Any] = {}
        for a in assets:
            it = bm.get(a.upper())
            if not it:
                continue
            # 常见字段：balance / crossWalletBalance / availableBalance / withdrawAvailable
            picked[a.upper()] = _pick(it, ["balance", "availableBalance", "crossWalletBalance", "withdrawAvailable"]) or it
        if picked:
            parts.append("bal=" + str(picked))
        else:
            parts.append(f"bal_assets={len(bm)}")

    if not parts:
        # 至少告诉你 endpoint 有无返回
        extra = snapshot.get("extra")
        if isinstance(extra, dict):
            ok = [k for k, v in extra.items() if v is not None]
            parts.append(f"endpoints_ok={len(ok)}/{len(extra)}")
        else:
            parts.append("no-data")

    return " ".join(parts)
