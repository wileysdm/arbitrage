#和frontier.py类似，功能疑似重复可以尝试删除
# -*- coding: utf-8 -*-
# arbitrage/data/features.py
"""
派生特征/指标（供策略或监控使用）。
"""
from __future__ import annotations
from typing import List, Tuple, Optional
from arbitrage.utils import vwap_to_qty


def mid_from_orderbook(bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> Optional[float]:
    if not bids or not asks: 
        return None
    return (bids[0][0] + asks[0][0]) / 2.0

def spread_bps(quote_ref: float, hedge_ref: float) -> float:
    return (quote_ref - hedge_ref) / max(1e-12, hedge_ref) * 1e4

def depth_imbalance(bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], n: int = 5) -> float:
    """前 n 档数量失衡（买方占比 0~1）"""
    tb = sum(q for _, q in bids[:n])
    ta = sum(q for _, q in asks[:n])
    if tb + ta <= 0:
        return 0.5
    return tb / (tb + ta)

def thickness_to_move(levels: List[Tuple[float, float]], side: str, pct_move: float) -> float:
    """
    估算将价格冲击 pct_move% 所需的 base 数量（单边）。
    side='ask' 表示向上扫，'bid' 表示向下扫。
    """
    if not levels:
        return 0.0
    start = levels[0][0]
    target = start * (1 + pct_move/100.0) if side == "ask" else start * (1 - pct_move/100.0)
    qty = 0.0
    if side == "ask":
        for px, q in levels:
            if px <= target:
                qty += q
            else:
                break
    else:
        for px, q in levels:
            if px >= target:
                qty += q
            else:
                break
    return qty

def slippage_bps(levels: List[Tuple[float, float]], qty: float) -> Optional[float]:
    filled, vwap = vwap_to_qty(levels, qty)
    if filled <= 0 or vwap is None:
        return None
    best = levels[0][0]
    return abs(vwap - best) / best * 1e4
