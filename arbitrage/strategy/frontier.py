# -*- coding: utf-8 -*-
"""
Frontier（候选生成 & 逐档撮合）——轻量实现（仅 coinm↔spot 使用）
- collect_frontier_candidates(...)：从订单簿组合多档，产出可成交候选
- print_per_level_book_edge(...)：打印前 N 档的价差参考
- place_entry_from_row(row, ...)：根据候选下单（maker/taker 可选）
"""
from __future__ import annotations
from typing import List, Tuple, Dict

from arbitrage.exchanges.exec_binance_rest import (
    place_spot_limit_maker, place_coinm_limit,
    place_spot_market, place_coinm_market
)

OrderBookSide = List[Tuple[float, float]]

def _edge_bps(quote_px: float, hedge_px: float) -> float:
    return (quote_px - hedge_px) / max(1e-12, hedge_px) * 1e4

def collect_frontier_candidates(spot_bids: OrderBookSide, spot_asks: OrderBookSide,
                                cm_bids: OrderBookSide, cm_asks: OrderBookSide,
                                contract_size_usd: float, max_levels: int = 20,
                                min_bp: float = 1.0, min_vusd: float = 1000.0,
                                only_positive_carry: bool = True):
    """
    返回 (fwd_list, rev_list)，元素为 dict：
      - side: "POS"（买现货/卖合约）或 "NEG"
      - px_spot / px_cm：各自参考价
      - Q / N：建议数量（现货base / 合约张）
      - edge_bps：名义价差 bps
    """
    fwd, rev = [], []
    # 正向：买现货(ask) / 卖合约(bid)
    for i in range(min(max_levels, len(spot_asks), len(cm_bids))):
        px_spot = spot_asks[i][0]
        px_cm   = cm_bids[i][0]
        edge = _edge_bps(px_cm, px_spot)
        if edge >= min_bp:
            Q = min(V_USD/px_spot, spot_asks[i][1])      # 受限于该档数量
            N = int(min(V_USD/contract_size_usd, cm_bids[i][1]))  # 张数
            if Q*px_spot >= min_vusd and N >= 1:
                fwd.append(dict(side="POS", px_spot=px_spot, px_cm=px_cm, Q=Q, N=N, edge_bps=edge))
    # 反向：卖现货(bid) / 买合约(ask)
    if not only_positive_carry:
        for i in range(min(max_levels, len(spot_bids), len(cm_asks))):
            px_spot = spot_bids[i][0]
            px_cm   = cm_asks[i][0]
            edge = _edge_bps(px_cm, px_spot)  # NEG 目标：edge <= -min_bp
            if edge <= -min_bp:
                Q = min(V_USD/px_spot, spot_bids[i][1])
                N = int(min(V_USD/contract_size_usd, cm_asks[i][1]))
                if Q*px_spot >= min_vusd and N >= 1:
                    rev.append(dict(side="NEG", px_spot=px_spot, px_cm=px_cm, Q=Q, N=N, edge_bps=edge))
    return fwd, rev

def print_per_level_book_edge(spot_bids, spot_asks, cm_bids, cm_asks,
                              contract_size_usd: float, max_levels: int = 10):
    print("=== Frontier Edge Preview ===")
    for i in range(min(max_levels, len(spot_asks), len(cm_bids))):
        px_spot = spot_asks[i][0]; px_cm = cm_bids[i][0]
        edge = _edge_bps(px_cm, px_spot)
        print(f"POS L{i+1}: cm_bid={px_cm:.2f} vs spot_ask={px_spot:.2f} | edge={edge:.2f}bp")
    for i in range(min(max_levels, len(spot_bids), len(cm_asks))):
        px_spot = spot_bids[i][0]; px_cm = cm_asks[i][0]
        edge = _edge_bps(px_cm, px_spot)
        print(f"NEG L{i+1}: cm_ask={px_cm:.2f} vs spot_bid={px_spot:.2f} | edge={edge:.2f}bp")

def place_entry_from_row(row: Dict, spot_symbol: str, cm_symbol: str, execution_mode: str = "taker"):
    side = row["side"]
    if side == "POS":
        if execution_mode == "maker":
            o1 = place_spot_limit_maker("BUY",  row["Q"], row["px_spot"], symbol=spot_symbol)
            o2 = place_coinm_limit("SELL", row["N"], row["px_cm"],  post_only=True, symbol=cm_symbol)
        else:
            o1 = place_spot_market("BUY",  row["Q"], symbol=spot_symbol)
            o2 = place_coinm_market("SELL", row["N"], reduce_only=False, symbol=cm_symbol)
    else:
        if execution_mode == "maker":
            o1 = place_spot_limit_maker("SELL", row["Q"], row["px_spot"], symbol=spot_symbol)
            o2 = place_coinm_limit("BUY",  row["N"], row["px_cm"],  post_only=True, symbol=cm_symbol)
        else:
            o1 = place_spot_market("SELL", row["Q"], symbol=spot_symbol)
            o2 = place_coinm_market("BUY",  row["N"], reduce_only=False, symbol=cm_symbol)
    return True, None  # 简化；如需返回 Position 可在此构造
