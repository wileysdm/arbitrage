# -*- coding: utf-8 -*-
"""
把“REST 快照 + WS 增量维护本地簿 + 价差计算”封装成一个简单门面 Feed。
main 只需要：feed = Feed(...); feed.start(); feed.get_snapshot()
"""
from __future__ import annotations
import time
from typing import Tuple, List
from arbitrage.config import (
    USE_WS_ORDERBOOK, WS_SPOT_BASE, WS_DAPI_BASE, WS_DEPTH_LIMIT
)

if USE_WS_ORDERBOOK:
    from arbitrage.exchanges.orderbook_ws import WSOrderBooks
else:
    from arbitrage.exchanges.md_binance_rest import (
        get_spot_depth_with_ts, get_coinm_depth_with_ts, get_coinm_mark
    )

class Feed:
    def __init__(self, spot_symbol: str, coinm_symbol: str):
        self.spot_symbol = spot_symbol
        self.coinm_symbol = coinm_symbol
        self.ob = None

    def start(self):
        if USE_WS_ORDERBOOK:
            self.ob = WSOrderBooks(self.spot_symbol, self.coinm_symbol, WS_SPOT_BASE, WS_DAPI_BASE, depth_limit=WS_DEPTH_LIMIT)
            self.ob.start()
            # 等待 ready（最多几秒，不阻塞太久）
            t0 = time.time()
            while not self.ob.ready():
                if time.time() - t0 > 6: break
                time.sleep(0.05)

    def stop(self):
        if self.ob: self.ob.stop()

    def get_snapshot(self) -> Tuple[bool, float, float, float, List, List, List, List, float]:
        """
        返回：
          ok, spread_bps, spot_mid, perp_mark, spot_bids, spot_asks, cm_bids, cm_asks, skew_ms
        """
        if USE_WS_ORDERBOOK and self.ob and self.ob.ready():
            (sbids, sasks, ts_s, ok_s), (cbids, casks, ts_c, ok_c), (mark, ts_mk) = self.ob.get_books(limit=5)
            if not (ok_s and ok_c and sbids and sasks and cbids and casks):
                return False, 0.0, 0.0, 0.0, [], [], [], [], 0.0
            spot_mid = (sbids[0][0] + sasks[0][0]) / 2.0
            cm_mid   = (cbids[0][0] + casks[0][0]) / 2.0
            perp_mark = mark if mark is not None else cm_mid
            spread_bps = (perp_mark - spot_mid) / spot_mid * 10000.0
            skew_ms = abs(ts_c - ts_s) * 1000.0
            return True, spread_bps, spot_mid, perp_mark, sbids, sasks, cbids, casks, skew_ms

        # 回退：纯 REST
        from arbitrage.exchanges.md_binance_rest import (
            get_spot_depth_with_ts, get_coinm_depth_with_ts, get_coinm_mark
        )
        sbids, sasks, t_s = get_spot_depth_with_ts(limit=100, symbol=self.spot_symbol)
        cbids, casks, t_c = get_coinm_depth_with_ts(limit=100, symbol=self.coinm_symbol)
        if not (sbids and sasks and cbids and casks):
            return False, 0.0, 0.0, 0.0, [], [], [], [], 0.0
        spot_mid  = (sbids[0][0] + sasks[0][0]) / 2.0
        perp_mark = get_coinm_mark(self.coinm_symbol)
        spread_bps = (perp_mark - spot_mid) / spot_mid * 10000.0
        skew_ms = abs(t_c - t_s) * 1000.0
        return True, spread_bps, spot_mid, perp_mark, sbids, sasks, cbids, casks, skew_ms
