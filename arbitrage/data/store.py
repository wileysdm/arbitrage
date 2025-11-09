# -*- coding: utf-8 -*-
# arbitrage/data/store.py
"""
可选持久化：将 OrderBook / MarkPrice / FundingRate 写入 Parquet。
默认关闭；当你在 service.py 里传入 Store(enable=True) 即可生效。
- 简单分区：base_dir/topic/symbol/date=YYYY-MM-DD/part-*.parquet
- 依赖 pandas；不依赖 duckdb（若需要聚合查询，可另行添加 duckdb 视图）。
"""
from __future__ import annotations
import os, time
from pathlib import Path
from typing import Iterable, Any, Dict

import pandas as pd
from dataclasses import asdict, is_dataclass
from datetime import datetime

class Store:
    def __init__(self, base_dir: str | Path = "data/parquet", enable: bool = False):
        self.base = Path(base_dir)
        self.enable = enable
        if self.enable:
            self.base.mkdir(parents=True, exist_ok=True)

    def _path(self, topic: str, symbol: str, ts: float) -> Path:
        date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        d = self.base / topic / symbol / f"date={date}"
        d.mkdir(parents=True, exist_ok=True)
        fname = f"part-{int(ts*1000)}.parquet"
        return d / fname

    def _to_frame(self, obj: Any) -> pd.DataFrame:
        if is_dataclass(obj):
            d: Dict = asdict(obj)
        elif isinstance(obj, dict):
            d = obj
        else:
            d = {"value": obj}
        # 展开 bids/asks（列表）时采用 json 形式，避免复杂 schema
        for k in ("bids", "asks"):
            if k in d and isinstance(d[k], list):
                d[k] = str(d[k])
        return pd.DataFrame([d])

    def append(self, topic: str, symbol: str, obj: Any):
        if not self.enable: 
            return
        ts = getattr(obj, "ts", time.time())
        path = self._path(topic, symbol, ts)
        df = self._to_frame(obj)
        df.to_parquet(path, index=False)

    # 便捷方法
    def save_orderbook(self, ob): self.append("orderbook", ob.symbol, ob)
    def save_mark(self, mp):      self.append("mark", mp.symbol, mp)
    def save_funding(self, fr):   self.append("funding", fr.symbol, fr)
