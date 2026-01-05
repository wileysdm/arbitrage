# -*- coding: utf-8 -*-
"""Sanity check: market key must include kind.

Run:
  C:/Users/lenovo/arbitrage/.venv/Scripts/python.exe scripts/sanity_market_key.py

Expected:
  - spot and usdtm with same symbol do NOT overwrite each other
  - bus.latest reads different snapshots for different kind keys
"""

from __future__ import annotations

import os
import sys
import time

# allow running as a script from the repo root or from scripts/
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from arbitrage.data.bus import Bus, Topic
from arbitrage.data.schemas import OrderBook, MarkPrice


def main() -> None:
    bus = Bus()
    sym = "BTCUSDT"

    bus.publish(
        Topic.ORDERBOOK,
        f"spot:{sym}",
        OrderBook(symbol=sym, ts=time.time(), bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]),
    )
    bus.publish(
        Topic.ORDERBOOK,
        f"usdtm:{sym}",
        OrderBook(symbol=sym, ts=time.time(), bids=[(200.0, 1.0)], asks=[(201.0, 1.0)]),
    )

    bus.publish(Topic.MARK, f"spot:{sym}", MarkPrice(symbol=sym, ts=time.time(), mark=100.5))
    bus.publish(Topic.MARK, f"usdtm:{sym}", MarkPrice(symbol=sym, ts=time.time(), mark=200.5))

    ob_spot = bus.latest(Topic.ORDERBOOK, f"spot:{sym}")
    ob_um = bus.latest(Topic.ORDERBOOK, f"usdtm:{sym}")
    mk_spot = bus.latest(Topic.MARK, f"spot:{sym}")
    mk_um = bus.latest(Topic.MARK, f"usdtm:{sym}")

    assert ob_spot and ob_um and mk_spot and mk_um
    assert ob_spot.bids[0][0] == 100.0 and ob_um.bids[0][0] == 200.0
    assert mk_spot.mark == 100.5 and mk_um.mark == 200.5

    print("OK: market key separates kinds")


if __name__ == "__main__":
    main()
