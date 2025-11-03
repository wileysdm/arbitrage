import time
import requests
import argparse

from arbitrage.config import (
    RUN_SECONDS, POLL_INTERVAL, MAX_BOOK_SKEW_MS,
    SPOT_SYMBOL, COINM_SYMBOL,
    AUTO_FROM_FRONTIER, PRINT_LEVELS, LEVELS_TO_PRINT
)
from arbitrage.exchanges.rules import fetch_spot_rules, fetch_coinm_rules
from arbitrage.strategy.logic import (
    try_enter_from_frontier, try_enter, need_exit, do_exit,
    print_levels_if_needed
)
from arbitrage.feeds import Feed  # 用 Feed 封装 WS/REST 获取簿与价差

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spot",  type=str, default=SPOT_SYMBOL, help="现货交易对，如 BTCUSDT / ETHUSDT")
    parser.add_argument("--coinm", type=str, default=COINM_SYMBOL, help="币本位永续，如 BTCUSD_PERP / ETHUSD_PERP")
    args = parser.parse_args()

    spot_symbol  = args.spot
    coinm_symbol = args.coinm

    spot_tick, spot_step = fetch_spot_rules(spot_symbol)
    contract_size, cm_tick, cm_step = fetch_coinm_rules(coinm_symbol)

    position = None
    t0 = time.time()
    placed_any = False

    feed = Feed(spot_symbol, coinm_symbol)   # ← 新：一行搞定“REST快照+WS维护”
    feed.start()

    while time.time() - t0 < RUN_SECONDS:
        try:
            ok, spread_bps, spot_mid, perp_mark, spot_bids, spot_asks, cm_bids, cm_asks, skew_ms = feed.get_snapshot()
            if not ok:
                time.sleep(POLL_INTERVAL); continue
            if skew_ms > MAX_BOOK_SKEW_MS:
                # 本地两本簿时间差较大时，略过这次判定
                time.sleep(POLL_INTERVAL); continue

            pos_side = None if position is None else position.side
            print(f"spread={spread_bps:.2f}bp | spot_mid={spot_mid:.2f} mark={perp_mark:.2f} | pos={pos_side}")

            # 可选打印“逐档评估”
            print_levels_if_needed(
                PRINT_LEVELS, position, AUTO_FROM_FRONTIER,
                spot_bids, spot_asks, cm_bids, cm_asks, contract_size, LEVELS_TO_PRINT
            )

            if position is None:
                if AUTO_FROM_FRONTIER:
                    done, position = try_enter_from_frontier(
                        spot_bids, spot_asks, cm_bids, cm_asks,
                        contract_size, spot_step, cm_step,
                        spot_symbol, coinm_symbol
                    )
                else:
                    done, position = try_enter(
                        spread_bps, spot_mid,
                        spot_bids, spot_asks, cm_bids, cm_asks,
                        contract_size, spot_step, cm_step,
                        spot_symbol, coinm_symbol
                    )
                placed_any = placed_any or done
            else:
                should, reason = need_exit(spread_bps, position)
                if should:
                    print(f"平仓理由：{reason}")
                    do_exit(position)
                    position = None

            time.sleep(POLL_INTERVAL)

        except requests.HTTPError as e:
            print("HTTPError:", e.response.text); time.sleep(1.2)
        except Exception as e:
            print("Error:", e); time.sleep(1.0)

    feed.stop()
    print("== 结束 ==")
    print("是否曾下过单：", placed_any)

if __name__ == "__main__":
    main()
