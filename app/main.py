# -*- coding: utf-8 -*-
"""
实盘主进程（与当前 logic.py 函数签名完全对齐）
- 行情：Feed(spot+coin-m) 启动、簿一致性校验、价差计算
- 策略：try_enter_from_frontier / try_enter / need_exit / do_exit
- 用户流：预检通过才启动（避免 401 日志刷屏）；DRY_RUN=True 时自动跳过
"""
from __future__ import annotations
import argparse
import time
import requests

from arbitrage.config import (
    DRY_RUN,
    RUN_SECONDS, POLL_INTERVAL, MAX_BOOK_SKEW_MS,
    SPOT_SYMBOL, COINM_SYMBOL,
    AUTO_FROM_FRONTIER, PRINT_LEVELS, LEVELS_TO_PRINT,
)
from arbitrage.feeds import Feed
from arbitrage.strategy.logic import (
    try_enter_from_frontier, try_enter, need_exit, do_exit, print_levels_if_needed
)
from arbitrage.models import Position
from arbitrage.exchanges.user_stream import UserStreams
from arbitrage.exchanges.binance_rest import (
    spot_symbol_meta, dapi_symbol_meta,
    spot_post, dapi_post,   # 仅用于用户流预检
)


def parse_args():
    p = argparse.ArgumentParser(description="Arbitrage Runner")
    p.add_argument('--spot', default=SPOT_SYMBOL, help='现货交易对，如 BTCUSDT')
    p.add_argument('--coinm', default=COINM_SYMBOL, help='币本位永续交易对，如 BTCUSD_PERP')
    p.add_argument('--seconds', type=int, default=RUN_SECONDS, help='运行秒数（<=0 表示一直跑）')
    p.add_argument('--print-levels', action='store_true', default=PRINT_LEVELS, help='打印逐档评估')
    return p.parse_args()


def _can_start_user_streams() -> bool:
    """listenKey 预检；失败则跳过用户流，避免 401 刷屏。"""
    try:
        s = spot_post("/api/v3/userDataStream", {})
        c = dapi_post("/dapi/v1/listenKey", {})
        return bool(s.get("listenKey")) and bool(c.get("listenKey"))
    except requests.HTTPError as e:
        try:
            body = e.response.text
        except Exception:
            body = str(e)
        print("[WARN] 用户流预检失败（多半是 API 权限/IP 白名单问题）。已跳过启动用户流：", body)
        return False
    except Exception as e:
        print("[WARN] 用户流预检异常，已跳过启动用户流：", e)
        return False


def main():
    args = parse_args()
    spot_symbol = args.spot.upper()
    coinm_symbol = args.coinm.upper()
    run_seconds = int(args.seconds)

    # === 读取交易对元数据（tick/合约面值）===
    spot_meta = spot_symbol_meta(spot_symbol)
    cm_meta   = dapi_symbol_meta(coinm_symbol)
    spot_step = float(spot_meta["price_tick"] or 0.0)           # 价格 tickSize（现货）
    cm_step   = float(cm_meta["price_tick"]   or 0.0)           # 价格 tickSize（币本位）
    contract_size = float(cm_meta.get("contract_size") or 0.0)  # V（USD/张）
    if contract_size <= 0:
        raise RuntimeError(f"[exchangeInfo] 未拿到 {coinm_symbol} 的 contract_size（V）。")

    # === 用户数据流（listenKey 预检通过才启；DRY_RUN=True 直接跳过）===
    user_streams = UserStreams(
        spot_symbol=spot_symbol,
        coinm_symbol=coinm_symbol,
        callbacks={
            # 需要时可在此接 on_resync/on_exec 等回调并落库
        }
    )
    if not DRY_RUN and _can_start_user_streams():
        user_streams.start()
    else:
        print('[INFO] 跳过连接用户数据流（DRY_RUN 或预检失败）')

    # === 行情订阅 ===
    feed = Feed(spot_symbol=spot_symbol, coinm_symbol=coinm_symbol)
    feed.start()
    print(f"[BOOT] feed started for {spot_symbol} / {coinm_symbol}")

    # === 主循环 ===
    t0 = time.time()
    placed_any = False
    position: Position | None = None

    try:
        while True:
            if run_seconds > 0 and time.time() - t0 > run_seconds:
                print('[STOP] run_seconds reached')
                break

            ok, spread_bps, spot_mid, perp_mark, spot_bids, spot_asks, cm_bids, cm_asks, skew_ms = feed.get_snapshot()
            if not ok:
                time.sleep(POLL_INTERVAL)
                continue
            if skew_ms > MAX_BOOK_SKEW_MS:
                print(f"[SKIP] skew={skew_ms:.0f}ms > {MAX_BOOK_SKEW_MS}ms")
                time.sleep(POLL_INTERVAL)
                continue

            pos_side = None if position is None else position.side
            print(f"spread={spread_bps:.2f}bp | spot_mid={spot_mid:.2f} mark={perp_mark:.2f} | pos={pos_side}")

            # 你的 print_levels_if_needed 需要 contract_size
            print_levels_if_needed(
                PRINT_LEVELS=args.print_levels,
                position=position,
                AUTO_FROM_FRONTIER=AUTO_FROM_FRONTIER,
                spot_bids=spot_bids, spot_asks=spot_asks,
                cm_bids=cm_bids, cm_asks=cm_asks,
                contract_size=contract_size,
                LEVELS_TO_PRINT=LEVELS_TO_PRINT
            )

            if position is None:
                if AUTO_FROM_FRONTIER:
                    # try_enter_from_frontier(spot_bids, spot_asks, cm_bids, cm_asks, contract_size, spot_step, cm_step, spot_symbol, coinm_symbol)
                    done, position = try_enter_from_frontier(
                        spot_bids, spot_asks, cm_bids, cm_asks,
                        contract_size, spot_step, cm_step,
                        spot_symbol=spot_symbol, coinm_symbol=coinm_symbol
                    )
                else:
                    # try_enter(spread_bps, spot_mid, spot_bids, spot_asks, cm_bids, cm_asks, contract_size, spot_step, cm_step, spot_symbol, coinm_symbol)
                    done, position = try_enter(
                        spread_bps, spot_mid,
                        spot_bids, spot_asks, cm_bids, cm_asks,
                        contract_size, spot_step, cm_step,
                        spot_symbol=spot_symbol, coinm_symbol=coinm_symbol
                    )
                placed_any = placed_any or done
            else:
                # need_exit(spread_bps, position) -> (bool, reason)
                need, why = need_exit(spread_bps, position)
                if need:
                    print(f"[EXIT] reason={why}")
                    do_exit(position)
                    position = None

            time.sleep(POLL_INTERVAL)

    except requests.HTTPError as e:
        print("HTTPError:", getattr(e.response, 'text', str(e)))
        time.sleep(1.0)
    except KeyboardInterrupt:
        print('^C received, exiting ...')
    except Exception as e:
        print('Error:', e)
    finally:
        try:
            feed.stop()
        except Exception:
            pass
        try:
            if not DRY_RUN:
                user_streams.stop()
        except Exception:
            pass
        print('== 结束 ==')
        print('是否曾下过单：', placed_any)


if __name__ == '__main__':
    main()
