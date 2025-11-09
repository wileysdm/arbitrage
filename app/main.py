# -*- coding: utf-8 -*-
"""
最小主程序：启动数据层 + 用户流，循环评估入场/出场
- 环境变量配置见 config.py
"""
from __future__ import annotations
import asyncio
import signal

from arbitrage.config import (
    HEDGE_KIND, QUOTE_KIND, HEDGE_SYMBOL, QUOTE_SYMBOL,
)
from arbitrage.data.service import boot_and_start, DataService
from arbitrage.exchanges.user_stream import run_all_user_streams
from arbitrage.strategy.logic import try_enter_unified, try_exit_unified

class Runner:
    def __init__(self):
        self.pos = None
        self._stopping = False

    async def start(self):
        regs = []
        regs.append((HEDGE_KIND, HEDGE_SYMBOL))
        if (QUOTE_KIND, QUOTE_SYMBOL) not in regs:
            regs.append((QUOTE_KIND, QUOTE_SYMBOL))

        # 启动数据层（WS；如需持久化把 persist=True）
        await boot_and_start(regs, use_ws=True, persist=False)

        # 启动用户数据流（按腿启用）
        enable_spot = (HEDGE_KIND=="spot" or QUOTE_KIND=="spot")
        enable_um   = (HEDGE_KIND in ("usdtm","usdcm")) or (QUOTE_KIND in ("usdtm","usdcm"))
        enable_cm   = (HEDGE_KIND=="coinm" or QUOTE_KIND=="coinm")
        asyncio.create_task(run_all_user_streams(enable_spot=enable_spot, enable_um=enable_um, enable_cm=enable_cm))

        # 主循环
        await self.loop()

    async def loop(self):
        while not self._stopping:
            try:
                if self.pos is None:
                    ok, pos = try_enter_unified()
                    if ok:
                        self.pos = pos
                else:
                    trig, reason = try_exit_unified(self.pos)
                    if trig:
                        print(f"[EXIT] {reason}")
                        self.pos = None
            except Exception as e:
                print("[MAIN] error:", e)
            await asyncio.sleep(0.25)  # 250ms 周期

    def stop(self):
        self._stopping = True

async def main():
    r = Runner()
    loop = asyncio.get_event_loop()
    try:
        # 在类 Unix 系统可用；Windows 大多不支持
        for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
            if sig is not None:
                loop.add_signal_handler(sig, r.stop)
    except (NotImplementedError, RuntimeError):
        # Windows 等环境下不支持就忽略，靠 Ctrl+C 触发 KeyboardInterrupt 退出
        pass

    try:
        await r.start()
    except KeyboardInterrupt:
        # Windows 下按 Ctrl+C 仍能退出；顺手标记 stop
        r.stop()

if __name__ == "__main__":
    asyncio.run(main())
