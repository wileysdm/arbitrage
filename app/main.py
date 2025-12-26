# -*- coding: utf-8 -*-
"""
最小主程序：启动数据层 + 用户流，循环评估入场/出场
- 环境变量配置见 config.py
"""

from __future__ import annotations
import asyncio
import os
import signal
import logging
from logging.handlers import RotatingFileHandler
import json
from typing import Optional

from arbitrage.config import (
    HEDGE_KIND, QUOTE_KIND, HEDGE_SYMBOL, QUOTE_SYMBOL,
)

from arbitrage.data.service import boot_and_start, DataService
from arbitrage.data.bus import Topic
from arbitrage.exchanges.user_stream import run_user_stream
from arbitrage.data.schemas import Position
from arbitrage.strategy.logic import try_enter_unified, try_exit_unified
from arbitrage.strategy.pending import GlobalPendingManager

# --- 全局日志配置 ---
log_formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_file = f'{HEDGE_SYMBOL}_{QUOTE_SYMBOL}.log'

# 文件日志处理器，带轮换
file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

# 控制台日志处理器
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.INFO)

# 获取根 logger 并添加处理器
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# 清除所有现有的处理器，以防万一
if root_logger.hasHandlers():
    root_logger.handlers.clear()
root_logger.addHandler(file_handler)
root_logger.addHandler(stream_handler)
# --- 日志配置结束 ---

class Runner:
    STATE_FILE = "runner_state.json"

    def __init__(self):
        self.pos: Optional[Position] = None
        self._stopping = False
        self._load_state()

    async def _listen_for_fills(self):
        """订阅并处理来自 user_stream 的成交事件"""
        bus = DataService.global_service().bus
        logging.info("subscribing to execution fills")
        # Wildcard subscription to get all fills
        async for key, fill_data in bus.subscribe(Topic.EXEC_FILLS):
            await GlobalPendingManager.on_fill(fill_data)
            logging.debug("received fill for market %s: %s", key, fill_data)

    def _load_state(self):
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r') as f:
                    state = json.load(f)
                    pos_data = state.get('pos')
                    if pos_data:
                        self.pos = Position.from_dict(pos_data)
                        logging.info("loaded position from state file: %s", self.pos)
            except (json.JSONDecodeError, IOError, TypeError) as e:
                logging.error("failed to load state from %s: %s", self.STATE_FILE, e)
                self.pos = None

    def _save_state(self):
        try:
            with open(self.STATE_FILE, 'w') as f:
                # 如果有持仓，调用 to_dict() 方法转换为字典
                pos_to_save = self.pos.to_dict() if self.pos else None
                json.dump({'pos': pos_to_save}, f, indent=4)
        except (IOError, TypeError) as e:
            logging.error("failed to save state to %s: %s", self.STATE_FILE, e)

    async def start(self):
        regs = []
        regs.append((HEDGE_KIND, HEDGE_SYMBOL))
        if (QUOTE_KIND, QUOTE_SYMBOL) not in regs:
            regs.append((QUOTE_KIND, QUOTE_SYMBOL))

        logging.info("environment parms: MAX_SLIPPAGE_BPS_SPOT: %s",
                     os.environ.get("MAX_SLIPPAGE_BPS_SPOT", "unset"))

        # 启动数据层（WS；如需持久化把 persist=True）
        logging.info("booting data service with regs=%s", regs)
        await boot_and_start(regs, use_ws=True, persist=False)

        # 启动用户数据流（统一账户：PAPI UDS）
        asyncio.create_task(run_user_stream())

        # 启动成交事件监听
        asyncio.create_task(self._listen_for_fills())

        logging.info("starting main loop")
        # 主循环
        await self.loop()

    async def loop(self):
        while not self._stopping:
            try:
                if self.pos is None:
                    ok, pos = await try_enter_unified()
                    if ok:
                        self.pos = pos
                        self._save_state()
                        logging.info("entered position: %s", pos)
                else:
                    trig, reason = try_exit_unified(self.pos)
                    if trig:
                        logging.info("[EXIT] %s", reason)
                        self.pos = None
                        self._save_state()
            except Exception as e:
                # log full stack
                logging.exception("[MAIN] error during main loop")
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
        logging.info("runner starting")
        await r.start()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received, stopping runner")
        r.stop()

if __name__ == "__main__":
    asyncio.run(main())
