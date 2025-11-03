# -*- coding: utf-8 -*-
"""
Binance 用户数据流（User Data Stream）统一封装：Spot + COIN‑M Futures
- 自动创建 listenKey、定时续期（keepalive）、掉线重连（指数退避）
- Staleness 看门狗：长时间无事件自动重建 listenKey 并重连
- 重连后执行“查账回补”：拉取 openOrders / account / positionRisk / 最近成交 → 回调上层
- 事件回调：
    * on_spot_exec_report(evt)
    * on_spot_balance(evt)
    * on_spot_list_status(evt)
    * on_cm_order_trade_update(evt)
    * on_cm_account_update(evt)
    * on_cm_margin_call(evt)
    * on_resync(snapshot_dict)   # 重连后的对账快照

依赖：websocket-client、arbitrage.exchanges.binance_rest 中的 spot_get/dapi_get/spot_signed/dapi_signed
配置：arbitrage.config 中的 SPOT_SYMBOL、COINM_SYMBOL、WS_SPOT_BASE、WS_DAPI_BASE、DRY_RUN
"""
from __future__ import annotations
import json, threading, time, traceback
from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, Any

try:
    import websocket  # pip install websocket-client
except Exception as e:
    raise ImportError("缺少依赖 websocket-client，请先：pip install websocket-client") from e

from arbitrage.config import (
    DRY_RUN, SPOT_SYMBOL, COINM_SYMBOL,
    WS_SPOT_BASE, WS_DAPI_BASE,
)
# 顶部 imports 替换为（增加 spot_post/spot_put/dapi_post/dapi_put）：
from arbitrage.exchanges.binance_rest import (
    spot_signed, dapi_signed,
    spot_get, dapi_get,
    spot_post, spot_put, dapi_post, dapi_put,   # <== 新增
)


# ===== 可调参数（可视需要迁入 config） =====
SPOT_KEEPALIVE_SEC = 15 * 60     # Spot 官方建议 ≤30min；取 15min 更安全
DAPI_KEEPALIVE_SEC = 45 * 60     # Futures 官方建议 ≤60min；取 45min 更安全
STALE_USER_SEC     = 60          # 若 last_ts 超过该秒数无事件 → 触发重建
RECONNECT_INIT     = 1.0         # WS 重连退避初始秒
RECONNECT_MAX      = 30.0        # WS 重连退避上限

@dataclass
class _StreamState:
    listen_key: Optional[str] = None
    last_ts: float = 0.0
    connected: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)

class _WSRunner:
    """通用 WS 客户端（带退避重连 + open/close/error 回调）。"""
    def __init__(self, name: str, url_fn: Callable[[], str], on_message, on_evt: Optional[Callable[[str], None]] = None):
        self.name = name
        self.url_fn = url_fn
        self.on_message = on_message
        self.on_evt = on_evt
        self._running = False
        self._t: Optional[threading.Thread] = None
        self.ws: Optional[websocket.WebSocketApp] = None

    def _loop(self):
        backoff = RECONNECT_INIT
        while self._running:
            try:
                url = self.url_fn()
                def _on_message(ws, msg):
                    self.on_message(msg)
                def _on_open(ws):
                    if self.on_evt: self.on_evt("open")
                def _on_close(ws, *a):
                    if self.on_evt: self.on_evt("close")
                def _on_error(ws, err):
                    print(f"[UDS:{self.name}] error: {err}")
                    if self.on_evt: self.on_evt("error")

                self.ws = websocket.WebSocketApp(
                    url,
                    on_message=_on_message,
                    on_open=_on_open,
                    on_close=_on_close,
                    on_error=_on_error,
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
            except Exception as e:
                print(f"[UDS:{self.name}] run_forever exception: {e}")
                if self.on_evt: self.on_evt("error")
            finally:
                if not self._running:
                    break
                print(f"[UDS:{self.name}] reconnecting in {backoff:.1f}s ...")
                time.sleep(backoff)
                backoff = min(RECONNECT_MAX, backoff * 2)

    def start(self):
        if self._running: return
        self._running = True
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()

    def stop(self):
        self._running = False
        try:
            if self.ws: self.ws.close()
        except Exception:
            pass

class SpotUserStream:
    def __init__(self,
                 symbol: str = SPOT_SYMBOL,
                 on_exec_report: Optional[Callable[[Dict[str, Any]], None]] = None,
                 on_balance: Optional[Callable[[Dict[str, Any]], None]] = None,
                 on_list_status: Optional[Callable[[Dict[str, Any]], None]] = None,
                 on_resync: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.symbol = symbol
        self.on_exec_report = on_exec_report
        self.on_balance = on_balance
        self.on_list_status = on_list_status
        self.on_resync = on_resync
        self.state = _StreamState()
        self._keepalive_t: Optional[threading.Thread] = None
        self._watchdog_t: Optional[threading.Thread] = None
        self.ws_runner = _WSRunner("spot", self._ws_url, self._on_message, self._on_evt)

    # --- listenKey 管理 ---
    def _create_listenkey(self) -> str:
        # Spot: POST /api/v3/userDataStream （无需签名；签名请求也可被接受）
        d = spot_post("/api/v3/userDataStream", {}) 
        lk = d.get("listenKey")
        if not lk: raise RuntimeError("无法获取 Spot listenKey")
        return lk

    def _keepalive(self):
        while self.ws_runner and self._running:
            time.sleep(SPOT_KEEPALIVE_SEC)
            try:
                with self.state.lock:
                    lk = self.state.listen_key
                if lk:
                    spot_put("/api/v3/userDataStream", {"listenKey": lk})
            except Exception as e:
                print("[UDS:spot] keepalive error:", e)

    def _watchdog(self):
        while self.ws_runner and self._running:
            time.sleep(1.0)
            with self.state.lock:
                last = self.state.last_ts
                lk   = self.state.listen_key
            if last and (time.time() - last) > STALE_USER_SEC:
                print(f"[UDS:spot] stale {(time.time()-last):.1f}s → rotate listenKey")
                try:
                    self._rotate_listenkey()
                except Exception as e:
                    print("[UDS:spot] rotate error:", e)

    def _rotate_listenkey(self):
        with self.state.lock:
            self.state.listen_key = self._create_listenkey()
        # 触发 WS 重连（新 URL）
        self.ws_runner.stop(); time.sleep(0.2); self.ws_runner.start()
        # 重连后做查账回补
        self._reconcile_and_callback()

    # --- WS URL / 事件 ---
    def _ws_url(self) -> str:
        with self.state.lock:
            lk = self.state.listen_key
        if not lk:
            lk = self._create_listenkey()
            with self.state.lock:
                self.state.listen_key = lk
        return f"{WS_SPOT_BASE}/ws/{lk}"

    def _on_evt(self, evt: str):
        with self.state.lock:
            if evt == "open":
                self.state.connected = True
            elif evt in ("close", "error"):
                self.state.connected = False
        if evt == "open":
            self._reconcile_and_callback()

    def _on_message(self, msg: str):
        try:
            d = json.loads(msg)
            et = d.get("e") or d.get("eventType")
            with self.state.lock:
                self.state.last_ts = time.time()
            if et == "executionReport" and self.on_exec_report:
                self.on_exec_report(d)
            elif et == "outboundAccountPosition" and self.on_balance:
                self.on_balance(d)
            elif et == "balanceUpdate" and self.on_balance:
                self.on_balance(d)
            elif et == "listStatus" and self.on_list_status:
                self.on_list_status(d)
        except Exception:
            traceback.print_exc()

    def _reconcile_and_callback(self):
        if DRY_RUN: return
        try:
            oo = spot_signed("/api/v3/openOrders", "GET", {"symbol": self.symbol})
            acct = spot_signed("/api/v3/account", "GET", {})
            trades = spot_signed("/api/v3/myTrades", "GET", {"symbol": self.symbol, "limit": 500})
            snapshot = {"spot_openOrders": oo, "spot_account": acct, "spot_trades": trades}
            if self.on_resync:
                self.on_resync(snapshot)
        except Exception as e:
            print("[UDS:spot] reconcile error:", e)

    # --- lifecycle ---
    @property
    def _running(self) -> bool:
        return getattr(self, "__running", False)

    def start(self):
        if DRY_RUN:
            print("[UDS:spot] DRY_RUN=True，跳过连接")
            return
        if self._running: return
        self.__running = True
        self.ws_runner.start()
        self._keepalive_t = threading.Thread(target=self._keepalive, daemon=True)
        self._keepalive_t.start()
        self._watchdog_t = threading.Thread(target=self._watchdog, daemon=True)
        self._watchdog_t.start()

    def stop(self):
        self.__running = False
        try: self.ws_runner.stop()
        except Exception: pass

class CoinMUserStream:
    def __init__(self,
                 symbol: str = COINM_SYMBOL,
                 on_order_trade_update: Optional[Callable[[Dict[str, Any]], None]] = None,
                 on_account_update: Optional[Callable[[Dict[str, Any]], None]] = None,
                 on_margin_call: Optional[Callable[[Dict[str, Any]], None]] = None,
                 on_resync: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.symbol = symbol
        self.on_order_trade_update = on_order_trade_update
        self.on_account_update = on_account_update
        self.on_margin_call = on_margin_call
        self.on_resync = on_resync
        self.state = _StreamState()
        self._keepalive_t: Optional[threading.Thread] = None
        self._watchdog_t: Optional[threading.Thread] = None
        self.ws_runner = _WSRunner("coinm", self._ws_url, self._on_message, self._on_evt)

    # --- listenKey 管理 ---
    def _create_listenkey(self) -> str:
        d = dapi_post("/dapi/v1/listenKey", {})
        lk = d.get("listenKey")
        if not lk: raise RuntimeError("无法获取 COIN‑M listenKey")
        return lk

    def _keepalive(self):
        while self.ws_runner and self._running:
            time.sleep(DAPI_KEEPALIVE_SEC)
            try:
                with self.state.lock:
                    lk = self.state.listen_key
                if lk:
                    dapi_put("/dapi/v1/listenKey", {"listenKey": lk})  # <== 无签名 PUT
            except Exception as e:
                print("[UDS:coinm] keepalive error:", e)

    def _watchdog(self):
        while self.ws_runner and self._running:
            time.sleep(1.0)
            with self.state.lock:
                last = self.state.last_ts
            if last and (time.time() - last) > STALE_USER_SEC:
                print(f"[UDS:coinm] stale {(time.time()-last):.1f}s → rotate listenKey")
                try:
                    self._rotate_listenkey()
                except Exception as e:
                    print("[UDS:coinm] rotate error:", e)

    def _rotate_listenkey(self):
        with self.state.lock:
            self.state.listen_key = self._create_listenkey()
        self.ws_runner.stop(); time.sleep(0.2); self.ws_runner.start()
        self._reconcile_and_callback()

    # --- WS URL / 事件 ---
    def _ws_url(self) -> str:
        with self.state.lock:
            lk = self.state.listen_key
        if not lk:
            lk = self._create_listenkey()
            with self.state.lock:
                self.state.listen_key = lk
        return f"{WS_DAPI_BASE}/ws/{lk}"

    def _on_evt(self, evt: str):
        with self.state.lock:
            if evt == "open":
                self.state.connected = True
            elif evt in ("close", "error"):
                self.state.connected = False
        if evt == "open":
            self._reconcile_and_callback()

    def _on_message(self, msg: str):
        try:
            d = json.loads(msg)
            et = d.get("e")
            with self.state.lock:
                self.state.last_ts = time.time()
            if et == "ORDER_TRADE_UPDATE" and self.on_order_trade_update:
                self.on_order_trade_update(d)
            elif et == "ACCOUNT_UPDATE" and self.on_account_update:
                self.on_account_update(d)
            elif et == "MARGIN_CALL" and self.on_margin_call:
                self.on_margin_call(d)
        except Exception:
            traceback.print_exc()

    def _reconcile_and_callback(self):
        if DRY_RUN: return
        try:
            oo = dapi_signed("/dapi/v1/openOrders", "GET", {"symbol": self.symbol})
            pos = dapi_signed("/dapi/v1/positionRisk", "GET", {"symbol": self.symbol})
            acct = dapi_signed("/dapi/v1/account", "GET", {})
            # 近 24h 成交快照（不足也没关系）
            now = int(time.time() * 1000)
            start = now - 24*3600*1000
            trades = dapi_signed("/dapi/v1/userTrades", "GET", {"symbol": self.symbol, "startTime": start, "limit": 1000})
            snapshot = {
                "coinm_openOrders": oo,
                "coinm_positionRisk": pos,
                "coinm_account": acct,
                "coinm_trades_24h": trades,
            }
            if self.on_resync:
                self.on_resync(snapshot)
        except Exception as e:
            print("[UDS:coinm] reconcile error:", e)

    # --- lifecycle ---
    @property
    def _running(self) -> bool:
        return getattr(self, "__running", False)

    def start(self):
        if DRY_RUN:
            print("[UDS:coinm] DRY_RUN=True，跳过连接")
            return
        if self._running: return
        self.__running = True
        self.ws_runner.start()
        self._keepalive_t = threading.Thread(target=self._keepalive, daemon=True)
        self._keepalive_t.start()
        self._watchdog_t = threading.Thread(target=self._watchdog, daemon=True)
        self._watchdog_t.start()

    def stop(self):
        self.__running = False
        try: self.ws_runner.stop()
        except Exception: pass

class UserStreams:
    """聚合 Spot + COIN‑M；统一 start/stop；对账回补透传到上层。"""
    def __init__(self,
                 spot_symbol: str = SPOT_SYMBOL,
                 coinm_symbol: str = COINM_SYMBOL,
                 callbacks: Optional[Dict[str, Callable[[Dict[str, Any]], None]]] = None):
        callbacks = callbacks or {}
        self.spot = SpotUserStream(
            symbol=spot_symbol,
            on_exec_report=callbacks.get("on_spot_exec_report"),
            on_balance=callbacks.get("on_spot_balance"),
            on_list_status=callbacks.get("on_spot_list_status"),
            on_resync=callbacks.get("on_resync"),
        )
        self.coinm = CoinMUserStream(
            symbol=coinm_symbol,
            on_order_trade_update=callbacks.get("on_cm_order_trade_update"),
            on_account_update=callbacks.get("on_cm_account_update"),
            on_margin_call=callbacks.get("on_cm_margin_call"),
            on_resync=callbacks.get("on_resync"),
        )

    def start(self):
        self.spot.start(); self.coinm.start()
    def stop(self):
        self.spot.stop(); self.coinm.stop()

# ====== 最小使用示例 ======
if __name__ == "__main__":
    def print_k(kind):
        def _f(d):
            print(f"[{kind}]", json.dumps(d)[:240], "...")
        return _f
    us = UserStreams(
        callbacks={
            "on_spot_exec_report": print_k("spot.exec"),
            "on_spot_balance": print_k("spot.bal"),
            "on_spot_list_status": print_k("spot.list"),
            "on_cm_order_trade_update": print_k("cm.otu"),
            "on_cm_account_update": print_k("cm.acct"),
            "on_cm_margin_call": print_k("cm.MC"),
            "on_resync": print_k("RESYNC"),
        }
    )
    us.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        us.stop()
