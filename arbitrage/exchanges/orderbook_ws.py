# -*- coding: utf-8 -*-
"""
REST 快照 + WS 增量维护本地 Order Book（spot & coin-m），含断序自动重建 + 断线重连 + staleness 看门狗。
"""
import json, threading, time, traceback
from collections import deque
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Optional

try:
    import websocket  # pip install websocket-client
except Exception as e:
    raise ImportError("缺少依赖 websocket-client，请先：pip install websocket-client") from e

from arbitrage.exchanges.binance_rest import spot_get, dapi_get

# —— 调参常量 ——（可按需在 config 中外置）
WAIT_BEFORE_SNAPSHOT_SEC = 1.5    # 拉快照前，先让WS事件进入缓冲
WAIT_BRIDGE_EVENT_SEC    = 2.0    # 快照后等待“桥接事件”的最长时间
BRIDGE_POLL_SLEEP_SEC    = 0.02
STALE_SEC                = 3.0    # 若 last_ts 超过该秒数无事件 → 触发重建
RECONNECT_BACKOFF_INIT   = 1.0    # WS 重连退避初始秒
RECONNECT_BACKOFF_MAX    = 30.0   # WS 重连退避上限

@dataclass
class BookState:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update_id: int = 0
    last_u: int = 0
    ready: bool = False
    last_ts: float = 0.0

    def top(self, side: str, limit: int) -> List[Tuple[float, float]]:
        book = self.bids if side == "bids" else self.asks
        if side == "bids":
            prices = sorted(book.keys(), reverse=True)[:limit]
        else:
            prices = sorted(book.keys())[:limit]
        return [(p, book[p]) for p in prices if book[p] > 0.0]

def _apply_side(updates, book: Dict[float, float]):
    for px_s, q_s in updates:
        p = float(px_s); q = float(q_s)
        if q == 0.0:
            book.pop(p, None)
        else:
            book[p] = q

class _WSClient:
    """带退避重连的 WS 客户端；on_evt: 'open'|'close'|'error'"""
    def __init__(self, url: str, on_msg, name: str, on_evt=None):
        self.url = url
        self.on_msg = on_msg
        self.name = name
        self.on_evt = on_evt
        self.ws = None
        self.t = None
        self._running = False
        self._lock = threading.Lock()

    def _run(self):
        backoff = RECONNECT_BACKOFF_INIT
        while self._running:
            try:
                def _on_message(ws, msg):
                    self.on_msg(msg)

                def _on_error(ws, err):
                    print(f"[WS:{self.name}] error:", err)
                    if self.on_evt: self.on_evt("error")

                def _on_close(ws, *a):
                    print(f"[WS:{self.name}] closed")
                    if self.on_evt: self.on_evt("close")

                def _on_open(ws):
                    print(f"[WS:{self.name}] opened")
                    if self.on_evt: self.on_evt("open")

                self.ws = websocket.WebSocketApp(
                    self.url,
                    on_message=_on_message,
                    on_error=_on_error,
                    on_close=_on_close,
                    on_open=_on_open
                )
                # 退出 run_forever() 的典型原因：网络断开/对端关闭/异常
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
            except Exception as e:
                print(f"[WS:{self.name}] run_forever exception:", e)
                if self.on_evt: self.on_evt("error")
            finally:
                if not self._running:
                    break
                # 进入退避重连
                print(f"[WS:{self.name}] reconnecting in {backoff:.1f}s ...")
                time.sleep(backoff)
                backoff = min(RECONNECT_BACKOFF_MAX, backoff * 2)

    def start(self):
        with self._lock:
            if self._running: return
            self._running = True
            self.t = threading.Thread(target=self._run, daemon=True)
            self.t.start()

    def stop(self):
        with self._lock:
            self._running = False
            try:
                if self.ws:
                    self.ws.close()
            except Exception:
                pass

class DepthMaintainer:
    """现货 or COIN-M 任一交易对的本地簿维护（@depth@100ms + /depth 快照）"""
    def __init__(self, symbol: str, venue: str, ws_base: str, limit: int = 200):
        self.symbol = symbol.upper()
        self.venue  = venue
        self.ws_url = f"{ws_base}/stream?streams={self.symbol.lower()}@depth@100ms"
        self.limit  = max(5, min(1000, limit))
        self.state  = BookState()
        self.buf    = deque(maxlen=10000)
        self.lock   = threading.Lock()
        self._resyncing = False
        self._stop   = False

        self.cli = _WSClient(self.ws_url, self._on_msg, name=f"{venue}:{self.symbol}", on_evt=self._on_ws_evt)
        self._sync_thread     = threading.Thread(target=self._sync_loop, daemon=True)
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)

    def start(self):
        self.cli.start()
        self._sync_thread.start()
        self._watchdog_thread.start()

    def stop(self):
        self._stop = True
        self.cli.stop()

    def _on_ws_evt(self, evt: str):
        # WS 关闭/报错 → 触发 resync（下一次连接成功后将重新拉快照并桥接）
        if evt in ("close", "error"):
            self._trigger_resync()

    def get_levels(self, limit=100):
        with self.lock:
            bids = self.state.top("bids", limit)
            asks = self.state.top("asks", limit)
            ts   = self.state.last_ts
            ok   = self.state.ready and len(bids)>0 and len(asks)>0
            return bids, asks, ts, ok

    def _on_msg(self, msg: str):
        try:
            d = json.loads(msg); x = d.get("data", d)
            if x.get("e") != "depthUpdate": return
            U = int(x["U"]); u = int(x["u"])
            pu = x.get("pu"); pu = int(pu) if pu not in (None, "") else None
            b = x.get("b", []); a = x.get("a", [])
            ev = (U, u, pu, b, a, time.time())
            with self.lock:
                if not self.state.ready:
                    self.buf.append(ev); return
                # —— 连续性校验 ——（含 pu / U-u 双口）
                if pu is not None:
                    if pu != self.state.last_u:
                        print(f"[{self.venue}:{self.symbol}] seq gap (pu={pu}, last_u={self.state.last_u}) → resync")
                        self._trigger_resync(); return
                else:
                    if u < self.state.last_u + 1:
                        return
                    if U > self.state.last_u + 1:
                        print(f"[{self.venue}:{self.symbol}] seq gap (U={U}, last_u={self.state.last_u}) → resync")
                        self._trigger_resync(); return
                _apply_side(b, self.state.bids); _apply_side(a, self.state.asks)
                self.state.last_u = u; self.state.last_ts = time.time()
        except Exception as e:
            print(f"[{self.venue}:{self.symbol}] parse/apply error:", e); traceback.print_exc()

    def _trigger_resync(self):
        with self.lock:
            if not self._resyncing:
                self._resyncing = True
                self.state.ready = False
                self.buf.clear()

    def _sync_loop(self):
        while not self._stop:
            if self.state.ready:
                time.sleep(0.05); continue
            try:
                # 拉快照前稍等，让缓冲里先有WS事件（否则桥接容易失败）
                t0 = time.time()
                while len(self.buf) == 0 and (time.time() - t0) < WAIT_BEFORE_SNAPSHOT_SEC:
                    time.sleep(0.01)

                snap = self._rest_depth_snapshot()
                lastUpdateId = int(snap.get("lastUpdateId", 0))
                bids = [(float(p), float(q)) for p,q in snap.get("bids", [])]
                asks = [(float(p), float(q)) for p,q in snap.get("asks", [])]
                with self.lock:
                    self.state.bids = {p:q for p,q in bids}
                    self.state.asks = {p:q for p,q in asks}
                    self.state.last_update_id = lastUpdateId
                    self.state.last_u = lastUpdateId

                self._drain_buffer_after_snapshot()

                with self.lock:
                    self.state.ready = True
                    self.state.last_ts = time.time()
                print(f"[{self.venue}:{self.symbol}] orderbook READY (last_u={self.state.last_u})")
            except Exception as e:
                print(f"[{self.venue}:{self.symbol}] sync error:", e)
                time.sleep(0.5)
            finally:
                self._resyncing = False

    def _watchdog_loop(self):
        while not self._stop:
            time.sleep(0.5)
            with self.lock:
                ready = self.state.ready
                last  = self.state.last_ts
            if ready:
                age = time.time() - (last or 0.0)
                if age > STALE_SEC:
                    print(f"[{self.venue}:{self.symbol}] stale {age:.2f}s → resync")
                    self._trigger_resync()

    def _rest_depth_snapshot(self) -> dict:
        return spot_get("/api/v3/depth", {"symbol": self.symbol, "limit": self.limit}) \
               if self.venue == "spot" else \
               dapi_get("/dapi/v1/depth", {"symbol": self.symbol, "limit": self.limit})

    def _drain_buffer_after_snapshot(self):
        with self.lock:
            last_u = self.state.last_u
            lastUpdateId = self.state.last_update_id

        deadline = time.time() + WAIT_BRIDGE_EVENT_SEC
        start_idx = None

        while True:
            with self.lock:
                cached = list(self.buf)
            for i, (U, u, pu, b, a, _) in enumerate(cached):
                if U <= lastUpdateId + 1 <= u:
                    start_idx = i; break
            if start_idx is not None:
                break
            if time.time() > deadline:
                raise RuntimeError("no buffered event satisfies U <= lastUpdateId+1 <= u; will retry")
            time.sleep(BRIDGE_POLL_SLEEP_SEC)

        applied_u = last_u
        for (U, u, pu, b, a, _) in cached[start_idx:]:
            if pu is not None:
                if pu != applied_u:
                    raise RuntimeError(f"gap during catch-up: pu={pu} != applied_u={applied_u}")
            else:
                if u < applied_u + 1:
                    continue
                if U > applied_u + 1:
                    raise RuntimeError(f"gap during catch-up (U={U}, expected {applied_u+1})")
            _apply_side(b, self.state.bids); _apply_side(a, self.state.asks)
            applied_u = u
        with self.lock:
            self.state.last_u = applied_u; self.state.last_ts = time.time()
            for _ in range(len(cached)):
                if self.buf: self.buf.popleft()

class CoinMMarkWS:
    """订阅 COIN-M 标记价（@markPrice），带重连与基本看门狗。"""
    def __init__(self, symbol: str, ws_base: str):
        self.symbol = symbol.upper()
        self.ws_url = f"{ws_base}/stream?streams={self.symbol.lower()}@markPrice"
        self.mark: Optional[float] = None
        self.ts: float = time.time()
        self._stop = False
        self.cli = _WSClient(self.ws_url, self._on_msg, name=f"mark:{self.symbol}", on_evt=self._on_ws_evt)
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)

    def _on_msg(self, msg: str):
        try:
            d = json.loads(msg); x = d.get("data", d)
            if x.get("e") not in ("markPriceUpdate", None): return
            p = x.get("p", x.get("markPrice"))
            if p is not None:
                self.mark = float(p); self.ts = time.time()
        except Exception as e:
            print(f"[MARK:{self.symbol}] parse error:", e)

    def _on_ws_evt(self, evt: str):
        # 关闭/错误发生时，允许 mark 暂时为空，由上层回退到 cm_mid
        if evt in ("close", "error"):
            pass

    def start(self):
        self.cli.start()
        self._watchdog_thread.start()

    def stop(self):
        self._stop = True
        self.cli.stop()

    def _watchdog_loop(self):
        while not self._stop:
            time.sleep(1.0)
            age = time.time() - (self.ts or 0.0)
            if age > STALE_SEC:
                # 不强制 resync；交由 WS 自动重连；这里只做日志
                print(f"[MARK:{self.symbol}] stale {age:.1f}s (ws will reconnect automatically)")

class WSOrderBooks:
    """统一封装：现货深度 + COIN-M 深度 + 标记价"""
    def __init__(self, spot_symbol: str, coinm_symbol: str, ws_spot_base: str, ws_dapi_base: str, depth_limit: int = 200):
        self.spot = DepthMaintainer(spot_symbol, "spot",  ws_spot_base, limit=depth_limit)
        self.cm   = DepthMaintainer(coinm_symbol, "coinm", ws_dapi_base, limit=depth_limit)
        self.mark = CoinMMarkWS(coinm_symbol, ws_dapi_base)
    def start(self):
        self.spot.start(); self.cm.start(); self.mark.start()
    def stop(self):
        self.spot.stop(); self.cm.stop(); self.mark.stop()
    def ready(self) -> bool:
        return self.spot.state.ready and self.cm.state.ready
    def get_books(self, limit=100):
        sbids, sasks, ts_s, ok_s = self.spot.get_levels(limit)
        cbids, casks, ts_c, ok_c = self.cm.get_levels(limit)
        return (sbids, sasks, ts_s, ok_s), (cbids, casks, ts_c, ok_c), (self.mark.mark, self.mark.ts)
