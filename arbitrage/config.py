import os

# —— 运行参数 ——
RUN_SECONDS = 25
POLL_INTERVAL = 0.06    # WS 已经很快了，可适当调小打印/判定节奏
# === 运行开关 ===
DRY_RUN = False  # True 时不连用户流、不做查账；先本地压测可改 True

# —— 启用“REST快照+WS增量”模式与端点 ——（主网默认）
USE_WS_ORDERBOOK = True
WS_DEPTH_LIMIT   = 200
WS_SPOT_BASE = "wss://stream.binance.com:9443"
WS_DAPI_BASE = "wss://dstream.binance.com"

# —— 风控阈值 ——
MAX_BOOK_SKEW_MS = 500
PAIR_TIMEOUT_SEC = 2.0
PAIR_POLL_INTERVAL = 0.2

# —— 入场策略 ——
AUTO_FROM_FRONTIER = True
MIN_V_USD = 5_000
EXECUTION_MODE = "hybrid"  # "taker" or "maker" or "hybrid"

ENTER_BPS = 6.0
EXIT_BPS  = 2.0
STOP_BPS  = 12.0
MAX_HOLD_SEC = 30

V_USD = 10_000
MAX_SLIPPAGE_BPS_SPOT  = 1.0
MAX_SLIPPAGE_BPS_COINM = 2.0

ONLY_POSITIVE_CARRY = False
MAX_Q_BTC_FRONTIER = 0.5  # 基础币上限

# —— 资金费与保证金 ——
ENABLE_FUNDING_INFO = True
FUNDING_GAMMA = 0.8
FUNDING_BUFFER_SEC = 20

ENABLE_CM_RISK_CHECK = True
LIQ_DIST_MIN_PCT = 0.03
MARGIN_RATIO_MAX = 0.70

# —— 打印逐档 ——
PRINT_LEVELS = False     # 用 WS 逐档时，默认关闭频繁打印以免刷屏
LEVELS_TO_PRINT = 5

# —— 标的 & 端点 ——（主网）
PAIR = os.environ.get("PAIR", "BTC").upper()
SPOT_SYMBOL  = os.environ.get("SPOT_SYMBOL",  f"{PAIR}USDT")
COINM_SYMBOL = os.environ.get("COINM_SYMBOL", f"{PAIR}USD_PERP")

SPOT_BASE = os.environ.get("SPOT_BASE", "https://api.binance.com")
DAPI_BASE = os.environ.get("DAPI_BASE", "https://dapi.binance.com")

# —— WS 端点（主网；测试网自行切换）
WS_SPOT_BASE = os.environ.get("WS_SPOT_BASE", "wss://stream.binance.com:9443")
WS_DAPI_BASE = os.environ.get("WS_DAPI_BASE", "wss://dstream.binance.com")

# —— 启用“REST 快照 + WS 维护本地簿”
USE_WS_ORDERBOOK = True
WS_DEPTH_LIMIT = 5  # 本地簿维护层数

SPOT_KEY    = os.environ.get("SPOT_KEY", "")
SPOT_SECRET = os.environ.get("SPOT_SECRET", "")
DAPI_KEY    = os.environ.get("DAPI_KEY", "")
DAPI_SECRET = os.environ.get("DAPI_SECRET", "")

TRADES_CSV = os.environ.get("TRADES_CSV", f"trades_{PAIR.lower()}.csv")

# —— 执行模式 —— 现有 "taker" / "maker" 基础上新增 "hybrid"
EXECUTION_MODE = os.environ.get("EXECUTION_MODE", "hybrid")  # "taker" | "maker" | "hybrid"

# —— HYBRID 策略参数 ——
# 先在哪一腿挂 Maker： "auto"=自动选择深度更好的一侧，"spot"=只在现货挂，"coinm"=只在合约挂
HYBRID_MAKER_LEG = os.environ.get("HYBRID_MAKER_LEG", "auto")  # "auto"|"spot"|"coinm"
# Maker 等待秒数（超时未成交则撤单；若有部分成交，将对这部分做对冲）
HYBRID_WAIT_SEC = float(os.environ.get("HYBRID_WAIT_SEC", "1.0"))
# 认为“可以触发对冲”的最小成交比例（0~1）
HYBRID_MIN_FILL_RATIO = float(os.environ.get("HYBRID_MIN_FILL_RATIO", "0.3"))
