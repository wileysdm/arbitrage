
## 2025.9.25更新，获取行情更改为websocket方式。
### 用rest获取一次快照，之后用websocket获取订单簿变化情况，以此来维护本地订单簿与价格
### 下单仍用rest方式
### 需要安装 websocket-client库
---------------------------------------------------------------------------
## 2025.9.16更新，支持多币种套利。
### 示例用法: 
### set PAIR = ETH
### python -m app.main
### 或者
### python -m app.main --spot ETHUSDT --coinm ETHUSD_PERP
### 注：币安测试网仅有BTC可以用，其它都无法使用。
---------------------------------------------------------------------------

# SPOT-CM-arbitrage
# README — BTC 现货 × 币本位永续 对冲 / 套利原型（Binance Testnet）

> **目标**：通过「现货 BTCUSDT」与「币本位永续 BTCUSD_PERP（逆向合约）」的对冲，力图让账户中的 **BTC 数量**随时间**净增加**（以 BTC 作为唯一结算口径）。  
> **场景**：**Binance 测试网（Testnet）**。  
> **特点**：逐档 **VWAP/前沿** 评估、两腿 **配对监控与救援**、**保证金风控**、**资金费（Funding）提示**、模块化接口。

---

## 1. 套利/对冲原理

### 1.1 为什么两边价格会联动？
- **同一标的**：现货 BTCUSDT 与币本位永续 BTCUSD_PERP 都在定价“比特币”。只是结算单位不同（USDT vs BTC）、形态不同（现货 vs 永续合约）。

- **资金费（Funding）的“慢回拉”**：永续无交割日，交易所通过资金费将其锚在现货指数附近：
  - 永续价 **高于** 现货 ⇒ 资金费 **为正**：多头付钱给空头，鼓励 **做空永续 / 做多现货**，把永续压回去；
  - 永续价 **低于** 现货 ⇒ 资金费 **为负**：空头付钱给多头，鼓励 **做多永续 / 做空现货**，把永续拉回去。


### 1.2 为什么还会产生价差？
即使长期联动，短时间内仍会“错位”，常见原因：
1) **撮合与延迟**：两个订单簿在不同引擎，信息传播有毫秒级延迟；单侧大单冲击时，另一侧来不及同步。
2) **流动性与库存**：杠杆偏好常令永续更受追捧而溢价；做市/套利的库存与保证金有限，无法瞬间吃光差价。
3) **资金费预期**：若市场预期未来资金费持续为正（或为负），愿意短期接受偏高（或偏低）的永续价格，形成方向性“基差”。
4) **口径差异**：信号多用 **Mark Price**（指数+保护机制），与最新成交价并非同一数；剧烈波动/强平时差异被放大。
5) **被动/非理性交易**：强平、被动减仓、资金再平衡、跟单盘，都可能短暂把一侧价格推离另一侧。

### 1.3 为何通常会回归？
> “回归”不是保证，但**机制上**存在回拉力，使价差有较高概率回到合理区间。

- **可赚套利=资金会行动**：当差价超出“手续费 + 滑点 + 资金成本”，套利者会：  
  - 永续 **偏高**：**空永续 / 多现货**（cash-and-carry）→ 卖压打低永续、买盘抬升现货 → **差价缩小**；  
  - 永续 **偏低**：反向操作，差价同样被压缩。
- **资金费的慢回拉**：若永续长期高于现货，资金费常 **为正**，空头持续 **收取** 资金费，吸引更多资金参与，促进回归；反之亦然。
- **做市库存均衡**：做市商围绕现货指数价双边报价，库存偏离会促使其调整报价到更合理位置，拉近两边价格。

### 1.4 但也可能长时间不回归（风险）
- **趋势市**：牛市里**正基差**、熊市里**负基差**可长期存在；
- **资金费“站队”**：若市场一致看涨，资金费或长期为正；做“空永续/多现货”在等待回归时需要承受资金费与价格波动；
- **约束与冲击**：保证金/库存限制、流动性骤降、强平链，会让差价先扩大再修复，甚至久不回归。

> 因此系统需要：**入场阈值（ENTER_BPS）**、**退出阈值（EXIT_BPS/STOP_BPS）**、**持仓超时（MAX_HOLD_SEC）**、**保证金与强平距离检查**等工程化保护。

### 1.5 一眼看懂的小结
- **联动**：同一标的 + 套保/做市传动带 + 资金费回拉；
- **价差**：延迟、流动性/库存、资金费预期、口径差异、被动交易；
- **回归**：套利/做市与资金费共同施力；但**不保证立刻**，需风控与退出机制配合。

---

## 2. 与 Binance 文档的一致性（Spot + COIN-M / Testnet）

> **只列出本代码实际调用到的接口/字段与单位**，确保端到端语义与官方一致。

### 2.1 认证与签名
- 所有 **`USER_DATA/TRADE`** 类请求均走统一签名：
  - `timestamp`、可选 `recvWindow`
  - `HMAC-SHA256(signature)` 签名整个 query
  - API Key 置于请求头 `X-MBX-APIKEY`
- 现货基址：`https://testnet.binance.vision`（Spot Testnet）  
  合约基址：`https://testnet.binancefuture.com`（COIN-M Testnet）  
- 代码位置：`r_signed()`（统一封装）

### 2.2 交易规则与单位（下单前取整）
- **Spot** `GET /api/v3/exchangeInfo`  
  - `PRICE_FILTER.tickSize` → 价格步长  
  - `LOT_SIZE.stepSize` → 数量步长  
  - 单位：价格 **USDT**，数量 **BTC**
- **COIN-M** `GET /dapi/v1/exchangeInfo`  
  - `contractSize` → 每张合约的 **USD/张** 面值  
  - `PRICE_FILTER.tickSize`、`LOT_SIZE.stepSize` → 价格/张数步长  
  - 单位：价格 **USD/BTC**，数量 **contracts(张)**
- 代码位置：`fetch_spot_rules()` / `fetch_coinm_rules()` + `round_step(...)`

### 2.3 行情口径与价差定义
- **订单簿**：  
  - Spot：`GET /api/v3/depth` → `bids/asks (price, qty_BTC)`  
  - COIN-M：`GET /dapi/v1/depth` → `bids/asks (price, qty_contracts)`
- **标记价/资金费**：`GET /dapi/v1/premiumIndex`  
  - `markPrice`（用于 `spread_bps`）  
  - `lastFundingRate`（小数，代码换算为 bp）  
  - `nextFundingTime`（毫秒时间戳）
- **价差定义**：`spread_bps = (markPrice − spotMid) / spotMid × 10,000`  
  代码位置：主循环计算

### 2.4 订单类型与关键参数
- **Spot 下单** `POST /api/v3/order`  
  - `type=MARKET` 或 `type=LIMIT_MAKER`（Post-Only，吃单将被拒）  
- **COIN-M 下单** `POST /dapi/v1/order`  
  - Maker：`type=LIMIT + timeInForce=GTX`（Post-Only）  
  - Taker：`type=MARKET`，**平仓**时带 `reduceOnly=true`（只减仓）  
- **订单查询**：  
  - Spot：`GET /api/v3/order`  
  - COIN-M：`GET /dapi/v1/order`  
- 统一使用 `newOrderRespType="RESULT"`  
- 代码位置：`place_spot_*()` / `place_coinm_*()` / `get_*_order_status()`

### 2.5 成交回报与费用/资金费记账
- **Spot 成交** `GET /api/v3/myTrades`  
  - 汇总 `price × qty` 得实际 **VWAP**  
  - 读取 `commission/commissionAsset`，若为 USDT → 以成交价折算为 **BTC**  
- **COIN-M 成交** `GET /dapi/v1/userTrades`  
  - 汇总成交、读取 `commission`（单位 **BTC**）
- **资金费/已实现盈亏** `GET /dapi/v1/income`  
  - 仅计 `FUNDING_FEE`、`REALIZED_PNL`（单位 **BTC**）
- 代码位置：`record_spot_order_impact()` / `record_coinm_order_impact()` / `pull_coinm_income_since_cursor()`

### 2.6 保证金与强平风控（入场前）
- **仓位风险** `GET /dapi/v1/positionRisk` → `liquidationPrice`  
- **账户** `GET /dapi/v1/account` → `totalMaintMargin/totalWalletBalance`  
- 逻辑：强平距离、维护保证金比 **任一不达标即拒单**  
- 代码位置：`check_cm_margin_ok()`

### 2.7 返回结构与响应控制
- 下单统一 `RESULT` 语义；订单状态字段（如 `status/executedQty` 或 `cumQty`）按接口解析  
- 失败/异常路径（HTTPError/超时）→ 打印并短暂休眠后继续循环  
- 代码位置：下单/查询封装与主循环 `try/except`

### 2.8 Testnet 与主网
- Testnet 与主网 **接口、签名、字段语义一致**；仅域名与撮合环境不同  
- 本仓库默认使用 **Testnet**，便于无风险联调

---

## 3. 账户与结算（BTC 唯一口径）
- **现货腿**：买入 `+Q_btc`、卖出 `−Q_btc`；手续费若以 USDT 计，按成交 VWAP 换算为 BTC 扣减。  
- **合约腿**：开/平仓手续费为 BTC；通过收益端点拉取 **Funding/Realized PnL（BTC）** 并累计。  
- **结果展示**：每笔交易结束（双腿都平）后，统计 **ΔBTC** = 现货变动 + 合约手续费/收益 + Funding。

---

## 4. 行情与规则获取（Testnet API）
- **现货深度**：`/api/v3/depth` → 价格 `USDT/BTC`、数量 `BTC`  
- **COIN‑M 深度**：`/dapi/v1/depth` → 价格 `USD/BTC`、数量 `contracts`  
- **标记价/资金费**：`/dapi/v1/premiumIndex` → `markPrice`、`lastFundingRate`、`nextFundingTime`  
- **交易所规则**：
  - 现货：`/api/v3/exchangeInfo` → `tickSize`、`stepSize`
  - COIN‑M：`/dapi/v1/exchangeInfo` → `contractSize`、`tickSize`、`stepSize`
- **错位簿防护**：比较两侧抓取的**本地时间**差，超过 `MAX_BOOK_SKEW_MS` 则丢弃该次信号。

---

## 5. 交易流程（入场/对冲/退出）

### 5.1 入场两模式
1) **前沿（逐档 VWAP）模式**（默认）：
   - 沿两侧订单簿累计 USD 容量，取**瓶颈名义** `V`；
   - 求两腿 VWAP 得 `arb_ratio / edge_usd`，过滤 `ENTER_BPS` 和 `MIN_V_USD`；
   - 触发 **现货腿上限**（`MAX_Q_BTC_FRONTIER`）时，两腿按比例缩放后按步进取整；
   - `EXECUTION_MODE` 可选 `taker/maker`。

2) **固定名义模式**：
   - 使用 `V_USD` 计算 `Q_btc / N_cntr`，在簿上模拟 VWAP 与滑点；
   - 满足 `ENTER_BPS` 和滑点阈值后（如 `MAX_SLIPPAGE_BPS_*`）执行。

### 5.2 配对监控与单腿救援（安全阀）
- 两腿下单后，在 `PAIR_TIMEOUT_SEC` 内轮询成交；若仅**一腿**成交：
  - 仅**现货**成交 → 立即**市价**反向平现货；
  - 仅**合约**成交 → 立即 **reduceOnly 市价**平合约。

### 5.3 退出条件（择一即触发）
- **回归**：`|spread_bps| ≤ EXIT_BPS`  
- **止损**：`|spread_bps| ≥ STOP_BPS`  
- **超时**：持仓时间 `≥ MAX_HOLD_SEC`  
- 触发后以市价平仓（合约腿 `reduceOnly`）。

---

## 6. 费用与滑点（统一折算 BTC）
- **现货手续费**：来自 `myTrades` 的 `commission/commissionAsset`；若为 USDT：`fee_BTC = fee_USDT / spot_vwap`。  
- **合约手续费**：`userTrades.commission`，天然是 BTC。  
- **资金费 / Realized PnL**：从 `/dapi/v1/income` 拉取 `FUNDING_FEE/REALIZED_PNL`（单位 BTC）累加。  
- **滑点控制**：用簿上 VWAP 与顶档价偏离（bp）做阈值；实际按真实成交计入盈亏。

---

## 7. 风险控制（硬阈值 + 执行期兜底 + 建议）

> **安全优先**：任何硬阈值不通过 ⇒ 直接拒单；任何一腿超时 ⇒ 立即对冲；异常 ⇒ 保守处理并继续循环。

### 7.1 术语
- `Mark`（COIN-M 标记价，USD）、`Liq`（强平价，USD）  
- `dist = |Mark − Liq| / Mark`（强平距离）  
- `spread_bps = (Mark − SpotMid) / SpotMid × 10,000`  
- `slippage_bps = |VWAP − 最优价| / 最优价 × 10,000`

### 7.2 入场前硬阈值（严格拦截）
**A. 簿时间一致性（数据质量）**  
- 规则：`skew_ms = |t_cm − t_spot| × 1000 ≤ MAX_BOOK_SKEW_MS`（默认 **500ms**）  
- 不满足：丢弃当次信号  
- 代码：主循环 `skew_ms` 判断

**B. 保证金/强平（COIN-M）**  
- 强平距离：`dist ≥ LIQ_DIST_MIN_PCT`（默认 **3%**）  
- 维护保证金比：`totalMaintMargin / totalWalletBalance ≤ MARGIN_RATIO_MAX`（默认 **70%**）  
- 任一不满足：**拒绝入场**  
- 代码：`check_cm_margin_ok()`

**C. 收益与名义门槛（前沿筛选）**  
- 最小套利：`ENTER_BPS`（默认 **6 bp**）  
- 最小名义：`MIN_V_USD`（默认 **$5,000**）  
- 代码：`collect_frontier_candidates(..., min_bp=ENTER_BPS, min_vusd=MIN_V_USD)`

**D. 过滤器（价格/数量合法性）**  
- Spot：数量按 `LOT_SIZE.stepSize` 取整  
- COIN-M：张数按 `stepSize` 取整  
- 代码：`fetch_*_rules()` + `round_step(...)`

**E. 规模上限（防过度放大）**  
- `MAX_Q_BTC_FRONTIER`（默认 **0.5 BTC**）：超过则**按比例缩两腿**  
- 代码：`place_entry_from_row()`（打印 `[CAP]`）

### 7.3 执行期风险（下单 → 成交）
**A. 单腿监控与紧急对冲（强约束）**  
- 监控窗口：`PAIR_TIMEOUT_SEC`（默认 **2s**），轮询：`PAIR_POLL_INTERVAL`（默认 **0.2s**）  
- 仅一腿成交时：  
  - 只 **Spot** 成交 ⇒ 立即 **市价反向** 平掉 Spot  
  - 只 **COIN-M** 成交 ⇒ 立即 **市价 + reduceOnly** 平掉合约  
- 代码：`monitor_and_rescue_single_leg()`

**B. 滑点与可成交性（固定名义路径）**  
- 超出滑点上限则放弃入场：Spot `MAX_SLIPPAGE_BPS_SPOT`（**1 bp**）、COIN-M `MAX_SLIPPAGE_BPS_COINM`（**2 bp**）  
- 代码：`try_enter()` → `vwap_slippage_bps()`

**C. Post-Only 保障**  
- Maker：Spot `LIMIT_MAKER`、COIN-M `LIMIT + timeInForce=GTX`（可能被拒以避免吃单）  
- Taker：两腿市价（前沿模式或救火）

### 7.4 持有与退出（择一即触发）
- **回归**：`|spread_bps| ≤ EXIT_BPS`（**2 bp**）  
- **止损**：`|spread_bps| ≥ STOP_BPS`（**12 bp**）  
- **超时**：`age ≥ MAX_HOLD_SEC`（**30 s**）  
- 执行：Spot 市价；COIN-M 市价 **`reduceOnly=true`**  
- 代码：`need_exit()` + `do_exit()`

### 7.5 资金费率（信息提示）
- 打印 `lastFundingRate`（转为 bp）与 `nextFundingTime`，并评估持有期是否**大概率跨结算**（缓冲 `FUNDING_BUFFER_SEC=20s`）  
- **提示用途**；当前不直接参与入/退判定  
- 代码：`get_coinm_funding()` / `will_cross_next_funding()`

### 7.6 异常与故障
- **HTTP/网络**：捕获后打印，**休眠 1.0–1.2s** 再继续  
- **风控接口异常**：`check_cm_margin_ok()` 异常 ⇒ **保守拒单**  
- **数据缺失**：任一簿为空 ⇒ **跳过本轮**  
- **CSV 写失败**：打印告警但不终止主循环

### 7.7 执行前自检（Checklist）
- [ ] `skew_ms ≤ MAX_BOOK_SKEW_MS`  
- [ ] `dist ≥ LIQ_DIST_MIN_PCT` 且 `MaintRatio ≤ MARGIN_RATIO_MAX`  
- [ ] `ENTER_BPS` 达标，候选名义 ≥ `MIN_V_USD`  
- [ ] Spot/COIN-M 数量均按 `stepSize` 取整  
- [ ] 若触发上限：确认 `[CAP]` 后名义仍匹配  
- [ ] Maker 参数：Spot `LIMIT_MAKER` / COIN-M `GTX`  
- [ ] 预计持有期是否跨资金费结算（仅提示）

---

## 8. 运行说明（Testnet）

### 8.1 安装依赖
```bash
pip install requests
```

### 8.2 设置环境变量（测试网 API）
```bash
# 现货
export SPOT_KEY=your_spot_key
export SPOT_SECRET=your_spot_secret
# 币本位永续
export DAPI_KEY=your_dapi_key
export DAPI_SECRET=your_dapi_secret
```

### 8.3 运行
```bash
python -m app.main
```
> 首次建议在 `arbitrage/config.py` 将 `DRY_RUN = True`（只打印，不下单）。

---

## 9. 关键参数（常用）
| 参数 | 含义 | 典型值 |
|---|---|---|
| `DRY_RUN` | 只打印不下单 | `True`（首次） |
| `RUN_SECONDS` | 主循环时长（秒） | `60` |
| `POLL_INTERVAL` | 拉簿频率（秒） | `0.6`–`1.0` |
| `ENTER_BPS / EXIT_BPS / STOP_BPS` | 入/回归/止损阈值（bp） | `6 / 2 / 12` |
| `MAX_HOLD_SEC` | 持仓超时（秒） | `30`–`120` |
| `AUTO_FROM_FRONTIER` | 用前沿筛选 | `True` |
| `MIN_V_USD` | 最小名义（USD） | `5000` |
| `MAX_Q_BTC_FRONTIER` | 前沿现货腿上限（BTC） | `0.2`–`0.5` |
| `EXECUTION_MODE` | 执行模式 | `taker / maker` |

---

## 10. 输出文件（对账）
- **`trades_log.csv`**：
```
ts_ms, ts_iso, event, trade_id, side, Q_btc, N_cntr, spot_vwap, perp_vwap,
spot_orderId, cm_orderId, fee_btc_spot, fee_btc_cm, income_btc, delta_btc
```
- **ΔBTC** = 现货腿（±Q − 现货费_BTC） + 合约腿（−合约费_BTC + Realized PnL + Funding）。

---

## 11. 目录结构（模块化）
```
app/            # 程序入口（主循环）
arbitrage/
  config.py     # 运行参数与阈值（Testnet端点、API密钥）
  models.py     # 数据模型（Position等）
  utils.py      # 工具函数 & 交易CSV写入
  exchanges/    # 行情/下单/账户/规则/配对监控
  strategy/     # 前沿评估、风控、入场/退出编排
```

---

## 12. 风险与免责声明
- 本原型面向**测试网**。接入真实资金前，请务必补齐：
  1) 结构化日志与告警；  
  2) 重试/熔断/限流；  
  3) 手续费/滑点/资金费后的**净收益评估**；  
  4) 全局账户与仓位风控；  
  5) 持仓持久化与异常恢复。