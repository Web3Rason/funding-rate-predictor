# 預測數據來源說明

## 核心公式

```
predicted_twap = (implied_twap × 已過分鐘 + forecast_p × 剩餘分鐘) / 窗口分鐘
predicted_rate = formula(predicted_twap)
```

兩個輸入：**implied_twap**（反推過去）+ **forecast_p**（預測未來）

---

## ① implied_twap — 從官方費率反推

各交易所的 `official_rate`（WS 即時推播），代表「截至目前為止的 TWAP 收斂到哪裡」，用二分搜尋反推出 `implied_twap`。

| 交易所 | 來源欄位 | 更新頻率 |
|--------|----------|----------|
| Binance | `funding_rate_indicative` | 每分鐘 |
| GateIO  | `funding_rate_indicative` | 每分鐘第 46 秒 |
| Bybit   | `fundingRate`（settled）  | 結算時跳變 |
| Bitget  | `fundingRate`（estimated）| 即時 |

---

## ② forecast_p — 即時訂單簿 Impact Premium

「剩餘時間若溢價維持不變，最終費率會是多少」

公式：
```
P = [Max(0, ImpactBid - Mark) - Max(0, Mark - ImpactAsk)] / Index
```

| 交易所 | 訂單簿來源 | 基準價格 |
|--------|------------|----------|
| Binance | diff stream + REST 快照（limit=1000） | Index |
| Bybit   | orderbook.50 WS（snapshot + delta）   | Mark  |
| Bitget  | books WS（snapshot + update）         | Mark  |
| GateIO  | depth stream + REST 快照              | Index |

> **GateIO 特例**：當某幣種 `premium_index` 恆為零（exchange 外部報價失靈），訂單簿 impact premium 與費率無關，自動切換為直接用 `official_rate` 反推 `forecast_p`，不再使用訂單簿。偵測條件：`funding_rate_indicative == interest_rate / periods_per_day`。

### IMN（Impact Margin Notional）各家取得方式

| 交易所 | API | 公式 |
|--------|-----|------|
| Binance | `leverageBracket` → Tier-1 `maintMarginRatio` | IMN = 200 / MMR |
| Bybit   | 同上（`linearInverseInstrumentInfo`）           | IMN = 200 / MMR |
| Bitget  | `query-position-lever` → Tier-1 `keepMarginRate` | IMN = 200 / MMR |
| GateIO  | 合約 API `funding_impact_value` | 官方直接提供（USDT 名義金額） |

---

---

## 貼 cap 修正（at_cap correction）

當 `official_rate` 觸及 cap 上限（如 Gate.io ONG = -2%），費率被截斷，`inverse_funding(official_rate)` 只能算出邊界值（如 -2.05%），無法反映真實累積溢價（可能是 -3.8%）。

**受影響的計算**：
- `needed_for_neg_cap_pct`（需維持多少溢價才能結算在 -cap）
- `needed_for_cap_pct`（需維持多少溢價才能結算在 +cap）
- `rate_if_zero_pct`（若剩餘時間溢價歸零，結算費率是多少）

**修正邏輯**：
```python
elapsed_twap = twap if at_cap else inverse_funding(official_rate)
# needed：
needed = (target_twap × window - elapsed_twap × elapsed) / remain
# rate_if_zero：
zero_twap = (elapsed_twap × elapsed) / window
rate_if_zero = funding_from_premium(zero_twap)
```

**`predicted_rate` 不受影響**：已有獨立的 at_cap 修正（369-373 行），直接將 `implied_twap` 替換為 `twap`。

---

## 沒有跨交易所資料

**根本原因：公式不需要。**

各交易所的費率公式只有兩個輸入——自己訂單簿的 impact premium、自己的 official rate——沒有任何參數來自其他交易所，跨交易所資料對公式無處代入。
