# 5021 FundingRate Predictor

即時資金費率預測系統，支援 Binance / Bybit / Bitget / Gate.io / OKX 五大交易所。

**Port：** Frontend 5021 | Backend 3021

---

## 功能

- **即時費率監控**：WebSocket 推播，延遲 < 500ms
- **結算費率預測**：基於 TWAP + inverse_funding 反推，主流幣誤差 < 1bp
- **下一分鐘費率預測**：PRE-SAMPLE 觸發（採樣前 1 秒），tick 均值外推
- **多幣種同時監控**：左側列表同步顯示所有幣種即時預測
- **預測準確度統計**：MAE / Bias / RMSE，自動記錄每分鐘 predicted vs actual
- **報價失靈偵測**：GateIO 幣種 premium_index 恆零時自動切換追蹤模式（如 HOOK）

---

## 支援交易所與公式

| 交易所 | 縮寫 | 公式 | I | δ | TWAP |
|--------|------|------|---|---|------|
| Binance | BN | F = clamp((P + clamp(I-P, ±δ))×N/8, ±cap) | 0.01%（固定） | 0.05%（固定） | 算術級數加權，滾動 8h |
| Bybit | BY | F = clamp(P + clamp(I-P, ±δ), ±cap) | 0.01%×N/8 | 0.05%（固定） | 算術級數加權，固定窗口 |
| Bitget | BG | F = clamp((P + clamp(I-P, ±δ))×N/8, ±cap) | 0.01%（固定） | 0.05%（固定） | 算術級數加權，固定窗口 |
| Gate.io | GT | F = clamp((P + clamp(I-P, ±δ))×N/8, ±cap) | 0.01%（固定，8h等效） | 0.05%（固定） | 算術級數加權，固定窗口 |
| OKX | OK | F = clamp(P + clamp(I-P, ±δ), ±cap) | 0.01%×N/8 | 0.05%（固定） | 算術級數加權，固定窗口 |

---

## 特殊機制

### 監控幣種持久化
- 新增/切換的幣種自動寫入 `monitors.json`，重啟後自動恢復
- 格式：`{"pairs": [...], "selected_key": "bybit:BTCUSDT"}`

### 預測精度警告標籤
- **`⚠ 預測精度下降`**：當以下任一條件成立時顯示
  - `high_volatility`：近 20 筆溢價標準差 > 0.3%（市場溢價不穩定）
  - `is_noisy_window`（僅 Bybit/Bitget）：結算週期已過 70%，公式反推誤差放大

### 貼 cap 反推修正
- 當 `official_rate` 觸及 cap 時，它被截斷、無法反推真實累積溢價
- `needed_for_*`（需維持溢價）與 `rate_if_zero`（溢價歸零費率）改用實際 `twap_premium` 計算
- 公式：`needed = (target_twap × window - actual_twap × elapsed) / remain`

### GateIO 報價失靈偵測
- `funding_rate_indicative = interest_rate_component`（= daily_rate / periods_per_day）時，代表 premium_index = 0（外部報價失靈）
- 自動設定 `_zero_premium_index = True`，切換為 `inverse_funding(official_rate)` 追蹤模式
- 費率跳變時自動恢復正常計算
- 前端「即時溢價」卡顯示黃色邊框 + 「報價失靈」badge

### Binance IMN 動態校正
- Impact Margin Notional = 200 USDT / Tier-1 MMR（從 leverageBracket API 取得）
- 每次結算後用 `p_inferred` 反推真實 IMN，EMA 平滑（α=0.3）

### Bitget 歷史 klines
- 無官方 premiumIndex klines，改為抓 `history-mark-candles` + `history-index-candles` 計算 (mark-index)/index
- 預填最多 200 筆 TWAP 歷史

---

## 架構

```
backend/
  main.py            FastAPI，多幣種 WS + 採樣 + 預測 loop
  exchange_client.py REST + WebSocket 封裝（四交易所）
  funding_engine.py  費率公式、TWAP、預測、inverse_funding
  stats_manager.py   預測準確度記錄（JSONL → logs/）

frontend/
  src/App.jsx        React + Tailwind，WebSocket 即時更新
```

---

## 啟動

```bash
start.bat        # 同時啟動 backend (3021) + frontend (5021)
```

---

## 已知限制

- Binance 每分鐘 12 次 5 秒採樣均值不公開，貼 cap 的極端小幣預測誤差有天花板
- Bitget premium klines 用 V2 history-mark-candles / history-index-candles 計算 (mark-index)/index；V3 PREMIUM_INDEX 回傳絕對價格（非比率），V3 MARKET_PRICE/INDEX_PRICE 兩者數值相同（premium 恆為 0），均不可用
- GateIO `premium_index_klines` history API 不穩定（from 參數可能被忽略）

---

## ⚠️ `self_improve.py` 會自動改寫原始碼

這支腳本會比對預測值與實際結算值，用 regex **直接改寫 `backend/funding_engine.py` 裡的
校正參數，然後重啟後端**。

跑之前請先確認：

- 你的 `funding_engine.py` 有進版控（它會被就地修改）
- 你接受「程式自己改自己」這件事

只想單純用預測功能的話，**不要跑這支**，`start.bat` 也不會啟動它。

## 執行期檔案

`monitors.json` 是監控清單的狀態檔，首次執行會自動建立，已列入 `.gitignore`。
