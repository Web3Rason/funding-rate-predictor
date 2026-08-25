# -*- coding: utf-8 -*-
"""
資金費率計算引擎
- 各交易所公式計算即時資金費率
- Rolling TWAP（Binance 8h / Bybit 動態窗口）
- 預測結算費率（假設即時 Premium 維持不變）
- 反推達成目標費率所需的 Premium

各交易所公式（Binance & Bybit & Bitget 相同結構）：
  F = clamp(P + clamp(I - P, -δ, δ), -cap, cap)
  P = TWAP of premium index（rolling window）
  I 與 δ 依結算週期等比縮放（基準值為 8h）：
    8h: I=0.01%,   δ=±0.05%
    4h: I=0.005%,  δ=±0.025%
    1h: I=0.00125%,δ=±0.00625%
  cap 不隨週期縮放（實測驗證）
"""
import logging
import time
from collections import deque
from typing import Optional

logger = logging.getLogger("FundingEngine")

# 各交易所設定
# 公式結構三所相同：F = clamp(P + clamp(I - P, -δ, δ), -cap, cap)
# 差異在於：窗口大小（exchange_client 偵測）、利率（均 0.01%）、上限（各幣不同）
EXCHANGE_CONFIG = {
    "binance": {
        "base_interest_rate": 0.0001,   # 0.01%（固定 8h 基準，不隨週期縮放；由 result_scale 統一處理）
        "base_clamp_inner": 0.0005,     # ±0.05%（固定 8h 基準，不縮放）
        "default_cap": 0.0075,          # ±0.75%（多數幣種，大幣如 BTC/ETH 為 ±0.5%）
        "scale_by_interval": False,     # I/δ 固定不縮放；最後乘 N/8（result_scale）
        "result_scale_by_interval": True, # F × (N/8)：文檔公式 F = [...] / (8/N)
        "rolling_window": True,         # Binance TWAP = 滾動窗口（跨結算不重置）
        "weighted_twap": True,          # 文檔：結算 > 1h 用算術級數加權；1h 時由引擎切換為等權
    },
    "bybit": {
        "base_interest_rate": 0.0001,   # 0.01% per 8h（基準值，依結算週期等比縮放）
        "base_clamp_inner": 0.0005,     # ±0.05%（固定，不隨週期縮放；官方文檔確認）
        "default_cap": 0.0150,          # ±1.5%（各幣上限不同，BTCUSDT 為 ±0.5%，cap 不縮放）
        "scale_by_interval": True,      # I 依結算週期等比縮放（8h→0.01%，1h→0.00125%）
        "scale_clamp_by_interval": False, # δ 固定 ±0.05%，不隨週期縮放（官方文檔確認）
        "rolling_window": False,        # Bybit TWAP = 固定窗口，每次結算後從零累積
        "weighted_twap": True,          # 算術級數加權 TWAP（文檔：越接近結算點權重越高）
    },
    "bitget": {
        "base_interest_rate": 0.0001,   # 0.01%（固定，不隨週期縮放；由 result_scale 統一處理）
        "base_clamp_inner": 0.0005,     # ±0.05%（固定，不隨週期縮放）
        "default_cap": 0.003,           # ±0.3%（Bitget 上限較嚴）
        "scale_by_interval": False,     # I/δ 固定，不縮放；最後乘 N/8（result_scale）
        "result_scale_by_interval": True, # 公式最終結果 × (N/8)
        "rolling_window": False,        # Bitget TWAP = 固定窗口，每次結算後從零累積
        "weighted_twap": True,          # 算術級數加權（官方文檔：越接近結算點權重越高）
    },
    "gateio": {
        # 2026-05-27 Gate 資費演算法升級（公告 51275）：改為與 Binance 同構
        # F = clamp([P_twap + clamp(0.01% − P_twap, ±0.05%)] / (8/N), ±cap)
        # 溢價指數改「時間加權平均」；基礎利率固定 0.01%（8h 等效）；
        # 週期縮放改由 result_scale = N/8 處理（不再把利率按週期縮放）。
        # 實測（TAIKO 1h）：新公式對 funding_rate_indicative 誤差 <0.05%，舊公式差 >1.2%（撞 cap）。
        "base_interest_rate": 0.0001,   # 0.01%（8h 等效基礎利率，固定不縮放）
        "base_clamp_inner": 0.0005,     # ±0.05%（固定不縮放）
        "default_cap": 0.0075,          # ±0.75%（預設，由 API funding_rate_limit 覆蓋）
        "scale_by_interval": False,     # 利率不隨週期縮放（由 result_scale 統一處理）
        "result_scale_by_interval": True, # F = [...] × (N/8) = [...] /(8/N)（新算法核心）
        "rolling_window": False,        # 固定窗口，每次結算後從零累積
        "weighted_twap": True,          # 時間加權平均（算術級數加權，新算法）
    },
    "aster": {
        # 文檔：https://docs.asterdex.com/trading/perpetuals/fees-and-specs/funding-rate
        # F = [P + clamp(I - P, ±0.05%)] / (8/N)  — 與 Binance 公式完全相同
        "base_interest_rate": 0.0001,   # 0.01%（固定 8h 基準，不縮放；由 result_scale 處理）
        "base_clamp_inner": 0.0005,     # ±0.05%（固定，不縮放）
        "default_cap": 0.0075,          # ±0.75%（預設，依幣種而異）
        "scale_by_interval": False,     # I/δ 不縮放
        "result_scale_by_interval": True, # F × (N/8)
        "rolling_window": False,        # 固定窗口，每次結算後重置
        "weighted_twap": True,          # >1h 結算：算術級數加權（文檔明確），1h 時由引擎切換
    },
    "okx": {
        # 文檔：https://www.okx.com/help/how-to-calculate-funding-rate
        # F = clamp(AvgP + clamp(I - AvgP, -0.05%, +0.05%), -cap, +cap)
        # I = 0.03%/day / (24/N)：8h→0.01%, 4h→0.005%, 1h→0.00125%
        "base_interest_rate": 0.0001,   # 0.01% per 8h（基準值，依結算週期等比縮放）
        "base_clamp_inner": 0.0005,     # ±0.05%（固定，不隨週期縮放）
        "default_cap": 0.0150,          # ±1.5%（預設，由 API 動態取得覆蓋）
        "scale_by_interval": True,      # I 依結算週期等比縮放（與 Bybit 相同）
        "scale_clamp_by_interval": False, # δ 固定 ±0.05%，不隨週期縮放
        "rolling_window": False,        # 固定窗口，結算後重置
        "weighted_twap": True,          # 算術級數加權 TWAP（官方：最新分鐘權重最高）
    },
}


class FundingEngine:
    def __init__(self, exchange: str = "binance", symbol: str = "BTCUSDT", window_minutes: int = 480, cap_override: float = None,
                 interest_rate_override: Optional[float] = None):
        self.exchange = exchange.lower()
        self.symbol = symbol.upper()
        cfg = EXCHANGE_CONFIG.get(self.exchange, EXCHANGE_CONFIG["binance"])

        # 依結算週期等比縮放 I 與 clamp_inner
        # 8h = 480min 為基準（scale=1.0）；4h→0.5；1h→0.125
        # 例外：Gate.io 的 I/δ 不隨週期縮放（實測 1h HOOK 驗證）
        scale = window_minutes / 480.0
        # interest_rate_override：per-symbol 動態值（如 GateIO commodities 合約 I=0），
        # 不為 None 時直接覆蓋 EXCHANGE_CONFIG 的預設與縮放邏輯
        self._interest_rate_override = interest_rate_override
        if interest_rate_override is not None:
            self.interest_rate = interest_rate_override
        elif cfg.get("scale_by_interval", True):
            self.interest_rate = cfg["base_interest_rate"] * scale
        else:
            self.interest_rate = cfg["base_interest_rate"]
        # clamp_inner 可獨立控制是否縮放（Bybit：I 縮放但 δ 固定 ±0.05%）
        # 預設：與 scale_by_interval 相同
        scale_clamp = cfg.get("scale_clamp_by_interval", cfg.get("scale_by_interval", True))
        self.clamp_inner = cfg["base_clamp_inner"] * scale if scale_clamp else cfg["base_clamp_inner"]
        # Bitget 獨有：公式最終結果乘以 N/8（= window/480）
        # F = (P + clamp(I-P, ±δ)) × (N/8)，而非縮放 I/δ
        self.result_scale = scale if cfg.get("result_scale_by_interval", False) else 1.0
        # 是否使用線性遞增加權 TWAP（Bitget）
        self.weighted_twap = cfg.get("weighted_twap", False)
        # cap 不隨結算週期縮放；優先使用 API 回傳的幣種實際 cap
        self.cap = cap_override if cap_override is not None else cfg["default_cap"]
        self.window_minutes = window_minutes
        # 是否為滾動窗口（True = Binance；False = Bybit/Bitget/Gate.io 固定窗口）
        self.is_rolling = cfg.get("rolling_window", True)

        # Rolling TWAP 歷史（最多 window_minutes 筆）
        self._premium_history: deque = deque(maxlen=window_minutes)

        # 最後一筆精確 P（來自 kline close，預測時用此代替 mark/index 近似）
        self._last_precise_p: Optional[float] = None

        # 即時狀態
        self._last_ticker: dict = {}
        self._last_sample_minute: int = -1  # 上次採樣的分鐘（防重複）

        # 前端圖表歷史（最近 60 筆，每分鐘一筆）
        self._chart_history: list = []

        # 當前分鐘即時 premium tick 蒐集（由 main.py 每 0.5s 呼叫 tick()）
        # add_sample() 時清空，compute() 時用均值代替趨勢外推
        self._current_minute_ticks: list = []

        # 是否為「官方溢價指數恆為 0」的幣種（如 GateIO HOOK）
        # premium_index klines 全零 → 市場 mark/index 溢價無法代表資金費率計算基礎
        # 此時 forecast_p 應使用 inverse_funding(official_rate) 而非 instant_premium
        self._zero_premium_index: bool = False

        # Bybit WS snapshot 有時回傳 fundingRate = ""，導致 last_funding_rate 持續為 0
        # TWAP 採樣因 new_r=0 被跳過 → 歷史逐漸過時，預測不可信
        self._funding_rate_missing: bool = False

    # ─── 歷史預填 ────────────────────────────────────

    def preload_history(self, klines: list):
        """啟動時從 klines 預填 TWAP rolling window"""
        self._premium_history.clear()
        # 只取最近 window_minutes 筆
        recent = klines[-self.window_minutes:]
        for k in recent:
            self._premium_history.append(float(k.get("premium", 0)))

        # 同步圖表歷史（最近 60 筆）
        for k in klines[-60:]:
            self._chart_history.append({
                "ts": k["ts"],
                "premium_pct": round(float(k.get("premium", 0)) * 100, 6),
            })

        # 用最新一筆 kline 初始化精確 P
        if recent:
            self._last_precise_p = float(recent[-1].get("premium", 0))

        # 偵測「官方溢價指數恆為 0」：若所有 klines 的 premium 絕對值 < 1e-9
        # 代表此幣種在此交易所沒有外部參考溢價指數（例如 GateIO HOOK）
        # 此時 tick/impact premium 無法代表真實資金費率計算基礎，預測應追蹤 official_rate
        if klines:
            non_zero = sum(1 for k in klines if abs(float(k.get("premium", 0))) > 1e-9)
            self._zero_premium_index = (non_zero == 0)
            if self._zero_premium_index:
                logger.info(f"[Engine] ⚠ 偵測到溢價指數恆零，啟用 official_rate 追蹤模式")

        logger.info(
            f"[Engine] 預填完成: {len(self._premium_history)} 筆 premium | "
            f"窗口 {self.window_minutes}min | 交易所 {self.exchange}"
        )

    def reset_history(self):
        """結算後清空 rolling history，重新開始累積"""
        self._premium_history.clear()
        self._chart_history.clear()
        logger.info(f"[Engine] history 已重置（結算）")

    def resize_window(self, new_window: int):
        """切換幣種/交易所時更新窗口大小（同步重算縮放參數）"""
        if new_window == self.window_minutes:
            return
        old_data = list(self._premium_history)
        self.window_minutes = new_window
        self._premium_history = deque(old_data[-new_window:], maxlen=new_window)

        # 重新計算縮放後的 I 與 clamp_inner
        cfg = EXCHANGE_CONFIG.get(self.exchange, EXCHANGE_CONFIG["binance"])
        scale = new_window / 480.0
        # 尊重 per-symbol override（GateIO commodities 等）
        if self._interest_rate_override is not None:
            self.interest_rate = self._interest_rate_override
        elif cfg.get("scale_by_interval", True):
            self.interest_rate = cfg["base_interest_rate"] * scale
        else:
            self.interest_rate = cfg["base_interest_rate"]
        scale_clamp = cfg.get("scale_clamp_by_interval", cfg.get("scale_by_interval", True))
        self.clamp_inner = cfg["base_clamp_inner"] * scale if scale_clamp else cfg["base_clamp_inner"]
        self.result_scale = scale if cfg.get("result_scale_by_interval", False) else 1.0
        logger.info(f"[Engine] 窗口調整為 {new_window}min，scale={scale:.4f}，I={self.interest_rate*100:.5f}%，clamp=±{self.clamp_inner*100:.5f}%，result_scale={self.result_scale:.4f}")

    # ─── 即時 tick 蒐集（由 main.py 每 0.5s 呼叫）───────

    def tick(self, instant_premium: float):
        """
        蒐集即時 premium 樣本（每 0.5s），用於統計本分鐘的採樣次數（tick_count）。
        實測 tick_avg（本分鐘所有樣本的均值）與 premiumIndexKlines 差距 <5%，
        準確度尚可，但 forecast_next_p 目前優先採用更精確的 history 趨勢外推。

        0 是合法樣本：當訂單簿對稱（ImpactBid ≤ Mark ≤ ImpactAsk）時，
        impact premium 公式 Max(0, ImpactBid-Mark) - Max(0, Mark-ImpactAsk) 本就 = 0，
        代表市場平衡。過濾 0 會讓 tick_avg 系統性高估，且導致「即時溢價=0 時
        next_minute_rate 仍依 history 趨勢外推」的 bug。
        """
        self._current_minute_ticks.append(instant_premium)
        # 保留最多 200 個（防止異常累積）
        if len(self._current_minute_ticks) > 200:
            self._current_minute_ticks = self._current_minute_ticks[-200:]

    # ─── 採樣（由 main.py 每分鐘呼叫）──────────────────

    def add_sample(self, p: float):
        """
        添加一筆 premium index 樣本到 rolling window
        由 main.py 在每分鐘邊界呼叫，並盡量傳入精確的 P 值
        （Binance/Bybit/Bitget/Aster/Gate.io: 真實 premiumIndex kline close）
        """
        now = time.time()
        self._premium_history.append(p)
        self._last_precise_p = p  # 每分鐘更新精確 P（來自 kline）
        self._current_minute_ticks = []  # 新分鐘開始，重置 tick 緩衝
        self._chart_history.append({
            "ts": now,
            "premium_pct": round(p * 100, 6),
        })
        if len(self._chart_history) > 60:
            self._chart_history = self._chart_history[-60:]

    # ─── 費率公式 ─────────────────────────────────────

    def funding_from_premium(self, p_twap: float) -> float:
        """
        各交易所通用公式：
          Binance/Bybit/GateIO: F = clamp(P + clamp(I-P, -δ, δ), -cap, cap)
          Bitget:               F = clamp((P + clamp(I-P, -δ, δ)) × result_scale, -cap, cap)
                                result_scale = N/8（N = 結算小時數）
        """
        inner = max(-self.clamp_inner, min(self.clamp_inner, self.interest_rate - p_twap))
        return max(-self.cap, min(self.cap, (p_twap + inner) * self.result_scale))

    def inverse_funding(self, target_rate: float) -> float:
        """
        從目標費率反推所需 TWAP premium（二分搜尋）
        回傳最接近 0 的邊界 P 使 F(P) = target_rate
        搜尋範圍擴大至 ±20%，避免極端溢價幣種（如貼 cap 的小幣）超出範圍
        """
        lo, hi = -0.20, 0.20
        for _ in range(120):
            mid = (lo + hi) / 2
            f = self.funding_from_premium(mid)
            if target_rate <= 0:
                # 找最大（最靠近0）的 P 使 F(P) <= target
                if f <= target_rate:
                    lo = mid
                else:
                    hi = mid
            else:
                if f >= target_rate:
                    hi = mid
                else:
                    lo = mid
        return (lo + hi) / 2

    # ─── TWAP 投射（加權/等權統一處理）─────────────────
    def _use_weighted_twap(self) -> bool:
        """當前窗口下是否使用算術級數加權 TWAP。
        Binance / Aster 在 1h 結算時官方切換為等權平均，其餘加權交易所固定加權。"""
        return self.weighted_twap and not (
            self.exchange in ("binance", "aster") and self.window_minutes <= 60
        )

    def _project_twap(self, past_twap: float, future_p: float, e: int) -> float:
        """將「已過 e 分鐘的 TWAP」+「未來常數 future_p」投射為整個窗口的 TWAP。

        加權模式（Binance>1h / Bybit / Bitget / Aster>1h / OKX）：
          final = (past_twap × W_past + future_p × W_future) / W_total
          其中 W_past = e(e+1)/2, W_future = (e+1+window)·r/2, W_total = window(window+1)/2

        等權模式（Gate.io / Binance 1h / Aster 1h）：
          final = (past_twap × e + future_p × r) / window
        """
        window = self.window_minutes
        if window <= 0:
            return past_twap
        e = max(0, min(e, window))
        r = window - e
        if r <= 0:
            return past_twap
        if self._use_weighted_twap():
            w_past   = e * (e + 1) / 2
            w_future = (e + 1 + window) * r / 2
            w_total  = window * (window + 1) / 2
            return (past_twap * w_past + future_p * w_future) / w_total
        return (past_twap * e + future_p * r) / window

    def _needed_future_p(self, target_twap: float, past_twap: float, e: int) -> Optional[float]:
        """反推：欲使結算 TWAP 達到 target_twap，未來剩餘 r 分鐘需維持的平均 premium。
        r <= 0 時回傳 None（已無剩餘時間可調整）。"""
        window = self.window_minutes
        if window <= 0:
            return None
        e = max(0, min(e, window))
        r = window - e
        if r <= 0:
            return None
        if self._use_weighted_twap():
            w_past   = e * (e + 1) / 2
            w_future = (e + 1 + window) * r / 2
            w_total  = window * (window + 1) / 2
            if w_future <= 0:
                return None
            return (target_twap * w_total - past_twap * w_past) / w_future
        return (target_twap * window - past_twap * e) / r

    # ─── 核心計算 ─────────────────────────────────────

    def compute(self, ticker: dict, next_funding_time: int) -> dict:
        """
        計算完整費率數據（即時 + TWAP + 預測）

        Args:
            ticker: 從 ExchangeClient.get_ticker() 取得
            next_funding_time: 下次結算時間（ms timestamp）
        """
        instant_premium = ticker.get("instant_premium", 0)
        mark = ticker.get("mark_price", 0)
        index = ticker.get("index_price", 0)
        bid1 = ticker.get("bid1", 0)
        ask1 = ticker.get("ask1", 0)
        official_rate = ticker.get("last_funding_rate", 0)

        # ─── TWAP：算術級數加權（Binance/Bybit/Bitget/Aster >1h）或等權（1h / GateIO）───
        # history[0]=最舊, history[-1]=最新
        history = list(self._premium_history)
        history_count = len(history)

        def get_twap(vals: list) -> float:
            n = len(vals)
            if n == 0:
                return instant_premium
            # 算術級數加權：Bybit / Bitget / Aster(>1h) / Binance(>1h)
            # Aster & Binance 在 1h 結算時切換為等權平均（官方文檔明確區分）
            # Bitget / Bybit 無此例外（固定算術級數加權）
            use_weighted = self.weighted_twap and not (
                self.exchange in ("binance", "aster") and self.window_minutes <= 60
            )
            if use_weighted:
                # P = (P₁×1 + P₂×2 + ... + Pₙ×n) / (1+2+...+n)
                total_w = n * (n + 1) / 2
                return sum((i + 1) * v for i, v in enumerate(vals)) / total_w
            return sum(vals) / n

        twap = get_twap(history)

        # 基於 TWAP 計算費率
        calc_rate = self.funding_from_premium(twap)

        # 剩餘時間
        now = time.time()
        remain_sec = max(0.0, next_funding_time / 1000 - now)
        remain_min = max(1.0, remain_sec / 60)
        elapsed_min = max(0.0, self.window_minutes - remain_min)

        # ── 核心預測：以官方即時費率為基準，反推 implied TWAP，再用即時溢價投射 ──
        # official_rate 是交易所（Binance/Bybit/Bitget）持續更新的當期預測費率，
        # 是最準確的歷史基準。我們不再嘗試從 klines 自行重算 TWAP，
        # 而是反推 official_rate 對應的 implied_twap，再投射「若即時溢價維持不變」的結算費率。
        r = int(remain_min)
        e = max(1, self.window_minutes - r)   # 已過分鐘數（= elapsed 在窗口內的估計）

        # forecast_p：用於結算預測（反映「若當前溢價維持不變」的情境）
        # forecast_p：用哪個溢價填充剩餘時間
        # - Gate.io：depth 就緒用 instant_premium（impact 精確值），否則用 kline close fallback
        # - Binance：impact-based instant_premium；短暫歸零可能是深度空洞而非真實溢價，
        #            fallback 到 _last_precise_p 避免雜訊
        # - Bybit/Bitget：instant_premium = (mark-index)/index
        #            0 代表 mark = index（公平溢價），是真實市場狀態，不應 fallback。
        #            _last_precise_p 來自 inverse_funding(official_rate)（TWAP 反推邊界），
        #            若 instant_premium=0 時 fallback 到它，會讓 forecast_p ≈ implied_twap，
        #            導致預測永遠等於即時費率，溢價歸零的收斂效果完全失效。
        # ── 穩定模式偵測 (Stable Mode Detection) ──
        # 1. 方差過高 (stddev > 0.3% => variance > 9e-6)
        # 2. 結算窗口後期 (Bybit/Bitget e > 70% window) - 避免雜訊放大
        # 3. 溢價指數恆零 (Feed-Failure)
        recent_history = list(self._premium_history)[-min(20, history_count):]
        if len(recent_history) >= 5:
            h_mean = sum(recent_history) / len(recent_history)
            h_var = sum((v - h_mean) ** 2 for v in recent_history) / len(recent_history)
        else:
            h_var = 0.0

        high_volatility = (h_var > 0.000009) # stddev > 0.3%
        is_noisy_window = (self.exchange in ("bybit", "bitget") and e > self.window_minutes * 0.7)
        at_cap = (official_rate != 0 and abs(official_rate) >= self.cap * 0.999)
        # 注意：at_cap 不再觸發穩定模式。
        # 舊邏輯會在貼 cap 時強制 forecast_p = inverse_funding(official)，
        # 造成「即時溢價 = 0 但預測仍停在 cap」的不一致（與 rate_if_zero 對不上）。
        # at_cap 只保留用於 implied_twap 修正（行 440）與 elapsed_twap 反推（行 537）。
        use_stable_mode = self._zero_premium_index

        impact_ready = ticker.get("impact_premium_ready", False)
        if use_stable_mode and official_rate != 0:
            # 穩定模式：對於高波動或雜訊幣種，不嘗試用即時溢價投射未來，
            # 而是假設「未來溢價均值 = 歷史溢價均值 (implied_twap)」。
            # 這會使 predicted_twap = (implied_twap * e + implied_twap * r) / W = implied_twap，
            # 即預測結算費率收斂到當前官方即時顯示費率（Flat 策略）。
            forecast_p = self.inverse_funding(official_rate)
        elif self.exchange == "gateio" and impact_ready:
            forecast_p = instant_premium  # order book 精確值（可以是 0，0 是合法值）
        elif self.exchange == "gateio" and self._last_precise_p is not None:
            forecast_p = self._last_precise_p  # fallback: kline close
        elif self.exchange == "binance":
            # 優先：tick 均值（最近 0.5s 採樣，最即時，與 forecast_next_p 一致）
            # 退回：instant_premium（非零時）
            # 最後：_last_precise_p（kline close，防止深度瞬間空洞時用 0 填充）
            _ticks = self._current_minute_ticks
            if _ticks:
                forecast_p = sum(_ticks) / len(_ticks)
            elif instant_premium != 0:
                forecast_p = instant_premium
            else:
                forecast_p = self._last_precise_p or 0
        else:
            # Bybit/Bitget：instant_premium 在 orderbook 就緒後已被覆蓋為 impact premium
            # 0 是合法值（mark = index），不做 fallback
            forecast_p = instant_premium

        if official_rate != 0:
            implied_twap = self.inverse_funding(official_rate)

            # ── 貼 cap 修正 ──────────────────────────────────────────────────
            # inverse_funding 在 official_rate = ±cap 時，只能算出「剛好觸 cap 的邊界值」
            # 當 official_rate 貼 cap 且 kline history 比邊界值更負時，
            # 用 history SMA 取代邊界值，讓預測在 cap 維持更長時間（更接近實際）。
            if history_count > 0 and abs(official_rate) >= self.cap * 0.999:
                if official_rate < 0 and twap < implied_twap:
                    implied_twap = twap
                elif official_rate > 0 and twap > implied_twap:
                    implied_twap = twap

            # 預測結算：implied TWAP 覆蓋已過時間 + 即時溢價填充剩餘時間
            # 加權模式下使用算術級數權重投射，等權模式下退化為 SMA
            predicted_twap = self._project_twap(implied_twap, forecast_p, e)
            predicted_rate = self.funding_from_premium(predicted_twap)

            # ── 下一分鐘費率 ─────────────────────────────────────────────────
            # forecast_next_p：預測 Binance 下一次採樣的分鐘均值
            tick_count = len(self._current_minute_ticks)
            last_50s = self._current_minute_ticks
            use_ticks = len(last_50s) >= 20

            # ── 噪聲與趨勢預測 ──────────────────────────────────────────────
            if self._zero_premium_index:
                forecast_next_p = implied_twap
                var_ratio = 0.0
            elif use_ticks:
                _tick_avg = sum(last_50s) / len(last_50s)
                forecast_next_p = _tick_avg
                var_ratio = 0.0
            
            if not self._zero_premium_index and not use_ticks:
                if history_count >= 2:
                    # 取最近 10 分鐘做局部分析
                    recent = list(self._premium_history)[-min(10, history_count):]
                    n = len(recent)
                    y_m = sum(recent) / n
                    # 方差基準值：0.00001 (0.1% stddev)
                    variance = sum((v - y_m) ** 2 for v in recent) / n
                    
                    # ── 趨勢計算 ──
                    x_m = (n - 1) / 2.0
                    numer = sum((i - x_m) * (recent[i] - y_m) for i in range(n))
                    denom = sum((i - x_m) ** 2 for i in range(n))
                    slope = numer / denom if denom != 0 else 0.0
                    
                    # ── 阻尼與收斂權重 (Damping & Convergence) ──
                    # 邏輯：方差越大，代表當前 P_last 越不可信（可能是噪聲），應向均值 y_m 收斂。
                    # var_ratio: 0 代表極度穩定，1 代表標準噪聲 (0.1%)，>10 代表極端噪聲 (HOOK 類)
                    var_ratio = variance / 0.00001
                    
                    # 1. 趨勢阻尼 (Damping)：噪聲越大，趨勢斜率越不可信
                    damping = 1.0 / (1.0 + var_ratio ** 2)
                    # 2. 末端權重 (Confidence)：噪聲越大，越不信任 recent[-1]，越信任 y_m
                    # conf = 1.0 (穩定時 100% 信任末端), conf -> 0 (噪聲時 100% 信任均值)
                    conf = 1.0 / (1.0 + var_ratio)
                    
                    # 針對 GateIO 這種短窗口，施加額外的保守係數
                    if self.exchange == "gateio":
                        damping *= 0.3
                        conf *= 0.5
                    
                    # 核心優化：不再直接用 recent[-1]，而是用自信度加權的基礎值 + 阻尼後的趨勢
                    base_p = (recent[-1] * conf) + (y_m * (1 - conf))
                    forecast_next_p = base_p + slope * damping
                elif history_count > 0:
                    forecast_next_p = list(self._premium_history)[-1]
                    var_ratio = 0.0
                else:
                    forecast_next_p = implied_twap
                    var_ratio = 0.0

            # ── 權重衰減下限 (Weight Floor / Kalman 啟發) ──────────────────
            # 當 e 很大時，1/(e+1) 會使預測對最新分鐘變得極度遲鈍。
            # 對於高噪聲幣種 (var_ratio > 10.0)，我們偏好「平滑」，使用真實 e（最大化分母作用）。
            # 對於穩定幣種 (var_ratio < 1.0)，我們偏好「靈敏」，使用封頂後的 e_calc（保持靈活度）。
            if var_ratio > 10.0:
                e_calc = e  # 高噪聲：回歸真實 e，最大化歷史數據的抑制噪聲作用
            else:
                # 穩定幣種：保持 responsiveness，防止預測僵化
                e_calc = min(60, e) if self.exchange == "gateio" else min(120, e)

            # 使用優化後的 e_calc 進行下一分鐘預測
            next_twap = (implied_twap * e_calc + forecast_next_p) / (e_calc + 1)
            next_minute_rate = self.funding_from_premium(next_twap)

        else:
            # 剛結算後 official_rate = 0，回退用 klines 歷史
            tick_count = 0
            var_ratio = h_var / 0.00001 if h_var > 0 else 0.0  # 與主分支相同基準
            # 以 history 的（加權/等權）TWAP 為 past_twap，再投射到整個窗口
            past_twap_fallback = get_twap(history) if history_count > 0 else forecast_p
            predicted_twap = self._project_twap(past_twap_fallback, forecast_p, e)
            predicted_rate = self.funding_from_premium(predicted_twap)
            next_vals = (history[1:] if history_count >= self.window_minutes else history) + [forecast_p]
            next_twap = get_twap(next_vals)
            next_minute_rate = self.funding_from_premium(next_twap)

        klines_unreliable = False  # 保留欄位供前端顯示，已不影響預測邏輯

        # ── 反推：達到目標費率，剩餘時間需維持的平均 premium ──
        # 貼 cap 時 official_rate 已被截斷，無法反推真實累積 TWAP，改用實際 twap
        elapsed_twap = twap if at_cap else self.inverse_funding(official_rate)

        def needed_premium_for(target_rate: float) -> Optional[float]:
            if official_rate == 0:
                return None
            target_twap_val = self.inverse_funding(target_rate)
            # 加權模式下用算術級數權重反推，等權模式下退化為 SMA 反推
            return self._needed_future_p(target_twap_val, elapsed_twap, e)

        needed_for_pos010 = needed_premium_for(0.01)          # +1.0%
        needed_for_cap = needed_premium_for(self.cap)         # 幣種費率上限
        needed_for_neg_cap = needed_premium_for(-self.cap)    # 幣種費率下限

        # 若剩餘時間溢價維持 0，結算費率會是多少
        if official_rate != 0 and e > 0:
            zero_twap = self._project_twap(elapsed_twap, 0.0, e)
            rate_if_zero = self.funding_from_premium(zero_twap)
        else:
            rate_if_zero = None

        # 即時費率（不用 TWAP，直接用 instant premium 套公式，供參考）
        instant_rate = self.funding_from_premium(instant_premium)

        return {
            "exchange": self.exchange,
            "window_minutes": self.window_minutes,
            "history_count": history_count,
            # 價格
            "mark_price": round(mark, 6),
            "index_price": round(index, 6),
            "bid1": round(bid1, 6),
            "ask1": round(ask1, 6),
            # 溢價
            "instant_premium_pct": round(instant_premium * 100, 6),
            "twap_premium_pct": round(twap * 100, 6),
            # 費率
            "instant_rate_pct": round(instant_rate * 100, 6),   # 即時 premium → 費率（非結算值）
            "calc_rate_pct": round(calc_rate * 100, 6),          # TWAP → 費率（最接近結算）
            "official_rate_pct": round(official_rate * 100, 6),  # 交易所顯示（上期結算值）
            # 下一分鐘採樣後預測（驗證用）
            "next_minute_twap_pct": round(next_twap * 100, 6),
            "next_minute_rate_pct": round(next_minute_rate * 100, 6),
            "tick_count": tick_count,  # 本分鐘已蒐集樣本數（120=全滿）
            # 結算預測（forecast_p = 用於填充未來的精確 P）
            "forecast_p_pct": round(forecast_p * 100, 6),
            "predicted_twap_pct": round(predicted_twap * 100, 6),
            "predicted_rate_pct": round(predicted_rate * 100, 6),
            "klines_unreliable": klines_unreliable,  # True = klines 不可信，預測改用 official_rate
            "var_ratio": round(var_ratio, 2),         # 噪聲比率：0=穩定, 1=標準噪聲(0.1%), >10=高噪聲
            "high_volatility": high_volatility,       # 近期 premium stddev > 0.3%
            "is_noisy_window": is_noisy_window,       # Bybit/Bitget 窗口後期，p_inferred 雜訊放大
            # 時間
            "remain_sec": int(remain_sec),
            "remain_min": round(remain_min, 1),
            "elapsed_min": round(elapsed_min, 1),
            # 反推目標 premium
            "needed_for_pos010_pct": round(needed_for_pos010 * 100, 6) if needed_for_pos010 is not None else None,
            "needed_for_cap_pct": round(needed_for_cap * 100, 6) if needed_for_cap is not None else None,
            "needed_for_neg_cap_pct": round(needed_for_neg_cap * 100, 6) if needed_for_neg_cap is not None else None,
            "rate_if_zero_pct": round(rate_if_zero * 100, 6) if rate_if_zero is not None else None,
            # 公式參數
            "interest_rate_pct": round(self.interest_rate * 100, 4),
            "clamp_inner_pct": round(self.clamp_inner * 100, 4),   # δ（clamp 邊界）
            "result_scale": round(self.result_scale, 6),           # N/8（1.0 表示不縮放）
            "cap_pct": round(self.cap * 100, 4),
            # 報價失靈偵測：premium_index 全零且費率非 cap（代表 exchange 報價來源斷線）
            "premium_feed_down": (
                self._zero_premium_index
                and official_rate != 0
                and abs(official_rate) < self.cap * 0.999
            ),
            # Bybit WS 費率缺失：last_funding_rate 持續為 0 → TWAP 歷史停止更新 → 預測不可信
            "funding_rate_feed_down": self._funding_rate_missing,
            "ts": now,
        }

    def get_chart_data(self) -> list:
        """回傳前端圖表用的 premium 歷史（最近 60 分鐘）"""
        return list(self._chart_history)
