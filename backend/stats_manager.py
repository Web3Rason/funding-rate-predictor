# -*- coding: utf-8 -*-
"""
預測準確度統計管理器
- 每分鐘記錄 PRE-SAMPLE 預測值 vs 實際 official_rate
- 計算 MAE / Bias / RMSE
- 提供 bias correction 供 FundingEngine 自動修正
"""
import asyncio
import json
import logging
import time
from pathlib import Path

logger = logging.getLogger("StatsManager")

LOGS_DIR = Path(__file__).parent.parent / "logs"
MAX_RECORDS = 1000      # 每個幣種最多保留 1000 筆
MIN_FOR_CORRECTION = 10 # 至少 10 筆才啟用 bias correction
CORRECTION_WINDOW = 60  # 計算 bias 用最近 60 筆（加權平均）
CORRECTION_CAP = 0.5    # bias correction 不超過近期 MAE 的 50%


class StatsManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._cache: dict[str, list] = {}  # symbol → records list（記憶體快取）

    # ─── 持久化 ────────────────────────────────────────

    def _path(self, symbol: str) -> Path:
        safe = symbol.replace(':', '_')
        return LOGS_DIR / f"{safe}_stats.json"

    def _load_sync(self, symbol: str) -> list:
        """同步讀取（初始化用）"""
        p = self._path(symbol)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                return []
        return []

    async def _save(self, symbol: str, records: list):
        """非同步寫入 JSON"""
        p = self._path(symbol)
        try:
            p.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            logger.warning(f"[Stats] 寫入失敗 {symbol}: {e}")

    def _ensure_loaded(self, symbol: str):
        if symbol not in self._cache:
            self._cache[symbol] = self._load_sync(symbol)

    # ─── 記錄 ──────────────────────────────────────────

    async def record(self, symbol: str, predicted_pct: float, actual_pct: float,
                     ts: float = None, meta: dict = None) -> float:
        """
        記錄一筆預測 vs 實際。回傳誤差（actual - predicted）。
        """
        async with self._lock:
            self._ensure_loaded(symbol)
            error = actual_pct - predicted_pct
            entry = {
                "ts": ts or time.time(),
                "predicted": round(predicted_pct, 6),
                "actual": round(actual_pct, 6),
                "error": round(error, 6),
            }
            if meta:
                entry.update(meta)
            records = self._cache[symbol]
            records.append(entry)
            if len(records) > MAX_RECORDS:
                self._cache[symbol] = records[-MAX_RECORDS:]
            await self._save(symbol, self._cache[symbol])
            logger.info(
                f"[Stats] {symbol} 預測={predicted_pct:.5f}%  實際={actual_pct:.5f}%  "
                f"誤差={error:+.5f}%  (共{len(self._cache[symbol])}筆)"
            )
            return error

    # ─── 統計 ──────────────────────────────────────────

    def get_stats(self, symbol: str, n_recent: int = 15) -> dict:
        """回傳統計數據（同步，供 API 使用）"""
        self._ensure_loaded(symbol)
        records = self._cache.get(symbol, [])
        count = len(records)
        if count == 0:
            return {"count": 0, "mae": None, "bias": None, "rmse": None, "recent": []}

        errors = [r["error"] for r in records]
        mae = sum(abs(e) for e in errors) / count
        bias = sum(errors) / count
        rmse = (sum(e ** 2 for e in errors) / count) ** 0.5

        # 近期 N 筆詳細記錄（倒序，最新在前）
        # 只取最近 24h 內的紀錄，避免跨日舊數據混入表格
        cutoff = time.time() - 86400
        recent_all = [r for r in records if r.get("ts", 0) >= cutoff]
        recent = list(reversed(recent_all[-n_recent:] if recent_all else records[-n_recent:]))

        return {
            "count": count,
            "mae": round(mae, 6),
            "bias": round(bias, 6),
            "rmse": round(rmse, 6),
            "recent": recent,
        }

    # ─── 自動 Bias Correction ─────────────────────────

    def get_bias_correction(self, symbol: str) -> float:
        """
        回傳應加到預測值的修正量（pct 單位）。

        算法：
          最近 CORRECTION_WINDOW 筆誤差的加權平均（越近權重越高）
          → 修正 = -bias（若預測偏高，下次調低）
          → 上限：近期 MAE 的 CORRECTION_CAP 倍（防止過度修正）
          → 樣本 < MIN_FOR_CORRECTION 時回傳 0（不修正）
        """
        self._ensure_loaded(symbol)
        records = self._cache.get(symbol, [])
        if len(records) < MIN_FOR_CORRECTION:
            return 0.0

        tail = records[-CORRECTION_WINDOW:]
        n = len(tail)
        # 線性遞增權重（最新 = 最高）
        weights = list(range(1, n + 1))
        total_w = sum(weights)
        bias = sum(w * r["error"] for w, r in zip(weights, tail)) / total_w

        # 計算近期 MAE 作為 cap 基準
        mae = sum(abs(r["error"]) for r in tail) / n
        max_corr = mae * CORRECTION_CAP
        # correction = -bias（反向補償）
        correction = max(-max_corr, min(max_corr, -bias))
        return correction
