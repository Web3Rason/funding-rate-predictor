# -*- coding: utf-8 -*-
"""
5021 預測自我改進監控器
- 每 5 分鐘讀取 stats JSON，計算滾動誤差指標
- 偵測系統性問題（tick 噪音 / kline 汙染 / 窗口偏移）
- 觸發針對性修正並重啟 backend
- 所有決策記錄到 logs/self_improve.log
"""
import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

HERE = Path(__file__).parent
BACKEND_DIR = HERE / "backend"
LOGS_DIR = HERE / "logs"
ENGINE_PATH = BACKEND_DIR / "funding_engine.py"
LOG_PATH = LOGS_DIR / "self_improve.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SelfImprove] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_PATH), encoding="utf-8"),
    ],
)
log = logging.getLogger("SI")

CHECK_INTERVAL = 300          # 每 5 分鐘檢查一次
DEEP_ANALYSIS_INTERVAL = 1800 # 每 30 分鐘深度分析
MIN_SAMPLES = 6               # 最少 6 筆才分析
MAE_GOOD = 0.003              # MAE < 0.003% → 優秀，不動
MAE_WARN = 0.008              # MAE 0.003–0.008% → 觀察
MAE_BAD  = 0.015              # MAE > 0.015% → 觸發修正

# ─── 讀取 Stats ──────────────────────────────────────────────────

def load_stats(symbol: str) -> list:
    p = LOGS_DIR / f"{symbol}_stats.json"
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []

def get_symbol_from_log() -> str:
    """從 app.log 最新行偵測當前監控幣種"""
    app_log = LOGS_DIR / "app.log"
    if not app_log.exists():
        return "BTCUSDT"
    try:
        with open(app_log, "rb") as f:
            f.seek(max(0, os.fstat(f.fileno()).st_size - 4096))
            lines = f.read().decode("utf-8", errors="replace").splitlines()
        for line in reversed(lines):
            m = re.search(r"\[binance\] (\w+USDT) 窗口", line)
            if m:
                return m.group(1)
    except Exception:
        pass
    return "BTCUSDT"

# ─── 統計計算 ────────────────────────────────────────────────────

def compute_metrics(records: list, window: int = 20) -> dict:
    if not records:
        return {}
    tail = records[-window:]
    errors = [r["error"] for r in tail]
    abs_errors = [abs(e) for e in errors]
    n = len(errors)
    mae = sum(abs_errors) / n
    bias = sum(errors) / n
    # 趨勢：最近 5 筆 vs 前 5 筆 MAE 比較
    trend = None
    if n >= 10:
        recent_mae = sum(abs(e) for e in errors[-5:]) / 5
        older_mae = sum(abs(e) for e in errors[:5]) / 5
        trend = "improving" if recent_mae < older_mae * 0.8 else \
                "worsening" if recent_mae > older_mae * 1.3 else "stable"
    # tick count 分析（有 meta 的記錄）
    high_tick = [r for r in tail if r.get("tick_count", 0) >= 50]
    low_tick  = [r for r in tail if r.get("tick_count",  0) <  50]
    high_tick_mae = sum(abs(r["error"]) for r in high_tick) / len(high_tick) if high_tick else None
    low_tick_mae  = sum(abs(r["error"]) for r in low_tick)  / len(low_tick)  if low_tick  else None
    return {
        "n": n,
        "mae": mae,
        "bias": bias,
        "trend": trend,
        "high_tick_mae": high_tick_mae,
        "low_tick_mae": low_tick_mae,
    }

# ─── Engine 參數讀取 ─────────────────────────────────────────────

def read_threshold() -> float:
    """從 funding_engine.py 讀取大費率幣(extreme_coin)的 noise_threshold"""
    txt = ENGINE_PATH.read_text(encoding="utf-8")
    # 新格式：noise_threshold = 1.2 if extreme_coin else 3.0
    m = re.search(r"noise_threshold = ([\d.]+) if extreme_coin", txt)
    if m:
        return float(m.group(1))
    # 舊格式 fallback
    m = re.search(r"abs\(tick_avg\) > abs\(history\[-1\]\) \* ([\d.]+)", txt)
    return float(m.group(1)) if m else 3.0

def write_threshold(new_val: float):
    """更新 funding_engine.py 的 extreme_coin noise_threshold"""
    txt = ENGINE_PATH.read_text(encoding="utf-8")
    new_txt = re.sub(
        r"(noise_threshold = )[\d.]+(  if extreme_coin)",
        lambda m: f"{m.group(1)}{new_val}{m.group(2)}",
        txt,
    )
    # 確保格式正確（空格數可能不同）
    if new_txt == txt:
        new_txt = re.sub(
            r"(noise_threshold = )[\d.]+( if extreme_coin)",
            lambda m: f"{m.group(1)}{new_val}{m.group(2)}",
            txt,
        )
    if new_txt != txt:
        ENGINE_PATH.write_text(new_txt, encoding="utf-8")
        log.info(f"  → ENGINE 已更新 extreme_coin noise_threshold: {new_val}")
        return True
    return False

# ─── Backend 重啟 ────────────────────────────────────────────────

def _kill_port(port: int) -> None:
    """中止佔用指定 port 的 process。找不到就什麼都不做。"""
    try:
        if os.name == "nt":
            out = subprocess.run(
                ["netstat", "-ano"], capture_output=True, text=True, timeout=10
            ).stdout
            pids = {
                line.split()[-1]
                for line in out.splitlines()
                if f":{port} " in line and "LISTENING" in line
            }
            for pid in pids:
                subprocess.run(["taskkill", "/F", "/PID", pid],
                               capture_output=True, timeout=10)
        else:
            out = subprocess.run(
                ["lsof", "-ti", f":{port}"], capture_output=True, text=True, timeout=10
            ).stdout
            for pid in out.split():
                subprocess.run(["kill", "-9", pid], capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        log.warning("  → 無法自動釋放 port %s，請自行確認", port)


def restart_backend():
    """殺掉 3021 port 的 process，重啟 backend"""
    log.info("  → 重啟 backend...")
    # 找出佔用 3021 的 process 並中止（跨平台，不依賴外部腳本）
    _kill_port(3021)
    time.sleep(2)
    # 重啟
    subprocess.Popen(
        [sys.executable, "main.py"],
        cwd=str(BACKEND_DIR),
        creationflags=0x08000000,  # CREATE_NO_WINDOW
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(3)
    log.info("  → Backend 已重啟")

# ─── 決策邏輯 ────────────────────────────────────────────────────

last_threshold_change = 0
last_restart_time = 0
RESTART_COOLDOWN = 600  # 10 分鐘內不重複重啟

def decide_and_act(symbol: str, records: list, metrics: dict, deep: bool = False):
    global last_threshold_change, last_restart_time

    if not metrics:
        return
    mae  = metrics["mae"]
    bias = metrics["bias"]
    n    = metrics["n"]

    log.info(f"[{symbol}] n={n}  MAE={mae*100:.4f}bp  Bias={bias:+.5f}%  trend={metrics.get('trend','?')}")

    # ── 1. MAE 已達優秀：不動 ────────────────────────────────
    if mae < MAE_GOOD:
        log.info(f"  ✓ 優秀（MAE < {MAE_GOOD*100:.1f}bp）")
        return

    # ── 2. Bias 修正（由 stats_manager 自動處理，只紀錄）────
    if abs(bias) > 0.002:
        direction = "高估" if bias < 0 else "低估"
        log.info(f"  ⚠ 系統性{direction} bias={bias:+.5f}%（stats_manager 自動修正中）")

    # ── 3. Tick 噪音診斷（deep analysis）────────────────────
    if deep and metrics.get("high_tick_mae") is not None:
        ht = metrics["high_tick_mae"]
        lt = metrics.get("low_tick_mae") or ht
        log.info(f"  [tick分析] 高tick MAE={ht:.5f}%  低tick MAE={lt:.5f}%")
        thresh = read_threshold()
        # 若高 tick 樣本的 MAE 仍很差，且閾值還有調降空間
        if ht > MAE_BAD and thresh > 1.5 and time.time() - last_threshold_change > 1800:
            new_thresh = max(1.5, round(thresh - 0.5, 1))
            log.info(f"  ⚡ tick 高頻仍有大誤差 → 降低閾值 {thresh} → {new_thresh}")
            if write_threshold(new_thresh):
                last_threshold_change = time.time()
                if time.time() - last_restart_time > RESTART_COOLDOWN:
                    restart_backend()
                    last_restart_time = time.time()
        # 若高 tick 誤差改善，且閾值被降得太低 → 放寬
        elif ht < MAE_WARN and thresh < 3.0 and time.time() - last_threshold_change > 1800:
            new_thresh = min(3.0, round(thresh + 0.5, 1))
            log.info(f"  ✓ tick 高頻改善 → 放寬閾值 {thresh} → {new_thresh}")
            if write_threshold(new_thresh):
                last_threshold_change = time.time()
                if time.time() - last_restart_time > RESTART_COOLDOWN:
                    restart_backend()
                    last_restart_time = time.time()

    # ── 4. 趨勢惡化告警 ─────────────────────────────────────
    if deep and metrics.get("trend") == "worsening" and mae > MAE_WARN:
        log.info(f"  ⚠ 誤差趨勢惡化，需人工介入（可能是幣種切換 / 結算後 kline 汙染）")

# ─── 主監控循環 ──────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("5021 自我改進監控啟動")
    log.info("  監控邏輯：5min 輕量 / 30min 深度分析 + 參數調整")
    log.info("  MAE 目標：< 0.003%（3bp）")
    log.info("=" * 60)

    last_deep = 0
    cycle = 0

    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        cycle += 1
        now = time.time()
        deep = (now - last_deep) >= DEEP_ANALYSIS_INTERVAL

        symbol = get_symbol_from_log()
        records = load_stats(symbol)

        if len(records) < MIN_SAMPLES:
            log.info(f"[{symbol}] 樣本不足（{len(records)}/{MIN_SAMPLES}），等待累積...")
            continue

        metrics = compute_metrics(records)
        decide_and_act(symbol, records, metrics, deep=deep)

        if deep:
            last_deep = now
            # 深度分析：印出最近 5 筆詳情
            log.info(f"  [最近5筆]")
            for r in records[-5:]:
                ts = datetime.fromtimestamp(r["ts"]).strftime("%H:%M:%S")
                tc = r.get("tick_count", "?")
                log.info(f"    {ts} pred={r['predicted']:+.5f}% actual={r['actual']:+.5f}% err={r['error']:+.5f}% ticks={tc}")

        log.info(f"  [cycle #{cycle}] 下次檢查 {CHECK_INTERVAL//60} 分鐘後")


if __name__ == "__main__":
    asyncio.run(main())
