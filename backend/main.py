# -*- coding: utf-8 -*-
"""
5021_FundingRate-Predictor 後端
FastAPI (port 3021)
支援 Binance / Bybit / Bitget / Gate.io，多幣種同時監控

架構：每個幣種獨立 WS + TWAP history + 預測任務
- _pair_ticker_loop: 每 0.5s 同步 ticker，並處理結算重置
- _pair_sample: WS 偵測到 official_rate 跳變時精確採樣 Premium
- _pair_prediction_loop: 採樣前 1 秒觸發預測日誌
"""
import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from exchange_client import ExchangeClient
from funding_engine import FundingEngine
from stats_manager import StatsManager

# ─── Logging ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "..", "logs", "app.log"),
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("5021")

# ─── Config ───────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent.parent / "config.json"
config = {"exchange": "binance", "symbol": "BTCUSDT", "poll_interval": 5}
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config.update(json.load(f))

MONITORS_PATH = Path(__file__).parent.parent / "monitors.json"

def _save_monitors():
    """將目前監控列表寫入 monitors.json（重啟後恢復用）"""
    data = {
        "pairs": [
            {"exchange": p["exchange"], "symbol": p["symbol"]}
            for p in active_monitors.values()
        ],
        "selected_key": selected_key,
    }
    try:
        MONITORS_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning(f"[monitors] 儲存失敗: {e}")

# ─── 全域狀態 ──────────────────────────────────────────
# key = f"{exchange}:{symbol}"（小寫 exchange，大寫 symbol）
active_monitors: Dict[str, dict] = {}
selected_key: str = ""
_monitor_lock = asyncio.Lock()
stats_manager: StatsManager = StatsManager()

SUPPORTED_EXCHANGES = ("binance", "bybit", "bitget", "gateio", "aster", "okx")
SYMBOL_SUFFIXES = ('USDT', 'BUSD', 'USDC', 'BTC', 'ETH')


def _pair_key(exchange: str, symbol: str) -> str:
    return f"{exchange.lower()}:{symbol.upper()}"


# ─── 預填 history ─────────────────────────────────────
async def _preload_history(client: ExchangeClient, engine: FundingEngine):
    try:
        ticker = await client.get_ticker()
        next_ft_ms = ticker.get("next_funding_time", 0)
        start_ms = None
        if next_ft_ms:
            start_ms = next_ft_ms - client.window_minutes * 60 * 1000
        klines = await client.get_klines_history(limit=client.window_minutes, start_ms=start_ms)
        if klines:
            engine.preload_history(klines)
            logger.info(f"[preload] 預填 {len(klines)} 筆 klines 完成")
        else:
            logger.warning("[preload] klines 空資料，TWAP 從即時累積")
    except Exception as e:
        logger.warning(f"[preload] 失敗（TWAP 從零開始）: {e}")


# ─── 每個 pair 的採樣回調 ──────────────────────────────
async def _pair_sample(key: str, new_r: float, ts: float, prev_r: float = None):
    """
    WS 偵測到 official_rate 跳變時精確採樣 Premium（per-pair 版）。
    數學原理同原 on_ws_sample：
      T_new = inverse_F(new_r)，T_prev = inverse_F(prev_r)
      若窗口未滿（n < W）：p_actual = T_new × (n+1) − T_prev × n
      若窗口已滿（n = W）：p_actual = W × (T_new − T_prev) + history[0]
    """
    pair = active_monitors.get(key)
    if not pair:
        return

    engine: FundingEngine = pair["engine"]
    client: ExchangeClient = pair["client"]
    ticker = pair.get("ticker", {})

    current_minute = int(ts // 60)
    if current_minute == pair["last_sample_minute"]:
        return  # 同分鐘防重複

    p = ticker.get("instant_premium", 0)
    inferred = False

    # ── 優先：從費率跳變反推真實分鐘均值 ─────────────────
    if (
        prev_r is not None
        and new_r != 0                                    # new_r=0 = 結算重置，不可推算
        and engine
        and abs(new_r) < engine.cap * 0.999              # new_r 未貼 cap
        and abs(prev_r) < engine.cap * 0.999             # prev_r 未貼 cap
    ):
        T_new = engine.inverse_funding(new_r)
        T_prev = engine.inverse_funding(prev_r)
        n = len(engine._premium_history)
        W = engine.window_minutes
        if n == 0:
            p_inferred = T_new
        elif engine.is_rolling and n >= W:
            # Binance 滾動窗口：最舊一筆被推出，T_new = (T_prev*W - p_old + p_inferred) / W
            p_old = engine._premium_history[0]
            p_inferred = W * (T_new - T_prev) + p_old
        else:
            # 固定窗口（Bybit/Bitget/Gate.io）或窗口尚未填滿：累積公式
            # T_new = (T_prev*n + p_inferred) / (n+1)  →  p_inferred = T_new*(n+1) - T_prev*n
            p_inferred = T_new * (n + 1) - T_prev * n

        ticks = engine._current_minute_ticks if engine else []
        tick_avg = sum(ticks) / len(ticks) if ticks else 0.0
        instant_p_at_sample = ticker.get("instant_premium", 0)
        idx_price = ticker.get("index_price", 0)
        history_n = len(engine._premium_history)
        imp_bid = getattr(client, "_last_impact_bid", None)
        imp_ask = getattr(client, "_last_impact_ask", None)
        imp_str = ""
        if imp_bid is not None and imp_ask is not None and idx_price > 0:
            imp_str = (
                f"  ImpactBid={imp_bid:.6f}({(imp_bid/idx_price-1)*100:.4f}%)"
                f"  ImpactAsk={imp_ask:.6f}({(imp_ask/idx_price-1)*100:.4f}%)"
                f"  idx={idx_price:.6f}"
            )
        logger.info(
            f"[WS-SAMPLE-COMPARE][{key}] "
            f"tick_avg={tick_avg*100:.6f}% (ticks={len(ticks)})  "
            f"instant_P={instant_p_at_sample*100:.6f}%  "
            f"inferred_P={p_inferred*100:.6f}%  "
            f"tick_vs_inferred={(tick_avg - p_inferred)*100:.6f}%  "
            f"history_n={history_n}  r: {prev_r*100:.6f}%→{new_r*100:.6f}%"
            f"{imp_str}"
        )
        inferred = True
        # ── Bybit / Binance：優先取官方 premium kline（與交易所 TWAP 同源），失敗才回退 p_inferred ──
        # p_inferred 在穩定態剛退出時容易失真（歷史被 inverse_F 污染），kline close 更可靠
        if client.exchange in ("bybit", "binance"):
            kline_p = await client.get_latest_premium_close()
            if kline_p is not None:
                p = kline_p
                logger.info(
                    f"[WS-SAMPLE][{key}] {client.exchange}官方kline P={p*100:.6f}% "
                    f"(p_inferred={p_inferred*100:.6f}%)  r: {prev_r*100:.6f}%→{new_r*100:.6f}%"
                )
            else:
                p = p_inferred
                logger.info(f"[WS-SAMPLE][{key}] 推算 P={p*100:.6f}%  r: {prev_r*100:.6f}% → {new_r*100:.6f}%")
        else:
            p = p_inferred
            logger.info(f"[WS-SAMPLE][{key}] 推算 P={p*100:.6f}%  r: {prev_r*100:.6f}% → {new_r*100:.6f}%")

        # ── 報價恢復偵測：費率跳變代表 premium_index ≠ 0，清除失靈 flag ──
        if engine._zero_premium_index:
            engine._zero_premium_index = False
            logger.info(f"[FeedRecovery][{key}] 費率跳變偵測，報價已恢復，退出失靈模式")

        # IMN 動態校正已移除：各交易所現已透過靜態 API 取得準確 IMN
        # Binance: leverageBracket (200 × initialLeverage)
        # Bybit:   risk-limit (200 / initialMargin)
        # Bitget:  query-position-lever (200 / keepMarginRate)
        # Gate.io: funding_impact_value (USDT 名義金額, 直接從 API)
        pass
    else:
        # ── 無法推算時退回 ────────────────────────────────
        if new_r == 0:
            # 結算後期初（new_r=0），不採樣：此時 official_rate 尚未建立，記錄會造成假誤差
            logger.debug(f"[WS-SAMPLE][{key}] r_new=0，跳過採樣")
            return
        # new_r != 0（無論是否貼 cap）：
        # - 穩定態（prev_r=None）：inverse_F 是最佳 TWAP 估計
        # - 貼 cap：優先取實際 premiumIndex kline close（Bybit/Binance），
        #   能反映真實溢價（可能遠低於 -2.525%），使 TWAP 顯示更準確；
        #   fallback 為 inverse_F(cap) 保持 TWAP 一致性
        p = engine.inverse_funding(new_r)
        at_cap = abs(new_r) >= engine.cap * 0.999
        if at_cap:
            kline_p = await client.get_latest_premium_close()
            if kline_p is not None:
                p = kline_p
                logger.info(
                    f"[WS-SAMPLE][{key}] 貼cap kline P={p*100:.6f}%  r={new_r*100:.6f}%"
                )
            else:
                logger.info(
                    f"[WS-SAMPLE][{key}] 貼cap P=inverse_F({new_r*100:.6f}%)={p*100:.6f}%"
                )
        else:
            logger.info(
                f"[WS-SAMPLE][{key}] 穩定態 P=inverse_F({new_r*100:.6f}%)={p*100:.6f}%"
            )

    if engine:
        if pair["last_sample_minute"] == current_minute:
            return
        engine.add_sample(p)
        pair["last_sample_minute"] = current_minute

        # 統計：比對上一分鐘預測 vs 實際費率
        pending = pair.get("pending_prediction", {})
        if pending and new_r != 0:  # new_r=0 = 結算後期初狀態，不記錄（actual=0 是假值）
            pred_ts = pending.get("ts", 0)
            if ts - pred_ts < 70:
                asyncio.create_task(stats_manager.record(
                    symbol=key,  # 用 exchange:symbol 避免跨交易所同名幣種污染統計
                    predicted_pct=pending["raw_pct"],
                    actual_pct=new_r * 100,
                    ts=ts,
                    meta={
                        "tick_count": pending.get("tick_count", 0),
                        "instant_premium_pct": pending.get("instant_premium_pct"),
                        "sample_sec": round(ts % 60, 1),  # WS 偵測到結算的秒數（分鐘內）
                    }
                ))


# ─── 每個 pair 的 ticker 更新 loop ──────────────────────
async def _pair_ticker_loop(key: str):
    """每 0.5s 同步 ticker，偵測結算，強制每分鐘採樣（fallback）"""
    pair = active_monitors.get(key)
    if not pair:
        return
    client: ExchangeClient = pair["client"]
    engine: FundingEngine = pair["engine"]
    last_next_ft = 0

    while key in active_monitors:
        try:
            if client.ws_ready:
                ticker = client._ws_cache
                if ticker:
                    new_next_ft = ticker.get("next_funding_time", 0)
                    if last_next_ft and new_next_ft != last_next_ft:
                        now_t = time.time()
                        window_sec = engine.window_minutes * 60
                        delta = new_next_ft - last_next_ft
                        is_scheduled = abs(delta - window_sec * 1000) < 60000
                        # delta > 60s：正常結算或週期變長（4h→8h）
                        # delta < -60s：週期變短中途生效（4h→1h，next_ft 提前）
                        # |delta| <= 60s：WS 抖動，忽略
                        if is_scheduled or abs(delta) > 60000:
                            direction = "縮短" if delta < 0 else ("變長" if not is_scheduled else "正常")
                            logger.info(f"[Settlement][{key}] 偵測到結算/週期變更（{direction}），重置 TWAP history")
                            new_window, new_cap = await client.refresh_settings()
                            if new_window != engine.window_minutes:
                                engine.resize_window(new_window)
                            if new_cap is not None:
                                engine.cap = new_cap
                            # 各交易所 I 是合約屬性，理論上不變；仍同步以防合約調整
                            # （refresh_settings 已重讀對應欄位）
                            if client.exchange == "gateio":
                                engine._interest_rate_override = client._gateio_base_interest
                                engine.interest_rate = client._gateio_base_interest
                            elif client.exchange == "okx" and client._okx_interest_rate is not None:
                                engine._interest_rate_override = client._okx_interest_rate
                                engine.interest_rate = client._okx_interest_rate
                            engine.reset_history()
                            asyncio.create_task(_preload_history(client, engine))
                            # Bybit：結算後 WS snapshot 可能再次送空 fundingRate，強制 REST 刷新
                            if client.exchange == "bybit":
                                asyncio.create_task(client._seed_funding_rate_from_rest(force=True))
                        else:
                            logger.warning(f"[Settlement][{key}] next_ft 異常跳變，忽略 reset")
                    last_next_ft = new_next_ft
                    pair["ticker"] = ticker
                    engine.tick(ticker.get("instant_premium", 0))


                    # 強制每分鐘採樣（fallback：費率貼 cap 時 WS 不跳變）
                    # 在偵測到採樣偏移後動態調整觸發秒數，避免 fallback 早於 WS 更新導致舊費率被記錄
                    now_t = time.time()
                    current_min = int(now_t // 60)
                    current_sec = now_t % 60
                    detected = client.sampling_offset
                    if detected is not None:
                        # 在預期更新秒數後 2 秒才觸發 fallback，讓 WS 有機會先更新
                        trigger_sec = (detected + 2) % 60
                    else:
                        trigger_sec = 2.0  # 尚未偵測到偏移時的保守預設
                    if current_min != pair["last_sample_minute"] and current_sec >= trigger_sec:
                        fr_for_fallback = ticker.get("last_funding_rate", 0)
                        # Bybit/Bitget WS 重連後 fundingRate 暫時為 0 → _pair_sample 會 skip
                        # 優先用 _last_r（上次已知費率），再用純利率確保 fallback 不被跳過
                        if not fr_for_fallback and client.exchange in ("bybit", "bitget", "okx"):
                            fr_for_fallback = client._last_r or 0.00005
                        asyncio.create_task(_pair_sample(key, fr_for_fallback, now_t))
                    # ── Bybit/Bitget FR 缺失偵測 ──
                    # WS snapshot fundingRate = "" → last_funding_rate = 0 持續 → TWAP 歷史停止更新
                    # 超過 3 分鐘仍為 0（REST 種子也無效）→ 標記 funding_rate_feed_down
                    if client.exchange in ("bybit", "bitget", "okx"):
                        fr = ticker.get("last_funding_rate", 0)
                        if fr == 0:
                            if not pair.get("_fr_missing_since"):
                                pair["_fr_missing_since"] = now_t
                            elif now_t - pair["_fr_missing_since"] > 180:
                                if not engine._funding_rate_missing:
                                    engine._funding_rate_missing = True
                                    logger.warning(
                                        f"[FRMissing][{key}] last_funding_rate=0 超過 3 分鐘，"
                                        f"TWAP 歷史停止更新，標記預測不可信"
                                    )
                        else:
                            pair["_fr_missing_since"] = None
                            if engine._funding_rate_missing:
                                engine._funding_rate_missing = False
                                logger.info(
                                    f"[FRRecovery][{key}] last_funding_rate 已恢復 "
                                    f"{fr*100:.6f}%，解除缺失標記"
                                )

                    # ── Feed 失靈偵測 / 恢復偵測（GateIO / Bitget） ──
                    # GateIO:  funding_rate_indicative 即時更新，卡在 min_rate = 報價失靈
                    # Bitget:  current-fund-rate / ticker fundingRate 卡在 min_rate 且 mark-index P 顯著
                    #          結算後 TWAP 重置會有短暫 min_rate，加 5 分鐘守衛避免假陽性
                    if client.exchange == "gateio":
                        official_rate = ticker.get("last_funding_rate", 0)
                        min_rate = getattr(client, "_gateio_min_rate", 0)
                        if min_rate > 0 and official_rate != 0:
                            at_min = abs(official_rate - min_rate) < 1e-6
                            if not engine._zero_premium_index and at_min:
                                engine._zero_premium_index = True
                                logger.info(f"[FeedCheck][{key}] FR={official_rate*100:.5f}%=min_rate，報價失靈")
                            elif engine._zero_premium_index and not at_min:
                                engine._zero_premium_index = False
                                logger.info(f"[FeedRecovery][{key}] FR={official_rate*100:.5f}%≠min_rate，報價已恢復")

                    elif client.exchange == "bitget":
                        official_rate = ticker.get("last_funding_rate", 0)
                        mark = ticker.get("mark_price", 0)
                        idx  = ticker.get("index_price", 0)
                        mark_idx_p = (mark - idx) / idx if idx > 0 else 0
                        min_rate = engine.interest_rate * engine.result_scale  # I × N/8
                        at_min = official_rate > 0 and abs(official_rate - min_rate) < 1e-6
                        has_premium = abs(mark_idx_p) > 0.001  # mark-index P > 0.1%
                        if at_min and has_premium:
                            if not pair.get("_bitget_feed_min_since"):
                                pair["_bitget_feed_min_since"] = now_t
                            elif now_t - pair["_bitget_feed_min_since"] > 300:  # 持續 5 分鐘才觸發
                                if not engine._zero_premium_index:
                                    engine._zero_premium_index = True
                                    logger.info(
                                        f"[BitgetFeed][{key}] FR={official_rate*100:.5f}%=min 持續>5min，"
                                        f"mark-idx P={mark_idx_p*100:.3f}%，報價失靈"
                                    )
                        else:
                            pair["_bitget_feed_min_since"] = None
                            if engine._zero_premium_index:
                                engine._zero_premium_index = False
                                logger.info(f"[BitgetFeed][{key}] 報價已恢復，FR={official_rate*100:.5f}%")


        except Exception as e:
            logger.warning(f"[{key}] ticker_update 異常: {e}")
        await asyncio.sleep(0.5)


# ─── 每個 pair 的預測觸發 loop ──────────────────────────
async def _pair_prediction_loop(key: str):
    """每分鐘在採樣前 1 秒觸發精確預測"""
    pair = active_monitors.get(key)
    if not pair:
        return
    client: ExchangeClient = pair["client"]
    engine: FundingEngine = pair["engine"]

    start_time = time.time()
    last_fired_min = -1
    first_detected_min = -1   # 第一次偵測到 offset 的那一分鐘，跳過不預測
    while key in active_monitors:
        try:
            await asyncio.sleep(0.5)
            now = time.time()
            current_sec = now % 60
            current_min = int(now // 60)

            detected = client.sampling_offset
            elapsed = now - start_time

            if detected is not None:
                # 記錄第一次偵測到的分鐘
                if first_detected_min == -1:
                    first_detected_min = current_min
                # 第一次偵測的當分鐘：PRE-SAMPLE 可能比 WS 晚，跳過
                if current_min == first_detected_min:
                    continue
                target_sec = (detected - 1) % 60
            elif elapsed < 60:
                # 前 60 秒：純收集資料，不預測
                continue
            else:
                # 超過 60 秒仍未偵測到跳變（費率穩定），退回交易所預設秒數
                target_sec = 45.0 if client.exchange == "gateio" else 59.0

            # 當本分鐘尚未觸發，且秒數剛越過 target_sec（0.5s 輪詢誤差內）
            if current_min != last_fired_min and current_sec >= target_sec:
                last_fired_min = current_min
                ticker = pair.get("ticker", {})
                if ticker and engine:
                    live_ticker = {**ticker, **{k: v for k, v in client._ws_cache.items()
                                                if k in ("instant_premium", "impact_premium_ready",
                                                         "mark_price", "index_price", "bid1", "ask1")}}
                    result = engine.compute(live_ticker, ticker.get("next_funding_time", 0))
                    pred = result["next_minute_rate_pct"]
                    now_ts = time.time()
                    pair["pre_sample"] = {
                        "next_minute_rate_pct": pred,
                        "instant_premium_pct": result["instant_premium_pct"],
                        "ts": now_ts,
                    }
                    pair["pending_prediction"] = {
                        "raw_pct": pred,
                        "tick_count": result.get("tick_count", 0),
                        "instant_premium_pct": result["instant_premium_pct"],
                        "ts": now_ts,
                    }
                    logger.info(
                        f"[PRE-SAMPLE][{key}] @{current_sec:.1f}s  "
                        f"target={target_sec:.1f}s  offset={detected}  "
                        f"pred={pred:.6f}%  predicted_rate={result['predicted_rate_pct']:.6f}%"
                    )
        except Exception as e:
            logger.warning(f"[{key}] prediction_trigger 異常: {e}")
            await asyncio.sleep(1)


# ─── 新增 / 移除幣種 ──────────────────────────────────────
async def _add_pair(exchange: str, symbol: str, preload_bg: bool = False) -> str:
    """建立並啟動一個幣種的監控（若已存在直接回傳 key）"""
    key = _pair_key(exchange, symbol)
    if key in active_monitors:
        return key

    api_key = config.get(f"{exchange}_api_key")
    api_secret = config.get(f"{exchange}_api_secret")
    api_passphrase = config.get(f"{exchange}_api_passphrase")
    client = ExchangeClient(exchange=exchange, symbol=symbol, api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    await client.initialize()
    # 各幣種 I 不同時用 API 真實值覆蓋 EXCHANGE_CONFIG 預設：
    # - GateIO: commodities=0、一般幣 = daily_rate / periods_per_day
    # - OKX:    BTC=0.01%、GAS=0.005%、NG/CL=0（每合約獨立，已是 per-period）
    ir_override = None
    if exchange == "gateio":
        ir_override = client._gateio_base_interest  # 新算法：8h 等效基礎利率（週期縮放由 result_scale 處理）
    elif exchange == "okx":
        ir_override = client._okx_interest_rate  # None 表示沒讀到，不覆蓋
    engine = FundingEngine(
        exchange=exchange, symbol=symbol,
        window_minutes=client.window_minutes,
        cap_override=client.cap,
        interest_rate_override=ir_override,
    )

    pair: dict = {
        "client": client,
        "engine": engine,
        "exchange": exchange,
        "symbol": symbol.upper(),
        "ticker": {},
        "pre_sample": {},
        "pending_prediction": {},
        "last_sample_minute": -1,
        "task_ticker": None,
        "task_prediction": None,
        "session_start_ts": time.time(),  # 本次監控開始時間，用於前端過濾舊統計紀錄
    }
    active_monitors[key] = pair

    # 綁定 per-pair on_sample callback（閉包鎖定 key 與 client）
    async def _bound_sample(new_r, ts, prev_r=None, _k=key, _c=client):
        p = active_monitors.get(_k)
        if not p or p["client"] is not _c:
            return  # stale callback
        await _pair_sample(_k, new_r, ts, prev_r)
    client.on_sample = _bound_sample

    if preload_bg:
        asyncio.create_task(_preload_history(client, engine))
    else:
        await _preload_history(client, engine)

    pair["task_ticker"] = asyncio.create_task(_pair_ticker_loop(key))
    pair["task_prediction"] = asyncio.create_task(_pair_prediction_loop(key))
    logger.info(f"[Monitor] 新增幣種 {key}")
    _save_monitors()
    return key


async def _remove_pair(key: str):
    """停止並移除一個幣種的監控"""
    pair = active_monitors.pop(key, None)
    if not pair:
        return
    for t in [pair.get("task_ticker"), pair.get("task_prediction")]:
        if t:
            t.cancel()
    asyncio.create_task(pair["client"].close())
    logger.info(f"[Monitor] 移除幣種 {key}")
    _save_monitors()


# ─── Lifespan ──────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global selected_key

    logger.info("=== 5021_FundingRate 啟動 ===")

    # 恢復上次監控列表
    if MONITORS_PATH.exists():
        try:
            saved = json.loads(MONITORS_PATH.read_text(encoding="utf-8"))
            pairs = saved.get("pairs", [])
            prev_selected = saved.get("selected_key", "")
            if pairs:
                logger.info(f"[monitors] 恢復 {len(pairs)} 個監控幣種")
                for item in pairs:
                    try:
                        await _add_pair(item["exchange"], item["symbol"], preload_bg=True)
                    except Exception as e:
                        logger.warning(f"[monitors] 恢復 {item} 失敗: {e}")
                if prev_selected and prev_selected in active_monitors:
                    selected_key = prev_selected
                elif active_monitors:
                    selected_key = next(iter(active_monitors))
        except Exception as e:
            logger.warning(f"[monitors] 載入失敗: {e}")

    yield

    for k in list(active_monitors.keys()):
        await _remove_pair(k)
    logger.info("=== 5021_FundingRate 關閉 ===")


app = FastAPI(title="FundingRate Predictor", lifespan=lifespan)
# 這個 API 沒有身分驗證，所以 CORS 只開本機來源。
# 要放寬用環境變數 CORS_ORIGINS（逗號分隔），但先在前面加一層認證再說。
_origins = os.environ.get("CORS_ORIGINS")
app.add_middleware(
    CORSMiddleware,
    allow_origins=(
        [o.strip() for o in _origins.split(",") if o.strip()]
        if _origins else
        ["http://127.0.0.1:5021", "http://localhost:5021"]
    ),
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    allow_credentials=False,
)


# ─── API Routes ───────────────────────────────────────

def _resolve_pair(exchange: Optional[str], symbol: Optional[str]) -> Optional[dict]:
    """依 exchange+symbol 找到 pair dict；若未指定則用 selected_key"""
    if symbol:
        sym = symbol.upper()
        ex = exchange.lower() if exchange else None
        key = _pair_key(ex, sym) if ex else next(
            (k for k in active_monitors if k.endswith(f":{sym}")), None
        )
    else:
        key = selected_key
    return active_monitors.get(key) if key else None


@app.get("/api/data")
async def get_data(symbol: str = None, exchange: str = None):
    """即時費率資料（含 TWAP + 預測）"""
    pair = _resolve_pair(exchange, symbol)
    if not pair:
        return {"error": "no data yet", "ts": time.time()}
    ticker = pair.get("ticker", {})
    if not ticker:
        return {"error": "no data yet", "ts": time.time()}

    engine: FundingEngine = pair["engine"]
    client: ExchangeClient = pair["client"]
    # _ws_cache 由 depth 訊息即時更新（impact_premium_ready / instant_premium）
    # pair["ticker"] 只有 ticker WS 訊息才更新，價格不動時可能過期數十秒
    # 合併兩者：以 pair["ticker"] 為基底，用最新 _ws_cache 覆蓋 impact 相關欄位
    live_ticker = {**ticker, **{k: v for k, v in client._ws_cache.items()
                                if k in ("instant_premium", "impact_premium_ready",
                                         "mark_price", "index_price", "bid1", "ask1")}}
    next_ft = ticker.get("next_funding_time", 0)
    result = engine.compute(live_ticker, next_ft)
    result["symbol"] = client.symbol
    result["exchange"] = client.exchange

    pre_sample = pair.get("pre_sample", {})
    if pre_sample:
        result["pre_sample_rate_pct"] = pre_sample["next_minute_rate_pct"]
        result["pre_sample_premium_pct"] = pre_sample["instant_premium_pct"]
        result["pre_sample_ts"] = pre_sample["ts"]

    return result


@app.websocket("/ws")
async def ws_data(websocket: WebSocket, exchange: str = None, symbol: str = None):
    """即時資料 WebSocket：每 300ms push 一次當前幣種的完整計算結果"""
    await websocket.accept()
    try:
        while True:
            pair = _resolve_pair(exchange, symbol)
            if pair:
                ticker = pair.get("ticker", {})
                if ticker:
                    engine: FundingEngine = pair["engine"]
                    client: ExchangeClient = pair["client"]
                    live_ticker = {**ticker, **{k: v for k, v in client._ws_cache.items()
                                                if k in ("instant_premium", "impact_premium_ready",
                                                         "mark_price", "index_price", "bid1", "ask1")}}
                    next_ft = ticker.get("next_funding_time", 0)
                    result = engine.compute(live_ticker, next_ft)
                    result["symbol"] = client.symbol
                    result["exchange"] = client.exchange
                    pre_sample = pair.get("pre_sample", {})
                    if pre_sample:
                        result["pre_sample_rate_pct"] = pre_sample["next_minute_rate_pct"]
                        result["pre_sample_premium_pct"] = pre_sample["instant_premium_pct"]
                        result["pre_sample_ts"] = pre_sample["ts"]
                    stats_key = _pair_key(client.exchange, client.symbol)
                    result["stats"] = stats_manager.get_stats(stats_key)
                    result["session_start_ts"] = pair.get("session_start_ts", 0)
                    result["fr_initializing"] = client.fr_initializing
                    await websocket.send_json(result)
                else:
                    await websocket.send_json({"error": "no data yet", "ts": time.time()})
            else:
                await websocket.send_json({"error": "no data yet", "ts": time.time()})
            await asyncio.sleep(0.3)
    except (WebSocketDisconnect, Exception):
        pass


@app.get("/api/symbols")
async def get_symbols():
    """所有監控中幣種的摘要列表"""
    result = []
    for key, pair in active_monitors.items():
        ticker = pair.get("ticker", {})
        pre_sample = pair.get("pre_sample", {})
        client: ExchangeClient = pair["client"]
        result.append({
            "key": key,
            "exchange": pair["exchange"],
            "symbol": pair["symbol"],
            "selected": key == selected_key,
            "ws_ready": client.ws_ready,
            "last_funding_rate_pct": round(ticker.get("last_funding_rate", 0) * 100, 6) if ticker else None,
            "predicted_rate_pct": pre_sample.get("predicted_rate_pct"),
            "next_minute_rate_pct": pre_sample.get("next_minute_rate_pct"),
            "instant_premium_pct": round(ticker.get("instant_premium", 0) * 100, 6) if ticker else None,
            "next_funding_time": ticker.get("next_funding_time", 0) if ticker else 0,
            "window_minutes": client.window_minutes,
        })
    return result


@app.get("/api/stats")
async def get_stats(symbol: str = None, exchange: str = None):
    """預測準確度統計"""
    pair = _resolve_pair(exchange, symbol)
    if not pair:
        return {"error": "not initialized"}
    stats_key = _pair_key(pair["exchange"], pair["symbol"])
    return stats_manager.get_stats(stats_key)


@app.get("/api/chart")
async def get_chart(symbol: str = None, exchange: str = None):
    """Premium 歷史圖表（最近 60 分鐘）"""
    pair = _resolve_pair(exchange, symbol)
    if not pair:
        return []
    return pair["engine"].get_chart_data()


@app.get("/api/inverse")
async def get_inverse(target: float = 0.0, symbol: str = None, exchange: str = None):
    """逆推：計算達到目標費率所需維持的平均溢價"""
    pair = _resolve_pair(exchange, symbol)
    if not pair:
        return {"error": "not initialized"}
    engine: FundingEngine = pair["engine"]
    ticker = pair.get("ticker", {})
    if not ticker:
        return {"needed_pct": None, "target_pct": round(target * 100, 6)}
    next_ft = ticker.get("next_funding_time", 0)
    result = engine.compute(ticker, next_ft)
    official_rate = ticker.get("last_funding_rate", 0) or 0
    remain_min = result.get("remain_min", 0)
    window = engine.window_minutes
    r = int(remain_min)
    e = max(1, window - r)
    if r <= 0 or official_rate == 0:
        return {"needed_pct": None, "target_pct": round(target * 100, 6)}
    target_twap = engine.inverse_funding(target)
    implied_twap = engine.inverse_funding(official_rate)
    needed = (target_twap * window - implied_twap * e) / r
    return {
        "needed_pct": round(needed * 100, 6),
        "target_pct": round(target * 100, 6),
        "remain_min": r,
    }


@app.get("/api/status")
async def get_status():
    """系統狀態（所有監控幣種）"""
    return {
        "selected_key": selected_key,
        "pairs": [
            {
                "key": k,
                "exchange": p["exchange"],
                "symbol": p["symbol"],
                "ws_ready": p["client"].ws_ready,
                "history_count": len(p["engine"]._premium_history),
                "window_minutes": p["engine"].window_minutes,
                "ticker_age": time.time() - p["ticker"].get("ts", 0) if p.get("ticker") else 999,
                "sampling_offset": p["client"].sampling_offset,
            }
            for k, p in active_monitors.items()
        ],
    }


class PairRequest(BaseModel):
    exchange: str
    symbol: str


@app.post("/api/add")
async def add_pair_api(req: PairRequest):
    """新增幣種到監控列表（若已存在則切換焦點）"""
    global selected_key
    async with _monitor_lock:
        exchange = req.exchange.lower()
        symbol = req.symbol.upper().strip()
        if not symbol.endswith(SYMBOL_SUFFIXES):
            symbol += 'USDT'
        if exchange not in SUPPORTED_EXCHANGES:
            return {"error": f"不支援的交易所: {exchange}"}

        key = _pair_key(exchange, symbol)
        if key in active_monitors:
            selected_key = key
            _save_monitors()
            return {"status": "ok", "key": key, "already_monitoring": True}

        try:
            key = await _add_pair(exchange, symbol, preload_bg=True)
            selected_key = key
            _save_monitors()
            client = active_monitors[key]["client"]
            return {"status": "ok", "key": key, "exchange": exchange, "symbol": symbol,
                    "window_minutes": client.window_minutes}
        except Exception as e:
            logger.error(f"新增幣種失敗: {e}")
            # 若初始化失敗清理
            active_monitors.pop(key, None)
            return {"error": str(e)}


@app.post("/api/remove")
async def remove_pair_api(req: PairRequest):
    """從監控列表移除幣種"""
    global selected_key
    exchange = req.exchange.lower()
    symbol = req.symbol.upper().strip()
    key = _pair_key(exchange, symbol)
    if key not in active_monitors:
        return {"error": "not monitoring"}
    await _remove_pair(key)
    if selected_key == key:
        selected_key = next(iter(active_monitors), "")
    _save_monitors()
    return {"status": "ok", "selected_key": selected_key, "remaining": list(active_monitors.keys())}


@app.post("/api/switch")
async def switch_pair(req: PairRequest):
    """切換主顯示幣種（若未監控則自動新增）"""
    global selected_key
    async with _monitor_lock:
        exchange = req.exchange.lower()
        symbol = req.symbol.upper().strip()
        if not symbol.endswith(SYMBOL_SUFFIXES):
            symbol += 'USDT'
        if exchange not in SUPPORTED_EXCHANGES:
            return {"error": f"不支援的交易所: {exchange}"}

        key = _pair_key(exchange, symbol)
        logger.info(f"切換至 [{exchange}] {symbol}")

        if key not in active_monitors:
            try:
                key = await _add_pair(exchange, symbol, preload_bg=True)
            except Exception as e:
                logger.error(f"切換失敗: {e}")
                return {"error": str(e)}

        selected_key = key
        _save_monitors()
        client = active_monitors[key]["client"]
        return {
            "status": "ok",
            "exchange": exchange,
            "symbol": symbol,
            "window_minutes": client.window_minutes,
        }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=3021)
