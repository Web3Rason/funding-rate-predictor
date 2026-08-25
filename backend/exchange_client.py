# -*- coding: utf-8 -*-
"""
Exchange API Client — Binance / Bybit / Bitget

資料架構：
  REST  → initialize() 偵測結算窗口 & cap、get_klines_history() 預填歷史
  WebSocket → 即時 ticker 推播（mark/index/funding_rate），偵測採樣時刻偏移

WebSocket 採樣偵測邏輯：
  每當 official_rate (r) 跳變，記錄當下 time.time() % 60 作為採樣偏移量
  使用滾動平均（最近 5 次）穩定偏移估計
  main.py 利用此偏移，在「下次預期採樣前 1 秒」觸發精確預測
"""
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from collections import deque
from typing import Optional, Callable

import httpx
import websockets

logger = logging.getLogger("ExchangeClient")


class _BitgetWSHub:
    """
    Bitget 所有 symbol 共用一條 WS 連線，避免多連線互踢問題。
    Bitget 每個 IP 的同時連線數受限，多連線會導致舊連線被踢。
    """

    def __init__(self):
        self._clients: dict = {}           # symbol → ExchangeClient
        self._task: Optional[asyncio.Task] = None
        self._ws = None                    # 當前 WS 連線參照

    def register(self, ec: "ExchangeClient"):
        is_new = ec.symbol not in self._clients
        self._clients[ec.symbol] = ec
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._ws_loop())
        elif is_new and self._ws is not None:
            # WS 已連線，補送 subscribe 給新 symbol
            asyncio.create_task(self._late_subscribe(ec.symbol))

    def unregister(self, ec: "ExchangeClient"):
        self._clients.pop(ec.symbol, None)

    async def _late_subscribe(self, sym: str):
        """WS 已連線時補訂閱新加入的 symbol"""
        if self._ws is None:
            return
        try:
            args = [
                {"instType": "USDT-FUTURES", "channel": "ticker", "instId": sym},
                {"instType": "USDT-FUTURES", "channel": "books",  "instId": sym},
            ]
            await self._ws.send(json.dumps({"op": "subscribe", "args": args}))
            logger.info(f"[BitgetHub] 補訂閱 {sym}")
        except Exception as e:
            logger.warning(f"[BitgetHub] 補訂閱 {sym} 失敗: {e}")

    async def _ws_loop(self):
        while self._clients:
            try:
                async with websockets.connect(
                    BITGET_WS, ping_interval=None, close_timeout=5
                ) as ws:
                    self._ws = ws
                    # 訂閱所有已註冊的 symbol
                    args = []
                    for sym in list(self._clients.keys()):
                        args += [
                            {"instType": "USDT-FUTURES", "channel": "ticker", "instId": sym},
                            {"instType": "USDT-FUTURES", "channel": "books",  "instId": sym},
                        ]
                    if args:
                        await ws.send(json.dumps({"op": "subscribe", "args": args}))

                    # 重連時重置各 client 的訂單簿狀態
                    for ec in list(self._clients.values()):
                        ec._bitget_depth_initialized = False
                        ec._bitget_depth_bids.clear()
                        ec._bitget_depth_asks.clear()

                    asyncio.create_task(self._keep_alive(ws))
                    logger.info(f"[BitgetHub] 已連線，訂閱 {list(self._clients.keys())}")

                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong"}))
                            continue
                        # 路由到對應的 ExchangeClient
                        arg = msg.get("arg", {})
                        instId = arg.get("instId", "")
                        ec = self._clients.get(instId)
                        if ec:
                            ec._handle_ws_msg(msg)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[BitgetHub] 斷線 ({e})，3 秒後重連...")
                self._ws = None
                await asyncio.sleep(3)
        self._ws = None

    async def _keep_alive(self, ws):
        """每 25 秒送 application-level ping（Bitget 要求 30s 內有 ping）"""
        try:
            while True:
                await asyncio.sleep(25)
                if ws.closed:
                    return
                await ws.send(json.dumps({"op": "ping"}))
        except Exception:
            pass


_BITGET_HUB: Optional["_BitgetWSHub"] = None


def _get_bitget_hub() -> "_BitgetWSHub":
    global _BITGET_HUB
    if _BITGET_HUB is None:
        _BITGET_HUB = _BitgetWSHub()
    return _BITGET_HUB

BINANCE_BASE  = "https://fapi.binance.com"
BYBIT_BASE    = "https://api.bybit.com"
BITGET_BASE   = "https://api.bitget.com"
ASTER_BASE    = "https://fapi.asterdex.com"

# Binance 2026-04-23 起把 markPrice/aggTrade/kline 等串流搬到新 endpoint /market/stream，
# 但 depth/bookTicker 仍只在舊 /stream，必須拆兩條 WS 連線。
BINANCE_MARKET_WS = "wss://fstream.binance.com/market/stream"  # markPrice@1s 走這
BINANCE_DEPTH_WS  = "wss://fstream.binance.com/stream"          # depth@100ms 走這
BYBIT_WS      = "wss://stream.bybit.com/v5/public/linear"
BITGET_WS     = "wss://ws.bitget.com/v2/ws/public"
GATEIO_BASE   = "https://api.gateio.ws/api/v4"
GATEIO_WS     = "wss://fx-ws.gateio.ws/v4/ws/usdt"
# Gate.io 增量訂單簿深度。訂閱 futures.order_book_update 的 level 與 REST 快照的 limit
# 必須是同一個值：Gate 只維護該深度內的檔位，若本地簿比推送深度更深，深層檔位收不到
# 「s=0 刪除」事件就會變成永不消失的幽靈檔。價格快速移動時幽靈檔會讓本地簿交叉
# （best_bid > best_ask），impact_ask 被凍結在舊價，impact premium 算出巨大負值。
# 實測 TUT_USDT（無 level + limit=200）40 秒即腐爛；對齊成 100 後 60 秒全程貼合真實盤口。
GATEIO_DEPTH_LEVEL = 100
ASTER_WS      = "wss://fstream.asterdex.com/stream"  # Aster combined stream（與 Binance 格式相同）
OKX_BASE      = "https://www.okx.com"
OKX_WS        = "wss://ws.okx.com:8443/ws/v5/public"


class ExchangeClient:
    def __init__(self, exchange: str = "binance", symbol: str = "BTCUSDT",
                 api_key: str = None, api_secret: str = None, api_passphrase: str = None):
        self.exchange = exchange.lower()
        self.symbol = symbol.upper()
        self._api_key: Optional[str] = api_key
        self._api_secret: Optional[str] = api_secret
        self._api_passphrase: Optional[str] = api_passphrase
        self._client: Optional[httpx.AsyncClient] = None
        self._window_minutes: int = 480
        self._cap: Optional[float] = None

        # WebSocket 狀態
        self._ws_cache: dict = {}          # 最新 ticker 資料（WS 推播更新）
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_ready = asyncio.Event()   # WS 第一筆資料到達後 set
        self._ws_ready_ts: float = 0.0     # WS 首次就緒的時間戳

        # 採樣偏移追蹤（記錄 official_rate 跳變時的秒數 offset）
        self._sampling_offsets: deque = deque(maxlen=3)
        self._last_r: Optional[float] = None
        self._last_index_price: float = 0.0  # 最後已知 index_price（depth 更新 fallback 用）

        # 採樣時刻回調（由 main.py 設定，WS 偵測到 r 跳變時呼叫）
        self.on_sample: Optional[Callable] = None

        # Binance 即時訂單簿（diff stream + REST 快照，無深度限制）
        self._depth_bids: dict = {}   # {price_str: qty_float}，維護完整 bid 簿
        self._depth_asks: dict = {}   # {price_str: qty_float}，維護完整 ask 簿
        self._depth_last_update_id: int = 0   # REST 快照的 lastUpdateId
        self._depth_initialized: bool = False  # 快照已載入且 diff 已同步
        self._depth_pending: list = []         # 快照載入前暫存的 diff 事件
        self._impact_notional: float = 200.0  # 暫存預設值；initialize() 後由 200/maintMarginRate 覆蓋

        # Gate.io 即時訂單簿（futures.order_book_update stream + REST 快照）
        self._gateio_depth_bids: dict = {}   # {price_str: size_float}，size 為合約數量
        self._gateio_depth_asks: dict = {}   # {price_str: size_float}
        self._gateio_depth_last_id: int = 0  # REST 快照的 id
        self._gateio_depth_initialized: bool = False
        self._gateio_depth_pending: list = []
        self._gateio_resync_task: Optional[asyncio.Task] = None  # 強引用，防 GC 掉重建快照的 task
        self._gateio_quanto_multiplier: float = 1.0  # 每合約對應基礎資產數量（BTC=0.0001, HOOK=1）
        self._gateio_impact_notional: float = 2000.0  # funding_impact_value（USDT 名義金額）直接從 API 取得
        self._gateio_min_rate: float = 0.0  # 溢價=0 時的結算費率（= daily_interest / periods_per_day）；
        #   新算法下 = base_interest × N/8，仍是「報價失靈偵測」的哨兵值（premium_index=0 時官方費率會等於此）
        self._gateio_base_interest: float = 0.0001  # 8h 等效基礎利率（新算法 clamp 內用）= daily_interest × 8/24
        self._gateio_is_commodity: bool = False  # contract_type == "commodities"（NATGAS 等）
        # Gate.io 真實下次結算時間（毫秒）= 合約 funding_next_apply × 1000。
        # Gate WS/tickers 均無此欄位（實測），只有合約端有，故由 _gateio_funding_poll_loop 定期更新。
        # next_funding_time 改用此真值後，Gate 一旦調整結算週期，main.py 的結算偵測會在
        # 下一次真實結算（≤1 個週期）即抓到 next_ft 跳變並 refresh_settings 重算窗口，
        # 不再受限於「用假設窗口推算 → 只能在假設窗口整點才複檢」的死結。
        self._gateio_next_apply_ms: int = 0
        self._gateio_poll_task: Optional[asyncio.Task] = None
        # commodities 合約特性：mark_type="index"（mark = last 不依訂單簿衍生）、interest_rate=0
        # 訂單簿 impact premium 公式不適用，必須改用 (mark-index)/index 作為 premium

        # Bybit 即時訂單簿（orderbook.50 stream：WS 直接推 snapshot + delta，無需 REST）
        self._bybit_depth_bids: dict = {}   # {price_str: qty_float}
        self._bybit_depth_asks: dict = {}   # {price_str: qty_float}
        self._bybit_depth_initialized: bool = False
        self._bybit_impact_log_ts: float = 0.0  # 診斷 log throttle（每 30 秒一次）

        # Bitget 即時訂單簿（books stream：WS 直接推 snapshot + update，IMN=200 USDT）
        self._bitget_depth_bids: dict = {}   # {price_str: qty_float}，qty = 合約數量
        self._bitget_depth_asks: dict = {}   # {price_str: qty_float}
        self._bitget_depth_initialized: bool = False

        # OKX 即時訂單簿（books stream：WS 推 snapshot + update，400 檔深度）
        self._okx_depth_bids: dict = {}   # {price_str: qty_float}
        self._okx_depth_asks: dict = {}   # {price_str: qty_float}
        self._okx_depth_initialized: bool = False
        # OKX 每個合約 interest_rate 不同：
        # - 加密合約：BTC/ETH 8h=0.01%；4h/1h 結算時 OKX 會自動縮放（0.005% / 0.00125%）
        # - TradFi 合約（XAU/XAG/NG/GAS/CL 等）：interestRate=0（OKX 公告 2026-03-18 起）
        # 文檔：https://www.okx.com/help/perps-funding-fee-mechanism
        # 公式 I 直接代入，不需再縮放——API 給的就是 per-period 真實值
        # （TradFi 仍用同一套訂單簿 impact 公式，mark price 計算未改變）
        self._okx_interest_rate: Optional[float] = None

    # ─── Gate.io 符號轉換 ─────────────────────────────────
    def _to_gateio_symbol(self) -> str:
        """BTCUSDT → BTC_USDT（Gate.io API 格式）"""
        sym = self.symbol
        for quote in ("USDT", "USDC", "BTC", "ETH"):
            if sym.endswith(quote) and len(sym) > len(quote):
                return sym[:-len(quote)] + "_" + quote
        return sym

    # ─── OKX 符號轉換 ────────────────────────────────────
    def _to_okx_swap_symbol(self) -> str:
        """BTCUSDT → BTC-USDT-SWAP（OKX 永續合約格式）"""
        sym = self.symbol
        for quote in ("USDT", "USDC"):
            if sym.endswith(quote) and len(sym) > len(quote):
                return sym[:-len(quote)] + "-" + quote + "-SWAP"
        return sym + "-SWAP"

    def _to_okx_index_symbol(self) -> str:
        """BTCUSDT → BTC-USDT（OKX index ticker 格式，無 -SWAP 後綴）"""
        sym = self.symbol
        for quote in ("USDT", "USDC"):
            if sym.endswith(quote) and len(sym) > len(quote):
                return sym[:-len(quote)] + "-" + quote
        return sym

    # ─── 屬性 ────────────────────────────────────────────
    @property
    def window_minutes(self) -> int:
        return self._window_minutes

    @property
    def cap(self) -> Optional[float]:
        return self._cap

    @property
    def sampling_offset(self) -> Optional[float]:
        """滾動平均採樣偏移（秒），None 表示尚未偵測到"""
        if not self._sampling_offsets:
            return None
        return sum(self._sampling_offsets) / len(self._sampling_offsets)

    @property
    def ws_ready(self) -> bool:
        return self._ws_ready.is_set()

    @property
    def fr_initializing(self) -> bool:
        """WS 剛連線且 last_funding_rate 尚為 0（切換幣種後的暫時缺失，通常 < 30s 自動恢復）"""
        if not self._ws_ready.is_set():
            return False
        if self._ws_cache.get("last_funding_rate", 0) != 0:
            return False
        return (time.time() - self._ws_ready_ts) < 30

    # ─── 初始化 ───────────────────────────────────────────
    async def _validate_symbol(self):
        """驗證幣種在交易所是否存在，不存在則 raise ValueError"""
        try:
            if self.exchange in ("binance", "aster"):
                resp = await self._client.get(
                    f"{self._fapi_base()}/fapi/v1/premiumIndex",
                    params={"symbol": self.symbol},
                )
                if resp.status_code == 400:
                    raise ValueError(f"找不到此幣種：{self.symbol} 在 {self.exchange} 期貨不存在")
            elif self.exchange == "bybit":
                resp = await self._client.get(
                    f"{BYBIT_BASE}/v5/market/tickers",
                    params={"category": "linear", "symbol": self.symbol},
                )
                data = resp.json()
                if not data.get("result", {}).get("list"):
                    raise ValueError(f"找不到此幣種：{self.symbol} 在 {self.exchange} 期貨不存在")
            elif self.exchange == "bitget":
                resp = await self._client.get(
                    f"{BITGET_BASE}/api/v2/mix/market/ticker",
                    params={"productType": "USDT-FUTURES", "symbol": self.symbol},
                )
                if resp.status_code >= 400 or resp.json().get("code", "0") != "00000":
                    raise ValueError(f"找不到此幣種：{self.symbol} 在 {self.exchange} 期貨不存在")
            elif self.exchange == "gateio":
                resp = await self._client.get(
                    f"{GATEIO_BASE}/futures/usdt/tickers",
                    params={"contract": self._to_gateio_symbol()},
                )
                if resp.status_code >= 400 or not resp.json():
                    raise ValueError(f"找不到此幣種：{self.symbol} 在 {self.exchange} 期貨不存在")
            elif self.exchange == "okx":
                inst_id = self._to_okx_swap_symbol()
                resp = await self._client.get(
                    f"{OKX_BASE}/api/v5/public/instruments",
                    params={"instType": "SWAP", "instId": inst_id},
                )
                data = resp.json().get("data", [])
                if not data:
                    raise ValueError(f"找不到此幣種：{self.symbol} 在 OKX 永續合約不存在")
        except ValueError:
            raise
        except Exception:
            pass  # 網路問題不阻止初始化

    async def initialize(self):
        self._client = httpx.AsyncClient(
            timeout=12,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
        try:
            await self._validate_symbol()
        except ValueError:
            await self._client.aclose()
            raise
        try:
            self._window_minutes, self._cap = await self._detect_window_and_cap()
            cap_str = f"±{self._cap*100:.2f}%" if self._cap is not None else "預設"
            logger.info(f"[{self.exchange}] {self.symbol} 窗口:{self._window_minutes}min cap:{cap_str}")
        except Exception as e:
            logger.warning(f"偵測窗口/cap 失敗（預設 480min）: {e}")
            self._window_minutes = 480
            self._cap = None

        # 啟動 WebSocket 背景任務
        if self.exchange == "bitget":
            # Bitget 所有 symbol 共用一條 WS，避免多連線互踢
            _get_bitget_hub().register(self)
            self._ws_task = _get_bitget_hub()._task
        else:
            self._ws_task = asyncio.create_task(self._ws_loop())

        # Gate.io：WS 無 funding_next_apply，啟動低頻輪詢維持真實結算時間
        if self.exchange == "gateio":
            self._gateio_poll_task = asyncio.create_task(self._gateio_funding_poll_loop())

    async def close(self):
        if self.exchange == "bitget":
            _get_bitget_hub().unregister(self)
        elif self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        if self._gateio_poll_task:
            self._gateio_poll_task.cancel()
            try:
                await self._gateio_poll_task
            except asyncio.CancelledError:
                pass
        if self._gateio_resync_task:
            self._gateio_resync_task.cancel()
            try:
                await self._gateio_resync_task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.aclose()

    # ─── Ticker（優先 WS 快取，fallback REST）────────────
    def _fapi_base(self) -> str:
        """Binance-like 交易所的 REST base URL"""
        return ASTER_BASE if self.exchange == "aster" else BINANCE_BASE

    async def get_ticker(self) -> dict:
        if self._ws_cache:
            return self._ws_cache
        # WS 尚未就緒時 fallback REST
        if self.exchange in ("binance", "aster"):
            return await self._binance_ticker()
        elif self.exchange == "bybit":
            return await self._bybit_ticker()
        elif self.exchange == "bitget":
            return await self._bitget_ticker()
        elif self.exchange == "gateio":
            return await self._gateio_ticker()
        elif self.exchange == "okx":
            return await self._okx_ticker()
        return self._empty_ticker()

    # ─── WebSocket 核心 ───────────────────────────────────
    async def _ws_loop(self):
        """WebSocket 監聽主迴圈，自動重連"""
        # Binance 從 2026-04-23 起把 markPrice 與 depth 拆到不同 endpoint，
        # 走獨立的 dual-loop（兩條 WS 各自重連）
        if self.exchange == "binance":
            await self._binance_dual_ws_loop()
            return

        while True:
            try:
                url = self._ws_url()
                # Bitget/OKX WS 不回應 WebSocket 協定層 ping，用 application-level ping 維持連線
                ws_ping_interval = None if self.exchange in ("bitget", "okx") else 20
                ws_ping_timeout  = None if self.exchange in ("bitget", "okx") else 10
                async with websockets.connect(
                    url,
                    ping_interval=ws_ping_interval,
                    ping_timeout=ws_ping_timeout,
                    close_timeout=5,
                ) as ws:
                    await self._ws_subscribe(ws)
                    logger.info(f"[WS] {self.exchange} {self.symbol} 已連線")
                    # Aster diff depth 需要先載入 REST 快照再開始同步
                    if self.exchange == "aster":
                        asyncio.create_task(self._init_depth_snapshot())
                    elif self.exchange == "gateio":
                        asyncio.create_task(self._gateio_keep_alive(ws))
                        # 重連時重置訂單簿狀態，重新載入快照
                        self._gateio_depth_initialized = False
                        self._gateio_depth_pending.clear()
                        self._gateio_resync_task = asyncio.create_task(self._init_gateio_depth_snapshot())
                    elif self.exchange == "bybit":
                        # Bybit WS 直接推 snapshot，重連時重置等待新 snapshot
                        self._bybit_depth_initialized = False
                        self._bybit_depth_bids.clear()
                        self._bybit_depth_asks.clear()
                    elif self.exchange == "bitget":
                        asyncio.create_task(self._bitget_keep_alive(ws))
                        self._bitget_depth_initialized = False
                        self._bitget_depth_bids.clear()
                        self._bitget_depth_asks.clear()
                    elif self.exchange == "okx":
                        asyncio.create_task(self._okx_keep_alive(ws))
                        self._okx_depth_initialized = False
                        self._okx_depth_bids.clear()
                        self._okx_depth_asks.clear()
                    async for raw in ws:
                        # OKX pong 回應是純字串 "pong"，非 JSON
                        if self.exchange == "okx" and raw == "pong":
                            continue
                        msg = json.loads(raw)
                        # Bitget server 會主動送 ping，需回應 pong 維持連線
                        if self.exchange == "bitget" and msg.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong"}))
                            continue
                        # OKX 訂閱確認事件（op=subscribe），跳過
                        if self.exchange == "okx" and msg.get("event") in ("subscribe", "error"):
                            if msg.get("event") == "error":
                                logger.warning(f"[OKX WS] 訂閱錯誤: {msg}")
                            continue
                        self._handle_ws_msg(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[WS] 斷線 ({e})，3 秒後重連...")
                await asyncio.sleep(3)

    # ─── Binance 雙 WS（markPrice + depth 端點分裂）──────
    async def _binance_dual_ws_loop(self):
        """Binance 2026-04-23 起 markPrice 與 depth 必須走不同 endpoint，
        各自跑一條 WS 連線，獨立重連。"""
        mark_task = asyncio.create_task(self._binance_markprice_ws())
        depth_task = asyncio.create_task(self._binance_depth_ws())
        try:
            await asyncio.gather(mark_task, depth_task)
        except asyncio.CancelledError:
            mark_task.cancel()
            depth_task.cancel()
            for t in (mark_task, depth_task):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            raise

    async def _binance_markprice_ws(self):
        """Binance markPrice@1s 串流（新 endpoint /market/stream）"""
        sym = self.symbol.lower()
        url = f"{BINANCE_MARKET_WS}?streams={sym}@markPrice@1s"
        while True:
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=10, close_timeout=5,
                ) as ws:
                    logger.info(f"[WS] binance {self.symbol} markPrice 已連線")
                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", msg)
                        self._handle_binance_markprice(data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[WS] binance {self.symbol} markPrice 斷線 ({e})，3 秒後重連...")
                await asyncio.sleep(3)

    async def _binance_depth_ws(self):
        """Binance depth@100ms 串流（舊 endpoint /stream，仍是 diff 推送）"""
        sym = self.symbol.lower()
        url = f"{BINANCE_DEPTH_WS}?streams={sym}@depth@100ms"
        while True:
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=10, close_timeout=5,
                ) as ws:
                    logger.info(f"[WS] binance {self.symbol} depth 已連線")
                    # 重連時重置訂單簿狀態，重新載入 REST 快照
                    self._depth_initialized = False
                    self._depth_pending.clear()
                    self._depth_bids.clear()
                    self._depth_asks.clear()
                    asyncio.create_task(self._init_depth_snapshot())
                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", msg)
                        self._handle_binance_depth(data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[WS] binance {self.symbol} depth 斷線 ({e})，3 秒後重連...")
                await asyncio.sleep(3)

    def _handle_binance_markprice(self, data: dict):
        """Binance markPriceUpdate 訊息處理（不包含 depth）"""
        ticker = self._parse_binance_ws(data)
        if ticker is None:
            return

        # Depth 已就緒則用 impact-based premium 覆蓋 (mark-index)/index
        if self._depth_initialized:
            premium = self._calc_impact_premium(ticker["index_price"])
            if premium is not None:
                ticker["instant_premium"] = premium

        # 偵測 official_rate 跳變 → 記錄採樣偏移
        new_r = ticker.get("last_funding_rate")
        if new_r is not None and self._last_r is not None and new_r != self._last_r:
            offset = time.time() % 60
            self._sampling_offsets.append(offset)
            logger.info(f"[WS] 採樣跳變偵測 offset={offset:.3f}s  {self._last_r*100:.6f}% → {new_r*100:.6f}%")
            if self.on_sample:
                asyncio.create_task(self.on_sample(new_r, time.time(), self._last_r))

        if new_r is not None:
            self._last_r = new_r

        if ticker.get("index_price", 0) > 0:
            self._last_index_price = ticker["index_price"]

        self._ws_cache = ticker
        if not self._ws_ready.is_set():
            self._ws_ready_ts = time.time()
            self._ws_ready.set()
            if (self._ws_cache.get("last_funding_rate", 0) == 0
                    or self._ws_cache.get("next_funding_time", 0) == 0):
                asyncio.create_task(self._seed_funding_rate_from_rest())

    def _handle_binance_depth(self, data: dict):
        """Binance depth diff 訊息處理（套用到本地訂單簿並重算 impact premium）"""
        self._apply_depth_diff(data)
        if self._ws_cache and self._depth_initialized:
            index_price = self._ws_cache.get("index_price", 0)
            premium = self._calc_impact_premium(index_price)
            if premium is not None:
                self._ws_cache["instant_premium"] = premium

    def _ws_url(self) -> str:
        if self.exchange == "aster":
            sym = self.symbol.lower()
            return f"{ASTER_WS}?streams={sym}@markPrice@1s/{sym}@depth@100ms"
        elif self.exchange == "bybit":
            return BYBIT_WS
        elif self.exchange == "bitget":
            return BITGET_WS
        elif self.exchange == "gateio":
            return GATEIO_WS
        elif self.exchange == "okx":
            return OKX_WS
        raise ValueError(f"Unknown exchange: {self.exchange}")

    async def _ws_subscribe(self, ws):
        """發送訂閱訊息（Aster 直接從 URL 訂閱，Bybit/Bitget/Gate.io/OKX 需發送訊息）"""
        if self.exchange == "bybit":
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": [f"tickers.{self.symbol}", f"orderbook.50.{self.symbol}"]
            }))
        elif self.exchange == "bitget":
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": [
                    {"instType": "USDT-FUTURES", "channel": "ticker", "instId": self.symbol},
                    {"instType": "USDT-FUTURES", "channel": "books", "instId": self.symbol},
                ]
            }))
        elif self.exchange == "gateio":
            sym = self._to_gateio_symbol()
            await ws.send(json.dumps({
                "time": int(time.time()),
                "channel": "futures.tickers",
                "event": "subscribe",
                "payload": [sym]
            }))
            await ws.send(json.dumps({
                "time": int(time.time()),
                "channel": "futures.order_book_update",
                "event": "subscribe",
                "payload": [sym, "100ms", str(GATEIO_DEPTH_LEVEL)]
            }))
        elif self.exchange == "okx":
            inst_id = self._to_okx_swap_symbol()
            idx_id  = self._to_okx_index_symbol()
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": [
                    {"channel": "tickers",      "instId": inst_id},
                    {"channel": "mark-price",   "instId": inst_id},
                    {"channel": "index-tickers", "instId": idx_id},
                    {"channel": "funding-rate",  "instId": inst_id},
                    {"channel": "books",         "instId": inst_id},
                ]
            }))
        # Aster 不需要額外訂閱訊息（從 URL 訂閱）

    def _handle_ws_msg(self, msg: dict):
        """解析 WS 訊息，更新快取，偵測採樣時刻"""
        ticker = None

        if self.exchange == "aster":
            # Aster 仍維持單一 combined stream（markPrice + depth 同 endpoint）
            stream = msg.get("stream", "")
            data = msg.get("data", msg)  # 若無 stream 欄位（不預期）則直接用 msg

            if "depth" in stream and "markPrice" not in stream:
                # diff depth 事件 → 套用到本地訂單簿
                self._apply_depth_diff(data)
                if self._ws_cache and self._depth_initialized:
                    index_price = self._ws_cache.get("index_price", 0)
                    premium = self._calc_impact_premium(index_price)
                    if premium is not None:
                        self._ws_cache["instant_premium"] = premium
                return  # depth 訊息不觸發採樣邏輯

            elif "markPrice" in stream:
                ticker = self._parse_binance_ws(data)

        elif self.exchange == "bybit":
            if msg.get("topic", "").startswith("orderbook"):
                self._apply_bybit_depth(msg)
                if self._ws_cache and self._bybit_depth_initialized:
                    mark = self._ws_cache.get("mark_price", 0)
                    index = self._ws_cache.get("index_price", 0)
                    premium = self._calc_bybit_impact_premium(mark, index)
                    if premium is not None:
                        self._ws_cache["instant_premium"] = premium
                        self._ws_cache["impact_premium_ready"] = True
                return
            ticker = self._parse_bybit_ws(msg)
        elif self.exchange == "bitget":
            if msg.get("arg", {}).get("channel") == "books":
                self._apply_bitget_depth(msg)
                if self._ws_cache and self._bitget_depth_initialized:
                    mark = self._ws_cache.get("mark_price", 0)
                    index = self._ws_cache.get("index_price", 0)
                    premium = self._calc_bitget_impact_premium(mark, index)
                    if premium is not None:
                        self._ws_cache["instant_premium"] = premium
                        self._ws_cache["impact_premium_ready"] = True
                return
            ticker = self._parse_bitget_ws(msg)
        elif self.exchange == "gateio":
            if msg.get("channel") == "futures.order_book_update" and msg.get("event") == "update":
                self._apply_gateio_depth_diff(msg.get("result", {}))
                if self._gateio_depth_initialized:
                    index_price = self._ws_cache.get("index_price", 0) or self._last_index_price
                    premium = self._calc_gateio_impact_premium(index_price)
                    if premium is not None:
                        self._ws_cache["instant_premium"] = premium
                        self._ws_cache["impact_premium_ready"] = True
                return
            ticker = self._parse_gateio_ws(msg)
        elif self.exchange == "okx":
            arg = msg.get("arg", {})
            channel = arg.get("channel", "")
            if channel == "books":
                self._apply_okx_depth(msg)
                if self._ws_cache and self._okx_depth_initialized:
                    index = self._ws_cache.get("index_price", 0)
                    premium = self._calc_okx_impact_premium(index)
                    if premium is not None:
                        self._ws_cache["instant_premium"] = premium
                        self._ws_cache["impact_premium_ready"] = True
                return
            ticker = self._parse_okx_ws(msg)

        if ticker is None:
            return

        # Aster：若訂單簿已就緒，用 impact-based premium 覆蓋 (mark-index)/index
        # （Binance 走 _binance_dual_ws_loop 不經此路徑）
        if self.exchange == "aster" and self._depth_initialized:
            premium = self._calc_impact_premium(ticker["index_price"])
            if premium is not None:
                ticker["instant_premium"] = premium

        # Gate.io：若訂單簿已就緒，用 impact-based premium 覆蓋，並標記為可信
        # commodities 合約走 (mark-index)/index 分支，需傳入剛解析的最新 mark
        if self.exchange == "gateio" and self._gateio_depth_initialized:
            premium = self._calc_gateio_impact_premium(
                ticker["index_price"], ticker.get("mark_price")
            )
            if premium is not None:
                ticker["instant_premium"] = premium
                ticker["impact_premium_ready"] = True

        # Bybit：若訂單簿已就緒，用 impact-based premium 覆蓋
        if self.exchange == "bybit" and self._bybit_depth_initialized:
            mark = ticker.get("mark_price", 0)
            index = ticker.get("index_price", 0)
            premium = self._calc_bybit_impact_premium(mark, index)
            if premium is not None:
                ticker["instant_premium"] = premium
                ticker["impact_premium_ready"] = True

        # Bitget：若訂單簿已就緒，用 impact-based premium 覆蓋
        if self.exchange == "bitget" and self._bitget_depth_initialized:
            mark = ticker.get("mark_price", 0)
            index = ticker.get("index_price", 0) or self._last_index_price
            premium = self._calc_bitget_impact_premium(mark, index)
            if premium is not None:
                ticker["instant_premium"] = premium
                ticker["impact_premium_ready"] = True

        # OKX：若訂單簿已就緒，用 impact-based premium 覆蓋
        # 包含 TradFi 合約（XAU/XAG/NG/GAS/CL）——OKX 對所有合約用同一套訂單簿 impact 公式
        if self.exchange == "okx" and self._okx_depth_initialized:
            index = ticker.get("index_price", 0)
            premium = self._calc_okx_impact_premium(index)
            if premium is not None:
                ticker["instant_premium"] = premium
                ticker["impact_premium_ready"] = True

        # 偵測 official_rate 跳變 → 記錄採樣偏移
        new_r = ticker.get("last_funding_rate")
        if new_r is not None and self._last_r is not None and new_r != self._last_r:
            offset = time.time() % 60
            self._sampling_offsets.append(offset)
            logger.info(f"[WS] 採樣跳變偵測 offset={offset:.3f}s  {self._last_r*100:.6f}% → {new_r*100:.6f}%")
            if self.on_sample:
                asyncio.create_task(self.on_sample(new_r, time.time(), self._last_r))

        if new_r is not None:
            self._last_r = new_r

        if ticker.get("index_price", 0) > 0:
            self._last_index_price = ticker["index_price"]

        self._ws_cache = ticker
        if not self._ws_ready.is_set():
            self._ws_ready_ts = time.time()
            self._ws_ready.set()
            # WS snapshot 可能缺少 fundingRate（Bybit 有時回傳空字串）或 nextFundingTime（Bitget delta 常不帶）
            # 若任一為 0，排程一次 REST 補充
            if (self._ws_cache.get("last_funding_rate", 0) == 0
                    or self._ws_cache.get("next_funding_time", 0) == 0):
                asyncio.create_task(self._seed_funding_rate_from_rest())

    async def _seed_funding_rate_from_rest(self, force: bool = False):
        """WS snapshot 缺少 fundingRate 時，從 REST 補充。
        force=True：強制更新 _last_r（例如結算後重置）
        """
        await asyncio.sleep(0.2)  # 讓 WS 事件循環穩定
        try:
            if self.exchange == "bybit":
                ticker = await self._bybit_ticker()
            elif self.exchange in ("binance", "aster"):
                ticker = await self._binance_ticker()
            elif self.exchange == "bitget":
                ticker = await self._bitget_ticker()
            elif self.exchange == "gateio":
                ticker = await self._gateio_ticker()
            elif self.exchange == "okx":
                ticker = await self._okx_ticker()
            else:
                return
            fr  = ticker.get("last_funding_rate", 0)
            nft = ticker.get("next_funding_time", 0)
            if fr:
                # force 模式（結算後）：強制更新 _last_r
                # 非 force：僅在 _ws_cache 仍為 0 時補充（避免蓋掉 WS 已更新的值）
                if force or self._ws_cache.get("last_funding_rate", 0) == 0:
                    self._ws_cache["last_funding_rate"] = fr
                    self._last_r = fr
                    logger.info(
                        f"[{self.exchange}] {self.symbol} REST 補充 fundingRate={fr*100:.6f}% "
                        f"（{'強制刷新' if force else 'WS snapshot 缺少 fundingRate'}）"
                    )
            # 補充 next_funding_time（Bitget WS delta 常不帶此欄位，導致 remain=0 → is_noisy_window 誤觸）
            if nft and (force or self._ws_cache.get("next_funding_time", 0) == 0):
                self._ws_cache["next_funding_time"] = nft
                logger.info(
                    f"[{self.exchange}] {self.symbol} REST 補充 nextFundingTime={nft} "
                    f"（{'強制刷新' if force else 'WS snapshot 缺少 nextFundingTime'}）"
                )
        except Exception as e:
            logger.warning(f"[{self.exchange}] {self.symbol} REST 補充 fundingRate 失敗: {e}")

    # ─── WS 訊息解析 ─────────────────────────────────────
    def _parse_binance_ws(self, msg: dict) -> Optional[dict]:
        """Binance markPriceUpdate stream"""
        if msg.get("e") != "markPriceUpdate":
            return None
        mark  = float(msg.get("p", 0))
        index = float(msg.get("i", 0))
        premium = (mark - index) / index if index > 0 else 0.0
        return {
            "mark_price":       mark,
            "index_price":      index,
            "bid1":             mark,
            "ask1":             mark,
            "instant_premium":  premium,
            "last_funding_rate": float(msg.get("r", 0)),
            "next_funding_time": int(msg.get("T", 0)),
            "interest_rate":    0.0001,
            "ts":               time.time(),
        }

    def _parse_bybit_ws(self, msg: dict) -> Optional[dict]:
        """Bybit linear tickers stream（支援 delta 更新：只推送變化欄位）"""
        data = msg.get("data", {})
        if not data:
            return None
        # Bybit tickers 是 delta 更新：每次只包含變化欄位。
        # 用 _ws_cache 保留上一次的完整快照，再以新欄位覆蓋。
        prev = self._ws_cache or {}
        mark  = float(data.get("markPrice", 0) or 0) or prev.get("mark_price", 0)
        index = float(data.get("indexPrice", 0) or 0) or prev.get("index_price", 0)
        if mark == 0:
            return None
        premium = (mark - index) / index if index > 0 else 0.0
        return {
            "mark_price":        mark,
            "index_price":       index,
            "bid1":              float(data.get("bid1Price", 0) or 0) or prev.get("bid1", mark),
            "ask1":              float(data.get("ask1Price", 0) or 0) or prev.get("ask1", mark),
            "instant_premium":   premium,
            "last_funding_rate": float(data.get("fundingRate", 0) or 0) or prev.get("last_funding_rate", 0),
            "next_funding_time": int(data.get("nextFundingTime", 0) or 0) or prev.get("next_funding_time", 0),
            "interest_rate":     0.0001,
            "ts":                time.time(),
        }

    def _parse_bitget_ws(self, msg: dict) -> Optional[dict]:
        """Bitget ticker stream"""
        data_list = msg.get("data", [])
        if not data_list:
            return None
        d = data_list[0]
        mark  = float(d.get("markPrice", 0) or 0)
        index = float(d.get("indexPrice", 0) or 0)
        if mark == 0:
            return None
        prev = self._ws_cache or {}
        premium = (mark - index) / index if index > 0 else 0.0
        return {
            "mark_price":       mark,
            "index_price":      index,
            "bid1":             float(d.get("bidPr", mark) or mark),
            "ask1":             float(d.get("askPr", mark) or mark),
            "instant_premium":  premium,
            "last_funding_rate": float(d.get("fundingRate", 0) or 0) or prev.get("last_funding_rate", 0),
            "next_funding_time": int(d.get("nextFundingTime", 0) or 0) or prev.get("next_funding_time", 0),
            "interest_rate":    0.0001,
            "ts":               time.time(),
        }

    def _parse_gateio_ws(self, msg: dict) -> Optional[dict]:
        """Gate.io futures.tickers stream"""
        if msg.get("event") != "update" or msg.get("channel") != "futures.tickers":
            return None
        result = msg.get("result", [])
        if not result:
            return None
        d = result[0] if isinstance(result, list) else result
        mark  = float(d.get("mark_price", 0) or 0)
        index = float(d.get("index_price", 0) or 0)
        if mark == 0:
            return None
        premium = (mark - index) / index if index > 0 else 0.0
        # Gate.io WS 無 next_funding_time 欄位，從結算週期計算
        # funding_rate = 上期結算值（1h/8h 才變）
        # funding_rate_indicative = 當前指示費率（每分鐘第 46 秒更新）← 用這個
        return {
            "mark_price":        mark,
            "index_price":       index,
            "bid1":              mark,
            "ask1":              mark,
            "instant_premium":   premium,
            "last_funding_rate": float(d.get("funding_rate_indicative", 0) or 0),
            "next_funding_time": self._gateio_next_funding_time_ms(),
            "interest_rate":     0.0001,
            "ts":                time.time(),
        }

    async def _bitget_keep_alive(self, ws):
        """每 25 秒發送 Bitget application-level ping 維持連線（Bitget 要求 30s 內送 ping，否則斷線）"""
        try:
            while True:
                await asyncio.sleep(25)
                await ws.send("ping")
        except Exception:
            pass

    async def _gateio_keep_alive(self, ws):
        """每 20 秒發送 Gate.io application-level ping 維持連線"""
        try:
            while True:
                await asyncio.sleep(20)
                await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.ping"}))
        except Exception:
            pass

    def _parse_okx_ws(self, msg: dict) -> Optional[dict]:
        """OKX 多頻道合併解析：tickers / mark-price / index-tickers / funding-rate"""
        arg = msg.get("arg", {})
        channel = arg.get("channel", "")
        data_list = msg.get("data", [])
        if not data_list:
            return None
        d = data_list[0]
        prev = self._ws_cache or {}

        if channel == "tickers":
            bid = float(d.get("bidPx", 0) or 0)
            ask = float(d.get("askPx", 0) or 0)
            if bid > 0:
                prev["bid1"] = bid
            if ask > 0:
                prev["ask1"] = ask
        elif channel == "mark-price":
            mp = float(d.get("markPx", 0) or 0)
            if mp > 0:
                prev["mark_price"] = mp
        elif channel == "index-tickers":
            ip = float(d.get("idxPx", 0) or 0)
            if ip > 0:
                prev["index_price"] = ip
        elif channel == "funding-rate":
            fr = d.get("fundingRate", "")
            if fr:
                prev["last_funding_rate"] = float(fr)
            resolved = self._okx_resolve_next_funding_time(d)
            if resolved > 0:
                prev["next_funding_time"] = resolved
        else:
            return None

        mark = prev.get("mark_price", 0)
        index = prev.get("index_price", 0)
        if mark == 0:
            return None
        premium = (mark - index) / index if index > 0 else 0.0
        return {
            "mark_price":        mark,
            "index_price":       index,
            "bid1":              prev.get("bid1", mark),
            "ask1":              prev.get("ask1", mark),
            "instant_premium":   premium,
            "last_funding_rate": prev.get("last_funding_rate", 0),
            "next_funding_time": prev.get("next_funding_time", 0),
            "interest_rate":     0.0001,
            "ts":                time.time(),
        }

    async def _okx_keep_alive(self, ws):
        """每 25 秒發送 OKX ping 維持連線（OKX 要求 30s 內 ping）"""
        try:
            while True:
                await asyncio.sleep(25)
                await ws.send("ping")
        except Exception:
            pass

    # ─── Gate.io 訂單簿 ──────────────────────────────────
    async def _init_gateio_depth_snapshot(self):
        """
        載入 Gate.io REST 訂單簿快照，初始化本地 book，再套用 pending diffs。
        同步規則（參照 Binance 模式）：
          1. GET /futures/usdt/order_book?with_id=true → 取得 id（快照版本）
          2. 丟棄所有 u <= id 的 pending diff 事件
          3. 之後 diff 事件正常套用
        """
        try:
            sym = self._to_gateio_symbol()
            resp = await self._client.get(
                f"{GATEIO_BASE}/futures/usdt/order_book",
                params={"contract": sym, "limit": GATEIO_DEPTH_LEVEL, "with_id": True},
            )
            resp.raise_for_status()
            snap = resp.json()
            snap_id = int(snap["id"])

            self._gateio_depth_bids = {d["p"]: float(d["s"]) for d in snap.get("bids", [])}
            self._gateio_depth_asks = {d["p"]: float(d["s"]) for d in snap.get("asks", [])}
            self._gateio_depth_last_id = snap_id

            # 套用快照期間暫存的 diff 事件
            for ev in self._gateio_depth_pending:
                if ev.get("u", 0) <= snap_id:
                    continue  # 過期，丟棄
                self._apply_gateio_diff_to_book(ev)
                self._gateio_depth_last_id = ev["u"]
            self._gateio_depth_pending.clear()
            self._gateio_depth_initialized = True
            logger.info(
                f"[GateIO Depth] 訂單簿初始化完成 id={snap_id} "
                f"bids={len(self._gateio_depth_bids)} asks={len(self._gateio_depth_asks)} "
                f"IMN={self._gateio_impact_notional:.0f} USDT quanto={self._gateio_quanto_multiplier}"
            )
            # 初始化完成後立即算一次 impact premium（_last_index_price 可能已由先前 ticker 訊息設定）
            if self._last_index_price > 0:
                premium = self._calc_gateio_impact_premium(self._last_index_price)
                if premium is not None:
                    self._ws_cache["instant_premium"] = premium
                    self._ws_cache["impact_premium_ready"] = True
        except Exception as e:
            # 失敗時 _gateio_depth_initialized 仍為 False，diff 會持續進 pending。
            # 不排重試就會永遠停在「等一個永遠不來的快照」，故 2 秒後自己再試一次。
            logger.warning(f"[GateIO Depth] 快照載入失敗: {e}，2 秒後重試")
            self._gateio_resync_task = asyncio.create_task(self._retry_gateio_snapshot())

    async def _retry_gateio_snapshot(self):
        await asyncio.sleep(2)
        if not self._gateio_depth_initialized:
            await self._init_gateio_depth_snapshot()

    def _resync_gateio_depth(self, reason: str):
        """本地簿已失真 → 重置並重抓 REST 快照（重抓期間的 diff 先進 pending）"""
        if not self._gateio_depth_initialized:
            return  # 已在重抓中，不重複排程
        logger.warning(f"[GateIO Depth] {self.symbol} 訂單簿失真（{reason}），重抓快照")
        self._gateio_depth_initialized = False
        self._gateio_depth_pending.clear()
        # 保留強引用，避免 task 被 GC 回收導致快照永遠不重建
        self._gateio_resync_task = asyncio.create_task(self._init_gateio_depth_snapshot())

    def _apply_gateio_depth_diff(self, data: dict):
        """套用 futures.order_book_update 事件到本地訂單簿"""
        if not self._gateio_depth_initialized:
            # 快照未就緒期間暫存；設上限避免快照持續失敗時無限膨脹
            if len(self._gateio_depth_pending) < 3000:
                self._gateio_depth_pending.append(data)
            return
        # 丟棄過期事件
        if data.get("u", 0) <= self._gateio_depth_last_id:
            return
        # 序號斷層：中間漏掉的增量補不回來，本地簿必然失真 → 重抓
        if data.get("U", 0) > self._gateio_depth_last_id + 1:
            self._resync_gateio_depth(f"序號斷層 U={data.get('U')} > last+1={self._gateio_depth_last_id + 1}")
            return
        self._apply_gateio_diff_to_book(data)
        self._gateio_depth_last_id = data["u"]
        # 交叉偵測：best_bid >= best_ask 代表簿裡有收不到刪除事件的幽靈檔，
        # 放著不管 impact_ask 會凍結在舊價、溢價變成假的巨大負值 → 立刻重抓自癒
        if self._gateio_depth_bids and self._gateio_depth_asks:
            best_bid = max(float(p) for p in self._gateio_depth_bids)
            best_ask = min(float(p) for p in self._gateio_depth_asks)
            if best_bid >= best_ask:
                self._resync_gateio_depth(f"本地簿交叉 bid={best_bid} >= ask={best_ask}")

    def _apply_gateio_diff_to_book(self, data: dict):
        """將單一 Gate.io diff 事件套用到 bid/ask dict；s=0 表示刪除此層"""
        for level in data.get("b", []):
            p, s = level["p"], float(level["s"])
            if s == 0:
                self._gateio_depth_bids.pop(p, None)
            else:
                self._gateio_depth_bids[p] = s
        for level in data.get("a", []):
            p, s = level["p"], float(level["s"])
            if s == 0:
                self._gateio_depth_asks.pop(p, None)
            else:
                self._gateio_depth_asks[p] = s

    def _calc_gateio_impact_premium(self, index_price: float, mark_price: Optional[float] = None) -> Optional[float]:
        """
        Gate.io 官方 premium index 計算：
        P = [max(0, ImpactBid - Index) - max(0, Index - ImpactAsk)] / Index

        IMN 使用 USDT 名義金額（funding_impact_value，由合約 API 直接給出）。
        每張合約名義金額 = price × quanto_multiplier，因此 size 張合約對應的
        USDT 名義 = size × quanto × price。走訂單簿時累積 USDT 名義至 IMN。
        例：BTC  IMN=30000 USDT,  quanto=0.0001
            ETH  IMN=20000 USDT,  quanto=0.01
            HOOK IMN=2000  USDT,  quanto=1

        commodities 合約特例（NATGAS 等大宗商品映射）：
        實測現象：訂單簿 IMN-based impact premium 對流動性差的合約會在
        ImpactBid<Index<ImpactAsk 時邏輯地歸零，但 funding_rate_indicative 持續
        反映非零 premium（NGUSDT 反推 P≈0.16% vs 我們算 0%）。
        近似修正：改用 (mark - index) / index——實測誤差約 0.02%（遠優於原本 0.2%）。
        注意：Gate.io 官方文檔未明說 commodities 有專屬公式，此修正基於實測反推、
        並非文檔證實的演算法。仍以 funding_rate_indicative 為最終真相校驗。
        """
        if self._gateio_is_commodity:
            # 優先用呼叫端傳入的最新 mark（剛 parse 完的 ticker 比 _ws_cache 新一個訊息）
            mark = mark_price if mark_price is not None else float(self._ws_cache.get("mark_price", 0) or 0)
            if mark <= 0 or index_price <= 0:
                return None
            return (mark - index_price) / index_price

        if not self._gateio_depth_bids or not self._gateio_depth_asks or index_price <= 0:
            return None

        target_notional = self._gateio_impact_notional  # USDT 名義金額
        quanto = self._gateio_quanto_multiplier

        sorted_bids = sorted(self._gateio_depth_bids.items(), key=lambda x: -float(x[0]))
        sorted_asks = sorted(self._gateio_depth_asks.items(), key=lambda x: float(x[0]))

        def fill_avg_price(levels) -> float:
            """走訂單簿，填滿 target_notional USDT 名義，返回加權平均成交價"""
            acc_notional = 0.0   # 累積 USDT 名義金額
            acc_qty = 0.0        # 累積基礎資產數量（contracts × quanto）
            last_price = 0.0
            for p_str, size in levels:
                price = float(p_str)
                last_price = price
                level_notional = size * quanto * price  # 該檔的 USDT 名義
                if acc_notional + level_notional >= target_notional:
                    remain = target_notional - acc_notional
                    acc_qty += remain / price
                    acc_notional = target_notional
                    break
                else:
                    acc_notional += level_notional
                    acc_qty += size * quanto
            if acc_qty <= 0:
                return last_price if last_price > 0 else 0.0
            if acc_notional < target_notional:
                return last_price  # 訂單簿不夠深，返回最末價
            return acc_notional / acc_qty

        impact_bid = fill_avg_price(sorted_bids)
        impact_ask = fill_avg_price(sorted_asks)
        return (max(0.0, impact_bid - index_price) - max(0.0, index_price - impact_ask)) / index_price

    # ─── 訂單簿 / Impact Premium（Bybit 專用）──────────────
    def _apply_bybit_depth(self, msg: dict):
        """套用 Bybit orderbook.50 snapshot/delta 到本地訂單簿"""
        msg_type = msg.get("type")
        data = msg.get("data", {})
        bids = data.get("b", [])
        asks = data.get("a", [])
        if msg_type == "snapshot":
            self._bybit_depth_bids = {p: float(s) for p, s in bids if float(s) > 0}
            self._bybit_depth_asks = {p: float(s) for p, s in asks if float(s) > 0}
            self._bybit_depth_initialized = True
            logger.info(
                f"[BybitDepth] 訂單簿初始化完成 "
                f"bids={len(self._bybit_depth_bids)} asks={len(self._bybit_depth_asks)}"
            )
        elif msg_type == "delta" and self._bybit_depth_initialized:
            for p, s in bids:
                if float(s) == 0:
                    self._bybit_depth_bids.pop(p, None)
                else:
                    self._bybit_depth_bids[p] = float(s)
            for p, s in asks:
                if float(s) == 0:
                    self._bybit_depth_asks.pop(p, None)
                else:
                    self._bybit_depth_asks[p] = float(s)

    def _calc_bybit_impact_premium(self, mark: float, index: float, override_imn: float = None) -> Optional[float]:
        """
        Bybit premium index 計算：
        P = [Max(0, ImpactBid - Index) - Max(0, Index - ImpactAsk)] / Index

        ImpactBid = 賣出 IMN USDT 的平均成交價（走 bids 降序）
        ImpactAsk = 買入 IMN USDT 的平均成交價（走 asks 升序）

        注意：Bybit 官方基準為 Index Price。
        若使用 Mark Price，因 Mark 本身受 Perp 影響，會導致折價時 ImpactAsk 輕易超過 Mark 算出 P=0。
        改用 Index（spot 基準）才能正確反映資金費率貼 cap 的極端折價狀態。
        """
        if not self._bybit_depth_bids or not self._bybit_depth_asks or index <= 0:
            return None

        target = override_imn if override_imn is not None else self._impact_notional
        sorted_bids = sorted(self._bybit_depth_bids.items(), key=lambda x: -float(x[0]))
        sorted_asks = sorted(self._bybit_depth_asks.items(), key=lambda x: float(x[0]))

        def fill_avg_price(levels, target_notional) -> float:
            acc_notional = 0.0
            acc_qty = 0.0
            last_price = 0.0
            for p_str, qty in levels:
                price = float(p_str)
                last_price = price
                level_notional = price * qty
                if acc_notional + level_notional >= target_notional:
                    remaining = target_notional - acc_notional
                    acc_qty += remaining / price
                    acc_notional = target_notional
                    break
                else:
                    acc_notional += level_notional
                    acc_qty += qty
            if acc_qty <= 0:
                return last_price if last_price > 0 else 0.0
            if acc_notional < target_notional:
                return last_price
            return acc_notional / acc_qty

        impact_bid = fill_avg_price(sorted_bids, target)
        impact_ask = fill_avg_price(sorted_asks, target)

        # 診斷 Log：顯示 mark/index 差異 + impact prices + 結算溢價（每 30 秒一次）
        import time as _time
        if override_imn is None:
            _now = _time.monotonic()
            if _now - self._bybit_impact_log_ts >= 30.0:
                self._bybit_impact_log_ts = _now
                p_raw = (max(0.0, impact_bid - index) - max(0.0, index - impact_ask)) / index
                logger.info(
                    f"[BybitImpact] {self.symbol} "
                    f"mark={mark:.6f} idx={index:.6f} "
                    f"bid={impact_bid:.6f} ask={impact_ask:.6f} "
                    f"P={p_raw*100:.4f}% IMN={target:.0f}"
                )

        if override_imn is None:
            self._last_impact_bid = impact_bid
            self._last_impact_ask = impact_ask
        return (max(0.0, impact_bid - index) - max(0.0, index - impact_ask)) / index

    # ─── 訂單簿 / Impact Premium（OKX 專用）─────────────

    def _apply_okx_depth(self, msg: dict):
        """套用 OKX books snapshot/update 到本地訂單簿"""
        action = msg.get("action")
        data_list = msg.get("data", [])
        if not data_list:
            return
        data = data_list[0]
        bids = data.get("bids", [])  # [[price, qty, _, numOrders], ...]
        asks = data.get("asks", [])
        if action == "snapshot":
            self._okx_depth_bids = {str(b[0]): float(b[1]) for b in bids if len(b) >= 2 and float(b[1]) > 0}
            self._okx_depth_asks = {str(a[0]): float(a[1]) for a in asks if len(a) >= 2 and float(a[1]) > 0}
            self._okx_depth_initialized = True
            logger.info(
                f"[OKXDepth] 訂單簿初始化完成 "
                f"bids={len(self._okx_depth_bids)} asks={len(self._okx_depth_asks)}"
            )
        elif action == "update" and self._okx_depth_initialized:
            for b in bids:
                if len(b) < 2:
                    continue
                p_str = str(b[0])
                if float(b[1]) == 0:
                    self._okx_depth_bids.pop(p_str, None)
                else:
                    self._okx_depth_bids[p_str] = float(b[1])
            for a in asks:
                if len(a) < 2:
                    continue
                p_str = str(a[0])
                if float(a[1]) == 0:
                    self._okx_depth_asks.pop(p_str, None)
                else:
                    self._okx_depth_asks[p_str] = float(a[1])

    def _calc_okx_impact_premium(self, index: float, override_imn: float = None) -> Optional[float]:
        """
        OKX premium index 計算（OKX 對所有合約使用同一套公式，含 TradFi）：
        P = [Max(0, ImpactBid - Index) - Max(0, Index - ImpactAsk)] / Index
        OKX 訂單簿 qty 單位為幣數量（非合約數），直接以 USDT notional 計算
        文檔：https://www.okx.com/help/perps-funding-fee-mechanism
        """
        if not self._okx_depth_bids or not self._okx_depth_asks or index <= 0:
            return None

        target = override_imn if override_imn is not None else self._impact_notional
        sorted_bids = sorted(self._okx_depth_bids.items(), key=lambda x: -float(x[0]))
        sorted_asks = sorted(self._okx_depth_asks.items(), key=lambda x: float(x[0]))

        def fill_avg_price(levels, target_notional) -> float:
            acc_notional = 0.0
            acc_qty = 0.0
            last_price = 0.0
            for p_str, qty in levels:
                price = float(p_str)
                last_price = price
                level_notional = price * qty
                if acc_notional + level_notional >= target_notional:
                    remaining = target_notional - acc_notional
                    acc_qty += remaining / price
                    acc_notional = target_notional
                    break
                else:
                    acc_notional += level_notional
                    acc_qty += qty
            if acc_qty <= 0:
                return last_price if last_price > 0 else 0.0
            if acc_notional < target_notional:
                return last_price
            return acc_notional / acc_qty

        impact_bid = fill_avg_price(sorted_bids, target)
        impact_ask = fill_avg_price(sorted_asks, target)

        if override_imn is None:
            self._last_impact_bid = impact_bid
            self._last_impact_ask = impact_ask
        return (max(0.0, impact_bid - index) - max(0.0, index - impact_ask)) / index

    # ─── 訂單簿 / Impact Premium（Bitget 專用）────────────

    def _apply_bitget_depth(self, msg: dict):
        """套用 Bitget books snapshot/update 到本地訂單簿"""
        action = msg.get("action")
        data_list = msg.get("data", [])
        if not data_list:
            return
        data = data_list[0]
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if action == "snapshot":
            self._bitget_depth_bids = {str(p): float(q) for p, q in bids if float(q) > 0}
            self._bitget_depth_asks = {str(p): float(q) for p, q in asks if float(q) > 0}
            self._bitget_depth_initialized = True
            logger.info(
                f"[BitgetDepth] 訂單簿初始化完成 "
                f"bids={len(self._bitget_depth_bids)} asks={len(self._bitget_depth_asks)}"
            )
        elif action == "update" and self._bitget_depth_initialized:
            for p, q in bids:
                p_str = str(p)
                if float(q) == 0:
                    self._bitget_depth_bids.pop(p_str, None)
                else:
                    self._bitget_depth_bids[p_str] = float(q)
            for p, q in asks:
                p_str = str(p)
                if float(q) == 0:
                    self._bitget_depth_asks.pop(p_str, None)
                else:
                    self._bitget_depth_asks[p_str] = float(q)

    def _calc_bitget_impact_premium(self, mark: float, index: float, override_imn: float = None) -> Optional[float]:
        """
        Bitget premium index 計算：
        P = [Max(0, ImpactBid - Index) - Max(0, Index - ImpactAsk)] / Index
        ImpactBid = 賣出 IMN USDT 的平均成交價（bid 降序）
        ImpactAsk = 買入 IMN USDT 的平均成交價（ask 升序）
        IMN = 200 USDT（Bitget 標準值）
        qty 單位 = 合約數量（sizeMultiplier=1 → 1 contract = 1 幣）
        """
        if not self._bitget_depth_bids or not self._bitget_depth_asks or index <= 0:
            return None

        imn = override_imn if override_imn is not None else self._impact_notional
        sorted_bids = sorted(self._bitget_depth_bids.items(), key=lambda x: -float(x[0]))
        sorted_asks = sorted(self._bitget_depth_asks.items(), key=lambda x: float(x[0]))

        def fill_avg_price(levels, target_notional) -> float:
            acc_notional = 0.0
            acc_qty = 0.0
            last_price = 0.0
            for p_str, qty in levels:
                price = float(p_str)
                last_price = price
                level_notional = price * qty  # qty = contracts = coins (sizeMultiplier=1)
                if acc_notional + level_notional >= target_notional:
                    remaining = target_notional - acc_notional
                    acc_qty += remaining / price
                    acc_notional = target_notional
                    break
                else:
                    acc_notional += level_notional
                    acc_qty += qty
            if acc_qty <= 0:
                return last_price if last_price > 0 else 0.0
            if acc_notional < target_notional:
                return last_price
            return acc_notional / acc_qty

        impact_bid = fill_avg_price(sorted_bids, imn)
        impact_ask = fill_avg_price(sorted_asks, imn)
        return (max(0.0, impact_bid - index) - max(0.0, index - impact_ask)) / index

    # ─── 訂單簿 / Impact Premium（Binance 專用）────────────
    async def _init_depth_snapshot(self):
        """
        載入 REST 完整訂單簿快照（limit=1000），初始化本地 book。
        Binance diff stream 同步規則：
          1. 快照的 lastUpdateId = L
          2. 丟棄所有 u <= L 的 diff 事件
          3. 第一個合法事件：U <= L+1 <= u
          4. 之後每個事件：pu == 上一個事件的 u
        """
        try:
            resp = await self._client.get(
                f"{self._fapi_base()}/fapi/v1/depth",
                params={"symbol": self.symbol, "limit": 1000},
            )
            resp.raise_for_status()
            snap = resp.json()
            last_id = snap["lastUpdateId"]

            self._depth_bids = {p: float(q) for p, q in snap["bids"]}
            self._depth_asks = {p: float(q) for p, q in snap["asks"]}
            self._depth_last_update_id = last_id

            # 套用快照期間暫存的 diff 事件
            for ev in self._depth_pending:
                if ev["u"] <= last_id:
                    continue  # 過期，丟棄
                self._apply_diff_to_book(ev)
                self._depth_last_update_id = ev["u"]
            self._depth_pending.clear()
            self._depth_initialized = True
            logger.info(
                f"[Depth] 訂單簿初始化完成 lastUpdateId={last_id} "
                f"bids={len(self._depth_bids)} asks={len(self._depth_asks)}"
            )
        except Exception as e:
            logger.warning(f"[Depth] 快照載入失敗: {e}")

    def _apply_depth_diff(self, data: dict):
        """套用 diff depth 事件到本地訂單簿"""
        if not self._depth_initialized:
            self._depth_pending.append(data)
            return
        # 丟棄過期事件
        if data.get("u", 0) <= self._depth_last_update_id:
            return
        self._apply_diff_to_book(data)
        self._depth_last_update_id = data["u"]

    def _apply_diff_to_book(self, data: dict):
        """將單一 diff 事件套用到 bid/ask dict；qty=0 表示刪除此層"""
        for p, q in data.get("b", []):
            if float(q) == 0:
                self._depth_bids.pop(p, None)
            else:
                self._depth_bids[p] = float(q)
        for p, q in data.get("a", []):
            if float(q) == 0:
                self._depth_asks.pop(p, None)
            else:
                self._depth_asks[p] = float(q)

    def _calc_impact_premium(self, index_price: float, override_imn: float = None) -> Optional[float]:
        """
        Binance 真實 premium index 計算：
        P = [max(0, ImpactBid - Index) - max(0, Index - ImpactAsk)] / Index

        ImpactBid = 賣出 self._impact_notional USDT 的平均成交價（走 bids 降序）
        ImpactAsk = 買入 self._impact_notional USDT 的平均成交價（走 asks 升序）
        """
        if not self._depth_bids or not self._depth_asks or index_price <= 0:
            return None

        target = override_imn if override_imn is not None else self._impact_notional

        # dict → sorted list（每次計算時排序；dict 維護方便，排序成本可接受）
        sorted_bids = sorted(self._depth_bids.items(), key=lambda x: -float(x[0]))
        sorted_asks = sorted(self._depth_asks.items(), key=lambda x: float(x[0]))

        def fill_avg_price(levels, target_notional) -> float:
            acc_notional = 0.0
            acc_qty = 0.0
            last_price = 0.0
            for p_str, qty in levels:
                price = float(p_str)
                last_price = price
                level_notional = price * qty
                if acc_notional + level_notional >= target_notional:
                    remaining = target_notional - acc_notional
                    acc_qty += remaining / price
                    acc_notional = target_notional
                    break
                else:
                    acc_notional += level_notional
                    acc_qty += qty
            
            if acc_qty <= 0:
                return last_price if last_price > 0 else 0.0
            
            # 如果深度不足以填滿 target_notional，回傳最後一檔價格作為近似
            if acc_notional < target_notional:
                return last_price
                
            return acc_notional / acc_qty

        impact_bid = fill_avg_price(sorted_bids, target)
        impact_ask = fill_avg_price(sorted_asks, target)
        p_result = (max(0.0, impact_bid - index_price) - max(0.0, index_price - impact_ask)) / index_price
        
        # 只有在非 override 模式下才更新 debug 變數
        if override_imn is None:
            self._last_impact_bid = impact_bid
            self._last_impact_ask = impact_ask
            
        return p_result

    def calibrate_imn(self, target_p: float, index_price: float) -> float:
        """
        利用 inferred_P 反推正確的 Impact Margin Notional (IMN)。
        對目標溢價 target_p 做二分搜尋，找出最接近的 IMN。
        範圍：1,000 USDT ~ 2,000,000 USDT
        """
        if abs(target_p) < 0.00005: # 溢價太小時（<0.005%）深度影響不明顯，不校正
            return self._impact_notional

        lo, hi = 1000.0, 2000000.0
        best_imn = self._impact_notional
        min_diff = float('inf')
        mark_price = self._ws_cache.get("mark_price", index_price) if self._ws_cache else index_price

        # 二分搜尋 20 次，精度足夠
        for _ in range(20):
            mid = (lo + hi) / 2
            if self.exchange == "bybit":
                p_check = self._calc_bybit_impact_premium(mark_price, index_price, override_imn=mid)
            elif self.exchange == "bitget":
                p_check = self._calc_bitget_impact_premium(mark_price, index_price, override_imn=mid)
            elif self.exchange in ("binance", "aster"):
                p_check = self._calc_impact_premium(index_price, override_imn=mid)
            elif self.exchange == "okx":
                p_check = self._calc_okx_impact_premium(index_price, override_imn=mid)
            else:
                p_check = None
                
            if p_check is None:
                break
            
            diff = abs(p_check - target_p)
            if diff < min_diff:
                min_diff = diff
                best_imn = mid
            
            # 單調性：IMN 越大 → 填更深訂單簿 → ImpactAsk/ImpactBid 更貼近 index → |P| 越小
            # 所以 |p_check| > |target_p| 時需要更大 IMN（lo=mid），反之 hi=mid
            if abs(p_check) > abs(target_p):
                lo = mid
            else:
                hi = mid
        
        return best_imn

    async def _bitget_signed_get(self, path: str, params: dict) -> dict:
        """帶 HMAC 簽名的 Bitget V2 認證 GET 請求"""
        ts = str(int(time.time() * 1000))
        qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        prehash = ts + "GET" + path + ("?" + qs if qs else "")
        sig = hmac.new(
            self._api_secret.encode(),
            prehash.encode(),
            hashlib.sha256,
        ).digest()
        sig_b64 = base64.b64encode(sig).decode()
        headers = {
            "ACCESS-KEY": self._api_key,
            "ACCESS-SIGN": sig_b64,
            "ACCESS-TIMESTAMP": ts,
            "ACCESS-PASSPHRASE": self._api_passphrase or "",
        }
        url = f"{BITGET_BASE}{path}" + ("?" + qs if qs else "")
        resp = await self._client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def _binance_signed_get(self, path: str, params: dict) -> dict:
        """帶 HMAC 簽名的 Binance 認證 GET 請求"""
        ts = int(time.time() * 1000)
        params["timestamp"] = ts
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        sig = hmac.new(
            self._api_secret.encode(),
            qs.encode(),
            hashlib.sha256,
        ).hexdigest()
        url = f"{self._fapi_base()}{path}?{qs}&signature={sig}"
        resp = await self._client.get(url, headers={"X-MBX-APIKEY": self._api_key})
        resp.raise_for_status()
        return resp.json()

    async def refresh_settings(self) -> tuple:
        """重新偵測窗口與 Cap 並更新內部狀態"""
        try:
            w, c = await self._detect_window_and_cap()
            self._window_minutes = w
            self._cap = c
            return w, c
        except Exception as e:
            logger.warning(f"重新偵測 {self.exchange} 設定失敗: {e}")
            return self._window_minutes, self._cap

    # ─── 結算週期 + Cap 偵測（REST）──────────────────────
    async def _detect_window_and_cap(self) -> tuple:
        if self.exchange == "binance":
            cap = None
            window = 480
            found_window = False
            try:
                fi = await self._client.get(f"{BINANCE_BASE}/fapi/v1/fundingInfo")
                fi.raise_for_status()
                for item in fi.json():
                    if item.get("symbol") == self.symbol:
                        cap_str = item.get("adjustedFundingRateCap", "")
                        cap = float(cap_str) if cap_str else None
                        # 優先從 fundingInfo 讀取結算週期（小時）
                        interval_hours = item.get("fundingIntervalHours")
                        if interval_hours:
                            window = int(interval_hours) * 60
                            found_window = True
                        break
            except Exception:
                pass

            # 取得 Impact Margin Notional：IMN = 200 USDT / Initial Margin Rate at maximum leverage
            # 官方公式：IMR = 1 / initialLeverage → IMN = 200 * initialLeverage
            # 優先：leverageBracket API（認證，Tier-1 initialLeverage 精確）
            # Fallback：liquidationFee（近似值，誤差較大）
            imn_set = False
            if self._api_key and self._api_secret:
                try:
                    bracket_data = await self._binance_signed_get(
                        "/fapi/v1/leverageBracket",
                        {"symbol": self.symbol},
                    )
                    for entry in bracket_data:
                        if entry.get("symbol") == self.symbol:
                            brackets = entry.get("brackets", [])
                            if brackets:
                                # Tier-1 = 第一個 bracket（最低 notional），取最大槓桿
                                # IMR = 1 / initialLeverage；IMN = 200 / IMR = 200 × initialLeverage
                                init_lev = float(brackets[0].get("initialLeverage", 0))
                                if init_lev > 0:
                                    self._impact_notional = 200.0 * init_lev
                                    logger.info(
                                        f"[{self.symbol}] leverageBracket Tier-1 maxLev={init_lev}x"
                                        f" → IMR={1/init_lev*100:.4f}% → IMN={self._impact_notional:.0f} USDT"
                                    )
                                    imn_set = True
                            break
                except Exception as e:
                    logger.warning(f"[{self.symbol}] leverageBracket 取得失敗: {e}，退回 liquidationFee")

            if not imn_set:
                try:
                    ei = await self._client.get(
                        f"{BINANCE_BASE}/fapi/v1/exchangeInfo",
                        params={"symbol": self.symbol},
                    )
                    ei.raise_for_status()
                    for sym_info in ei.json().get("symbols", []):
                        if sym_info.get("symbol") == self.symbol:
                            liq_fee = float(sym_info.get("liquidationFee", 0) or 0)
                            if liq_fee > 0:
                                self._impact_notional = 200.0 / liq_fee
                                logger.warning(
                                    f"[{self.symbol}] 使用 liquidationFee={liq_fee*100:.2f}%"
                                    f" → IMN={self._impact_notional:.0f} USDT（非精確 MMR，誤差較大）"
                                )
                            break
                except Exception as e:
                    logger.warning(f"取得 liquidationFee 失敗，使用預設 IMN=200: {e}")

            # 如果 fundingInfo 沒拿到 window（罕見），則回退到歷史計算
            if not found_window:
                try:
                    hist = await self._client.get(
                        f"{BINANCE_BASE}/fapi/v1/fundingRate",
                        params={"symbol": self.symbol, "limit": 3},
                    )
                    hist.raise_for_status()
                    times = [int(d["fundingTime"]) for d in hist.json()]
                    if len(times) >= 2:
                        window = max(60, round((times[-1] - times[-2]) / 60000))
                except Exception:
                    pass
            return window, cap

        elif self.exchange == "aster":
            # Aster API 格式與 Binance 相同
            cap = None
            window = 480
            # 從 fundingInfo 取 cap 與結算窗口（fundingFeeCap / fundingIntervalHours）
            try:
                fi = await self._client.get(f"{ASTER_BASE}/fapi/v1/fundingInfo")
                fi.raise_for_status()
                for item in fi.json():
                    if item.get("symbol") == self.symbol:
                        fee_cap = float(item.get("fundingFeeCap", 0) or 0)
                        if fee_cap > 0:
                            cap = fee_cap
                        interval_hours = item.get("fundingIntervalHours")
                        if interval_hours:
                            window = int(interval_hours) * 60
                        break
                logger.info(f"[Aster] {self.symbol} fundingInfo → window={window}min cap={cap}")
            except Exception as e:
                logger.warning(f"[Aster] fundingInfo 失敗: {e}")
            # IMN = 200 * initialLeverage（Tier-1，與 Binance 相同公式）
            # 優先：leverageBracket API（認證）；Fallback：liquidationFee（近似）
            imn_set = False
            if self._api_key and self._api_secret:
                try:
                    bracket_data = await self._binance_signed_get(
                        "/fapi/v1/leverageBracket",
                        {"symbol": self.symbol},
                    )
                    for entry in bracket_data:
                        if entry.get("symbol") == self.symbol:
                            brackets = entry.get("brackets", [])
                            if brackets:
                                init_lev = float(brackets[0].get("initialLeverage", 0))
                                if init_lev > 0:
                                    self._impact_notional = 200.0 * init_lev
                                    logger.info(
                                        f"[Aster] {self.symbol} leverageBracket Tier-1 maxLev={init_lev}x"
                                        f" → IMN={self._impact_notional:.0f} USDT"
                                    )
                                    imn_set = True
                            break
                except Exception as e:
                    logger.warning(f"[Aster] leverageBracket 失敗: {e}，退回 liquidationFee")
            if not imn_set:
                try:
                    ei = await self._client.get(
                        f"{ASTER_BASE}/fapi/v1/exchangeInfo",
                        params={"symbol": self.symbol},
                    )
                    ei.raise_for_status()
                    for sym_info in ei.json().get("symbols", []):
                        if sym_info.get("symbol") == self.symbol:
                            liq_fee = float(sym_info.get("liquidationFee", 0) or 0)
                            if liq_fee > 0:
                                self._impact_notional = 200.0 / liq_fee
                                logger.info(
                                    f"[Aster] {self.symbol} liquidationFee fallback={liq_fee*100:.2f}%"
                                    f" → IMN={self._impact_notional:.0f} USDT"
                                )
                            break
                except Exception as e:
                    logger.warning(f"[Aster] IMN 取得失敗: {e}")
            return window, cap

        elif self.exchange == "bybit":
            resp = await self._client.get(
                f"{BYBIT_BASE}/v5/market/instruments-info",
                params={"category": "linear", "symbol": self.symbol},
            )
            resp.raise_for_status()
            items = resp.json().get("result", {}).get("list", [])
            if items:
                interval = int(items[0].get("fundingInterval", 480))
                upper = items[0].get("upperFundingRate", "")
                cap = float(upper) if upper else None
            # IMN = 200 USDT / Tier-1 Initial Margin Rate（Bybit 官方公式）
            try:
                risk_resp = await self._client.get(
                    f"{BYBIT_BASE}/v5/market/risk-limit",
                    params={"category": "linear", "symbol": self.symbol},
                )
                risk_resp.raise_for_status()
                risk_list = risk_resp.json().get("result", {}).get("list", [])
                tier1 = next((r for r in risk_list if r.get("isLowestRisk") == 1), risk_list[0] if risk_list else None)
                if tier1:
                    imr = float(tier1.get("initialMargin", 0))
                    if imr > 0:
                        self._impact_notional = 200.0 / imr
                        logger.info(f"[Bybit] {self.symbol} Tier-1 IMR={imr*100:.2f}% → IMN={self._impact_notional:.0f} USDT")
            except Exception as e:
                logger.warning(f"[Bybit] IMN 取得失敗，用預設 200: {e}")
            return max(60, interval), cap

        elif self.exchange == "bitget":
            resp = await self._client.get(
                f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
                params={"symbol": self.symbol, "productType": "USDT-FUTURES"},
            )
            resp.raise_for_status()
            data = resp.json().get("data", [{}])
            if data:
                interval = int(data[0].get("fundingRateInterval", 8)) * 60
                max_rate_str = data[0].get("maxFundingRate", "")
                cap = float(max_rate_str) if max_rate_str else None
            else:
                interval, cap = 480, None
            # IMN = 200 USDT / Tier-1 keepMarginRate
            try:
                lever_params = {"symbol": self.symbol, "productType": "USDT-FUTURES", "marginCoin": "USDT"}
                if self._api_key and self._api_secret:
                    lever_data = await self._bitget_signed_get("/api/v2/mix/market/query-position-lever", lever_params)
                else:
                    r = await self._client.get(f"{BITGET_BASE}/api/v2/mix/market/query-position-lever", params=lever_params)
                    r.raise_for_status()
                    lever_data = r.json()
                tiers = lever_data.get("data", [])
                tier1 = next((t for t in tiers if str(t.get("level", "")) == "1"), None)
                if tier1:
                    mmr = float(tier1.get("keepMarginRate", 0.005) or 0.005)
                    self._impact_notional = 200.0 / mmr
                    logger.info(f"[Bitget] {self.symbol} Tier-1 MMR={mmr*100:.2f}% → IMN={self._impact_notional:.0f} USDT")
            except Exception as e:
                logger.warning(f"[Bitget] IMN 取得失敗，用預設 200: {e}")
            return interval, cap

        elif self.exchange == "gateio":
            sym = self._to_gateio_symbol()
            resp = await self._client.get(f"{GATEIO_BASE}/futures/usdt/contracts/{sym}")
            resp.raise_for_status()
            d = resp.json()
            interval_sec = int(d.get("funding_interval", 28800) or 28800)
            window = max(60, interval_sec // 60)
            # 記錄 Gate 真實下次結算時間（秒→毫秒）；WS 無此欄位，只能從合約端取得
            fna = int(d.get("funding_next_apply", 0) or 0)
            self._gateio_next_apply_ms = fna * 1000 if fna > 0 else 0
            # 資費率上限：直接使用 funding_rate_limit（Gate.io 定義的每結算週期實際上限）
            # cap_ratio×maintenance_rate 對部分幣種（如 ESP）會低估上限，不採用
            rate_limit = float(d.get("funding_rate_limit", 0) or 0)
            cap = rate_limit if rate_limit > 0 else None
            # 儲存 quanto_multiplier 及 funding_impact_value（USDT 名義金額，直接從 API 取得）
            # funding_impact_value：Gate.io 定義的 impact margin USDT 名義金額
            # 例：HOOK=2000 USDT, BTC=30000 USDT, ETH=20000 USDT, AIGENSYN=5000 USDT
            self._gateio_quanto_multiplier = float(d.get("quanto_multiplier", 1) or 1)
            self._gateio_impact_notional = float(d.get("funding_impact_value", 2000) or 2000)
            # 計算利率組件 I（每結算週期）= 日利率 / 每日結算次數
            # 例：HOOK interest_rate=0.0003（0.03% daily），1h 結算 → I = 0.0003/24 = 0.0000125
            # 注意：commodities 合約 interest_rate=0，不能用 `or 0.0003` fallback（0 是合法值）
            ir_raw = d.get("interest_rate")
            contract_ir = float(ir_raw) if ir_raw is not None and ir_raw != "" else 0.0003
            periods_per_day = 86400 / interval_sec
            # 溢價=0 哨兵（feed 失靈偵測用）：官方費率在 premium_index=0 時會收斂到此值
            self._gateio_min_rate = contract_ir / periods_per_day
            # 新算法（公告 51275）引擎基礎利率：8h 等效 = 每日利率 × 8/24（= contract_ir/3）。
            # 週期縮放由 funding_engine 的 result_scale = N/8 處理，故此處不按週期縮放。
            # 例：daily 0.03% → 0.01%；commodities daily 0 → 0。
            self._gateio_base_interest = contract_ir * 8 / 24
            # commodities 合約偵測（NATGAS、GAS 等大宗商品映射）
            # 特性：mark_type="index"（mark 直接取 last_price 而非訂單簿衍生），訂單簿 impact 公式不適用
            self._gateio_is_commodity = (d.get("contract_type", "") == "commodities")
            logger.info(
                f"[Gate.io] {self.symbol} 窗口={window}min  "
                f"cap={cap*100:.4f}%  "
                f"I={self._gateio_min_rate*100:.5f}%/period  "
                f"quanto={self._gateio_quanto_multiplier}  IMN={self._gateio_impact_notional:.0f} USDT  "
                f"commodity={self._gateio_is_commodity}"
                if cap else
                f"[Gate.io] {self.symbol} 窗口={window}min  cap 無法計算，使用預設  "
                f"I={self._gateio_min_rate*100:.5f}%/period  "
                f"quanto={self._gateio_quanto_multiplier}  IMN={self._gateio_impact_notional:.0f} USDT  "
                f"commodity={self._gateio_is_commodity}"
            )
            return window, cap

        elif self.exchange == "okx":
            inst_id = self._to_okx_swap_symbol()
            idx_id  = self._to_okx_index_symbol()
            window = 480
            cap = None
            # funding-rate API 一次取得 cap + 結算週期 + IMN
            try:
                fr_resp = await self._client.get(
                    f"{OKX_BASE}/api/v5/public/funding-rate",
                    params={"instId": inst_id},
                )
                fr_resp.raise_for_status()
                fr_data = fr_resp.json().get("data", [])
                if fr_data:
                    d = fr_data[0]
                    # cap
                    max_rate = d.get("maxFundingRate", "")
                    if max_rate:
                        cap = abs(float(max_rate))
                    # 結算週期：從 fundingTime - prevFundingTime 推算
                    ft = int(d.get("fundingTime", 0) or 0)
                    pft = int(d.get("prevFundingTime", 0) or 0)
                    if ft > 0 and pft > 0 and ft > pft:
                        interval_ms = ft - pft
                        window = max(60, interval_ms // 60000)
                    # IMN：OKX funding-rate 直接提供 impactValue（USDT）
                    impact_val = d.get("impactValue", "")
                    if impact_val:
                        self._impact_notional = float(impact_val)
                    # OKX interestRate：每個合約獨立（BTC 8h=0.01%、GAS=0.005%、TradFi=0），
                    # 單位已是「每結算週期」，OKX 已自動縮放，不需再依 window 縮放。
                    # 引擎優先用此值（覆蓋 EXCHANGE_CONFIG 的 scale_by_interval 邏輯）。
                    ir_raw = d.get("interestRate", "")
                    if ir_raw != "" and ir_raw is not None:
                        self._okx_interest_rate = float(ir_raw)
            except Exception as e:
                logger.warning(f"[OKX] funding-rate 取得失敗: {e}")
            # fallback IMN：從 position-tiers API（需用 uly 參數）
            if self._impact_notional <= 200.0:
                try:
                    tier_resp = await self._client.get(
                        f"{OKX_BASE}/api/v5/public/position-tiers",
                        params={"instType": "SWAP", "uly": idx_id, "tdMode": "cross"},
                    )
                    tier_resp.raise_for_status()
                    tiers = tier_resp.json().get("data", [])
                    if tiers:
                        imr = float(tiers[0].get("imr", 0) or 0)
                        if imr > 0:
                            self._impact_notional = 200.0 / imr
                except Exception as e:
                    logger.warning(f"[OKX] position-tiers fallback 失敗: {e}")
            ir_disp = f"{self._okx_interest_rate*100:.5f}%" if self._okx_interest_rate is not None else "預設"
            logger.info(
                f"[OKX] {self.symbol} 窗口={window}min  "
                f"cap={cap*100:.4f}%  I={ir_disp}/period  IMN={self._impact_notional:.0f} USDT"
                if cap else
                f"[OKX] {self.symbol} 窗口={window}min  cap=None（用預設）  "
                f"I={ir_disp}/period  IMN={self._impact_notional:.0f} USDT"
            )
            return window, cap

        return 480, None

    # ─── REST Ticker fallback ────────────────────────────
    async def _binance_ticker(self) -> dict:
        base = self._fapi_base()
        url = f"{base}/fapi/v1/premiumIndex"
        resp = await self._client.get(url, params={"symbol": self.symbol})
        resp.raise_for_status()
        d = resp.json()
        mark  = float(d.get("markPrice", 0))
        index = float(d.get("indexPrice", 0))
        premium = (mark - index) / index if index > 0 else 0.0
        bid1, ask1 = mark, mark
        try:
            ob = await self._client.get(
                f"{base}/fapi/v1/depth",
                params={"symbol": self.symbol, "limit": 5},
            )
            ob.raise_for_status()
            ob_data = ob.json()
            if ob_data.get("bids"):
                bid1 = float(ob_data["bids"][0][0])
            if ob_data.get("asks"):
                ask1 = float(ob_data["asks"][0][0])
        except Exception:
            pass
        return {
            "mark_price": mark, "index_price": index,
            "bid1": bid1, "ask1": ask1,
            "instant_premium": premium,
            "last_funding_rate": float(d.get("lastFundingRate", 0)),
            "next_funding_time": int(d.get("nextFundingTime", 0)),
            "interest_rate": float(d.get("interestRate", 0.0001)),
            "ts": time.time(),
        }

    async def _bybit_ticker(self) -> dict:
        url = f"{BYBIT_BASE}/v5/market/tickers"
        resp = await self._client.get(url, params={"category": "linear", "symbol": self.symbol})
        resp.raise_for_status()
        items = resp.json().get("result", {}).get("list", [])
        if not items:
            return self._empty_ticker()
        t = items[0]
        mark  = float(t.get("markPrice", 0))
        index = float(t.get("indexPrice", 0))
        return {
            "mark_price": mark, "index_price": index,
            "bid1": float(t.get("bid1Price", mark)),
            "ask1": float(t.get("ask1Price", mark)),
            "instant_premium": (mark - index) / index if index > 0 else 0.0,
            "last_funding_rate": float(t.get("fundingRate", 0)),
            "next_funding_time": int(t.get("nextFundingTime", 0)),
            "interest_rate": 0.0001,
            "ts": time.time(),
        }

    async def _bitget_ticker(self) -> dict:
        ticker_url = f"{BITGET_BASE}/api/v2/mix/market/ticker"
        fund_url   = f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate"
        params = {"symbol": self.symbol, "productType": "USDT-FUTURES"}
        ticker_resp, fund_resp = await asyncio.gather(
            self._client.get(ticker_url, params=params),
            self._client.get(fund_url,   params=params),
        )
        ticker_resp.raise_for_status()
        fund_resp.raise_for_status()
        t_list = ticker_resp.json().get("data", [])
        f_list = fund_resp.json().get("data", [{}])
        if not t_list:
            return self._empty_ticker()
        t = t_list[0]; f = f_list[0] if f_list else {}
        mark  = float(t.get("markPrice", 0))
        index = float(t.get("indexPrice", 0))
        return {
            "mark_price": mark, "index_price": index,
            "bid1": float(t.get("bidPr", mark)),
            "ask1": float(t.get("askPr", mark)),
            "instant_premium": (mark - index) / index if index > 0 else 0.0,
            "last_funding_rate": float(f.get("fundingRate", 0)),
            "next_funding_time": int(f.get("nextUpdate", 0)),
            "interest_rate": 0.0001,
            "ts": time.time(),
        }

    def _gateio_next_funding_time_ms(self) -> int:
        """Gate.io WS 無 next_funding_time 欄位。優先用合約端真實 funding_next_apply，
        缺失或已過期（poll 尚未刷新的短暫空窗）時，才退回用假設窗口推算。"""
        now_ms = int(time.time() * 1000)
        if self._gateio_next_apply_ms > now_ms:
            return self._gateio_next_apply_ms
        # fallback：真值不可用時，用當前假設窗口推算（舊行為）
        interval_sec = self._window_minutes * 60
        now = int(time.time())
        return ((now // interval_sec) + 1) * interval_sec * 1000

    async def _gateio_funding_poll_loop(self):
        """每 60 秒讀一次 Gate 合約，更新真實下次結算時間 funding_next_apply。
        WS 不推此欄位，故必須低頻 REST 輪詢補齊（屬 WS 拿不到的合法例外）。
        結算週期若被 Gate 調整，funding_next_apply 會在下次真實結算跳到新值，
        main.py 的 next_ft 跳變偵測即會 refresh_settings 重算窗口。"""
        sym = self._to_gateio_symbol()
        while True:
            try:
                await asyncio.sleep(60)
                resp = await self._client.get(f"{GATEIO_BASE}/futures/usdt/contracts/{sym}")
                resp.raise_for_status()
                d = resp.json()
                fna = int(d.get("funding_next_apply", 0) or 0)
                if fna > 0:
                    self._gateio_next_apply_ms = fna * 1000
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[GateIO] {self.symbol} funding_next_apply 輪詢失敗: {e}")

    async def _gateio_ticker(self) -> dict:
        sym = self._to_gateio_symbol()
        resp = await self._client.get(
            f"{GATEIO_BASE}/futures/usdt/tickers",
            params={"contract": sym},
        )
        resp.raise_for_status()
        items = resp.json()
        if not items:
            return self._empty_ticker()
        d = items[0]
        mark  = float(d.get("mark_price", 0) or 0)
        index = float(d.get("index_price", 0) or 0)
        return {
            "mark_price":        mark,
            "index_price":       index,
            "bid1":              float(d.get("highest_bid", mark) or mark),
            "ask1":              float(d.get("lowest_ask", mark) or mark),
            "instant_premium":   (mark - index) / index if index > 0 else 0.0,
            "last_funding_rate": float(d.get("funding_rate_indicative", 0) or 0),
            "next_funding_time": self._gateio_next_funding_time_ms(),
            "interest_rate":     0.0001,
            "ts":                time.time(),
        }

    @staticmethod
    def _okx_resolve_next_funding_time(fr_data: dict) -> int:
        """OKX fundingTime=當期結算, nextFundingTime=下期結算。
        回傳「最近的下一次結算」毫秒時間戳，統一語義與其他交易所一致。
        採用魯棒性設計：從 API 提供的值中選擇最近的一個未來時間點。"""
        try:
            ft  = int(fr_data.get("fundingTime", 0) or 0)
            nft = int(fr_data.get("nextFundingTime", 0) or 0)
            now_ms = int(time.time() * 1000)
            
            # 找出所有大於當前時間的結算點
            candidates = [t for t in [ft, nft] if t > now_ms + 1000] # 預留 1s 緩衝
            if candidates:
                return min(candidates)
            
            # 如果都沒有未來的（可能剛結算且 API 尚未更新 nft），則回傳最大的那個
            return max(ft, nft)
        except Exception:
            return int(fr_data.get("nextFundingTime", 0) or 0)

    async def _okx_ticker(self) -> dict:
        inst_id = self._to_okx_swap_symbol()
        idx_id  = self._to_okx_index_symbol()
        # 並行取 ticker + funding-rate + index-ticker + mark-price
        ticker_resp, fr_resp, idx_resp, mark_resp = await asyncio.gather(
            self._client.get(f"{OKX_BASE}/api/v5/market/ticker", params={"instId": inst_id}),
            self._client.get(f"{OKX_BASE}/api/v5/public/funding-rate", params={"instId": inst_id}),
            self._client.get(f"{OKX_BASE}/api/v5/market/index-tickers", params={"instId": idx_id}),
            self._client.get(f"{OKX_BASE}/api/v5/public/mark-price", params={"instType": "SWAP", "instId": inst_id}),
        )
        t = (ticker_resp.json().get("data") or [{}])[0]
        f = (fr_resp.json().get("data") or [{}])[0]
        i = (idx_resp.json().get("data") or [{}])[0]
        m = (mark_resp.json().get("data") or [{}])[0]
        mark  = float(m.get("markPx", 0) or 0)
        index = float(i.get("idxPx", 0) or 0)
        return {
            "mark_price":        mark,
            "index_price":       index,
            "bid1":              float(t.get("bidPx", mark) or mark),
            "ask1":              float(t.get("askPx", mark) or mark),
            "instant_premium":   (mark - index) / index if index > 0 else 0.0,
            "last_funding_rate": float(f.get("fundingRate", 0) or 0),
            "next_funding_time": self._okx_resolve_next_funding_time(f),
            "interest_rate":     0.0001,
            "ts":                time.time(),
        }

    def _empty_ticker(self) -> dict:
        return {
            "mark_price": 0, "index_price": 0, "bid1": 0, "ask1": 0,
            "instant_premium": 0, "last_funding_rate": 0,
            "next_funding_time": 0, "interest_rate": 0.0001, "ts": time.time(),
        }

    # ─── 歷史 Klines（TWAP 預填，REST）──────────────────
    async def get_latest_premium_close(self) -> Optional[float]:
        """
        取得最新一分鐘 premiumIndex kline close（用於貼 cap 時的精確採樣）。
        Bybit / Binance 專用，其他交易所返回 None。
        """
        if self.exchange not in ("bybit", "binance"):
            return None
        try:
            logger.info(f"[KlineClose] {self.exchange}:{self.symbol} 開始取 limit=2 klines")
            klines = await self.get_klines_history(limit=2)
            logger.info(f"[KlineClose] {self.exchange}:{self.symbol} klines={klines}")
            if klines:
                return klines[-1]["premium"]
            logger.warning(f"[KlineClose] {self.exchange}:{self.symbol} klines 空資料，返回 None")
        except Exception as e:
            logger.warning(f"[{self.symbol}] get_latest_premium_close 失敗: {e}", exc_info=True)
        return None

    async def get_klines_history(self, limit: int = 480, start_ms: int = None) -> list:
        if self.exchange == "binance":
            return await self._binance_premium_klines(limit, start_ms=start_ms)
        elif self.exchange == "aster":
            return await self._aster_premium_klines(limit, start_ms=start_ms)
        elif self.exchange == "bybit":
            return await self._bybit_premium_klines(limit, start_ms=start_ms)
        elif self.exchange == "bitget":
            return await self._bitget_premium_klines(limit, start_ms=start_ms)
        elif self.exchange == "gateio":
            return await self._gateio_premium_klines(limit, start_ms=start_ms)
        elif self.exchange == "okx":
            return await self._okx_premium_klines(limit, start_ms=start_ms)
        return []

    async def _aster_premium_klines(self, limit: int = 480, start_ms: int = None) -> list:
        """
        Aster premiumIndexKlines（格式與 Binance 相同，kline[4] = 收盤溢價率）
        """
        params = {"symbol": self.symbol, "interval": "1m", "limit": min(limit, 1500)}
        if start_ms:
            params["startTime"] = start_ms
        try:
            resp = await self._client.get(f"{ASTER_BASE}/fapi/v1/premiumIndexKlines", params=params)
            resp.raise_for_status()
            klines = resp.json()
            if not klines:
                logger.warning("[Aster] premiumIndexKlines 空資料")
                return []
            result = sorted(
                [{"ts": int(k[0]) / 1000, "premium": float(k[4])} for k in klines if len(k) >= 5],
                key=lambda x: x["ts"],
            )
            logger.info(f"[Aster] premiumIndexKlines 取得 {len(result)} 筆")
            return result
        except Exception as e:
            logger.warning(f"[Aster] premiumIndexKlines 失敗: {e}")
            return []

    async def _binance_premium_klines(self, limit: int = 480, start_ms: int = None) -> list:
        url = f"{BINANCE_BASE}/fapi/v1/premiumIndexKlines"
        params = {"symbol": self.symbol, "interval": "1m", "limit": limit}
        if start_ms:
            params["startTime"] = start_ms
        resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        return sorted(
            [{"ts": int(k[0]) / 1000, "premium": float(k[4])} for k in resp.json()],
            key=lambda x: x["ts"],
        )

    async def _gateio_premium_klines(self, limit: int = 480, start_ms: int = None) -> list:
        sym = self._to_gateio_symbol()
        params = {"contract": sym, "interval": "1m", "limit": min(limit, 2000)}
        if start_ms:
            params["from"] = start_ms // 1000  # Gate.io 使用秒
        resp = await self._client.get(f"{GATEIO_BASE}/futures/usdt/premium_index", params=params)
        resp.raise_for_status()
        klines = resp.json()
        # Gate.io 格式：[{"t": Unix秒, "o": open, "c": close, "h": high, "l": low}]
        return sorted(
            [{"ts": int(k["t"]), "premium": float(k["c"])} for k in klines if "t" in k and "c" in k],
            key=lambda x: x["ts"],
        )

    async def _bybit_premium_klines(self, limit: int = 480, start_ms: int = None) -> list:
        url = f"{BYBIT_BASE}/v5/market/premium-index-price-kline"
        params = {"category": "linear", "symbol": self.symbol, "interval": "1", "limit": limit}
        if start_ms:
            params["start"] = start_ms
        resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        klines = resp.json().get("result", {}).get("list", [])
        return sorted(
            [{"ts": int(k[0]) / 1000, "premium": float(k[4])} for k in klines],
            key=lambda x: x["ts"],
        )

    async def _bitget_premium_klines(self, limit: int = 480, start_ms: int = None) -> list:
        """
        Bitget premiumIndex K線：
        優先使用 V3 API (type=PREMIUM_INDEX，最多 1000 筆，官方真實溢價指數)
        Fallback: V2 mark - index 相減近似（對小幣誤差較大）
        """
        # Bitget V3 API 的 PREMIUM_INDEX 回傳絕對價格（非比率），
        # MARKET_PRICE/INDEX_PRICE 兩者數值相同導致 premium 永遠為 0，均不可用。
        # 唯一正確方法：V2 history-mark-candles + history-index-candles 自行計算比率。
        params_v2 = {
            "symbol": self.symbol,
            "productType": "USDT-FUTURES",
            "granularity": "1m",
            "limit": str(min(limit, 200)),
        }
        if start_ms:
            params_v2["startTime"] = str(start_ms)
        try:
            mark_resp, idx_resp = await asyncio.gather(
                self._client.get(f"{BITGET_BASE}/api/v2/mix/market/history-mark-candles", params=params_v2),
                self._client.get(f"{BITGET_BASE}/api/v2/mix/market/history-index-candles", params=params_v2),
            )
            mark_resp.raise_for_status()
            idx_resp.raise_for_status()
            mark_data = mark_resp.json().get("data", [])
            idx_data  = idx_resp.json().get("data", [])
            if not mark_data or not idx_data:
                logger.warning("[Bitget] mark/index klines 空資料")
                return []
            idx_map = {int(k[0]): float(k[4]) for k in idx_data if len(k) >= 5}
            result = []
            for k in mark_data:
                if len(k) < 5:
                    continue
                ts_ms = int(k[0])
                mark_close = float(k[4])
                idx_close = idx_map.get(ts_ms)
                if idx_close and idx_close > 0:
                    premium = (mark_close - idx_close) / idx_close
                    result.append({"ts": ts_ms / 1000, "premium": premium})
            result.sort(key=lambda x: x["ts"])
            logger.info(f"[Bitget] V2 premium klines 計算完成：{len(result)} 筆")
            return result
        except Exception as e:
            logger.warning(f"[Bitget] premium klines 取得失敗: {e}")
            return []

    async def _okx_premium_klines(self, limit: int = 480, start_ms: int = None) -> list:
        """
        OKX premium klines：mark-price-candles + index-candles 相減
        OKX candle 格式：[ts_ms, open, high, low, close, ...]
        每次最多 100 筆，分頁取得更多歷史
        """
        inst_id = self._to_okx_swap_symbol()
        idx_id  = self._to_okx_index_symbol()
        all_result = []
        remaining = limit
        cursor_ms = start_ms  # after 參數（OKX 分頁：回傳 < after 的資料）

        while remaining > 0:
            batch = min(remaining, 100)
            params_mark = {"instId": inst_id, "bar": "1m", "limit": str(batch)}
            params_idx  = {"instId": idx_id,  "bar": "1m", "limit": str(batch)}
            if cursor_ms:
                params_mark["after"] = str(cursor_ms)
                params_idx["after"]  = str(cursor_ms)
            try:
                mark_resp, idx_resp = await asyncio.gather(
                    self._client.get(f"{OKX_BASE}/api/v5/market/mark-price-candles", params=params_mark),
                    self._client.get(f"{OKX_BASE}/api/v5/market/index-candles", params=params_idx),
                )
                mark_resp.raise_for_status()
                idx_resp.raise_for_status()
                mark_data = mark_resp.json().get("data", [])
                idx_data  = idx_resp.json().get("data", [])
                if not mark_data or not idx_data:
                    break
                idx_map = {str(k[0]): float(k[4]) for k in idx_data if len(k) >= 5}
                batch_result = []
                for k in mark_data:
                    if len(k) < 5:
                        continue
                    ts_str = str(k[0])
                    mark_close = float(k[4])
                    idx_close = idx_map.get(ts_str)
                    if idx_close and idx_close > 0:
                        premium = (mark_close - idx_close) / idx_close
                        batch_result.append({"ts": int(ts_str) / 1000, "premium": premium})
                all_result.extend(batch_result)
                remaining -= len(mark_data)
                if len(mark_data) < batch:
                    break  # 已無更多資料
                # 分頁：取最早的 ts 作為下一次的 after
                earliest_ts = min(int(k[0]) for k in mark_data)
                cursor_ms = earliest_ts
            except Exception as e:
                logger.warning(f"[OKX] premium klines batch 取得失敗: {e}")
                break

        all_result.sort(key=lambda x: x["ts"])
        logger.info(f"[OKX] premium klines 計算完成：{len(all_result)} 筆")
        return all_result
