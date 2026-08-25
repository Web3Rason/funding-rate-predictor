import { useState, useEffect, useCallback, useRef } from 'react'

// ─── Polling Hook ─────────────────────────────────────────────────────────────
function usePolling(url, interval = 5000) {
  const [data, setData] = useState(null)
  useEffect(() => {
    let active = true
    const poll = async () => {
      try {
        const res = await fetch(url)
        if (res.ok && active) setData(await res.json())
      } catch {}
      if (active) setTimeout(poll, interval)
    }
    poll()
    return () => { active = false }
  }, [url, interval])
  return data
}

// URL 變化時立刻清空；每次 effect 有自己的 active，舊 setTimeout 絕不汙染新 URL 的資料
function useResetPolling(url, interval = 5000) {
  const [data, setData] = useState(null)
  useEffect(() => {
    let active = true
    setData(null)
    const poll = async () => {
      try {
        const res = await fetch(url)
        if (res.ok && active) setData(await res.json())
      } catch {}
      if (active) setTimeout(poll, interval)
    }
    poll()
    return () => { active = false }
  }, [url, interval])
  return data
}

// 所有監控 pair 各自維持 WebSocket，切換只換顯示，不重連
function useAllPairsWS(monitors) {
  const [allData, setAllData] = useState({})
  const wsMap = useRef({})

  useEffect(() => {
    const keys = new Set((monitors || []).map(m => m.key))

    // 關掉已移除的 pair
    Object.keys(wsMap.current).forEach(key => {
      if (!keys.has(key)) {
        wsMap.current[key].close()
        delete wsMap.current[key]
        setAllData(prev => { const n = {...prev}; delete n[key]; return n })
      }
    })

    // 新 pair 開 WebSocket
    ;(monitors || []).forEach(m => {
      if (wsMap.current[m.key]) return
      const proto = location.protocol === 'https:' ? 'wss' : 'ws'
      const ws = new WebSocket(`${proto}://${location.host}/ws?exchange=${m.exchange}&symbol=${m.symbol}`)
      ws.onmessage = (e) => {
        try {
          const d = JSON.parse(e.data)
          // error 幀（no data yet）不覆蓋已有的好資料，避免閃爍
          if (!d.error) setAllData(prev => ({...prev, [m.key]: d}))
        } catch {}
      }
      ws.onclose = () => {
        delete wsMap.current[m.key]
      }
      wsMap.current[m.key] = ws
    })
  }, [monitors])

  useEffect(() => () => Object.values(wsMap.current).forEach(ws => ws.close()), [])

  return allData
}

// ─── 工具函式 ─────────────────────────────────────────────────────────────────
const fmt = (v, d = 4) => v != null ? v.toFixed(d) : '--'
const fmtPrice = (v) => {
  if (v == null) return '--'
  if (v === 0) return '0'
  const mag = Math.floor(Math.log10(Math.abs(v)))
  const d = Math.max(0, 3 - mag)
  return v.toFixed(d)
}
const pct = (v, d = 4) => v != null ? `${v >= 0 ? '+' : ''}${v.toFixed(d)}%` : '--'
const pctPlain = (v, d = 4) => v != null ? `${v.toFixed(d)}%` : '--'

function fmtCountdown(sec) {
  if (!sec && sec !== 0) return '--'
  const h = Math.floor(sec / 3600)
  const m = Math.floor((sec % 3600) / 60)
  const s = sec % 60
  if (h > 0) return `${h}h ${m}m ${s}s`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

function rateColor(v) {
  if (v == null) return 'text-gray-300'
  if (v < -0.01) return 'text-green-400'
  if (v < 0.01) return 'text-yellow-300'
  return 'text-red-400'
}
function premiumColor(v) {
  if (v == null) return 'text-gray-300'
  return v < 0 ? 'text-green-400' : 'text-red-400'
}

// ─── 計算方法頁面 ─────────────────────────────────────────────────────────────
const FORMULA_DATA = {
  binance: {
    label: 'Binance', cls: 'bg-yellow-600 text-black',
    docUrl: 'https://www.binance.com/en/support/faq/360033525031',
    sections: [
      {
        title: '核心公式',
        content: `Step 1: F_raw = [ P + clamp(I − P, ±0.05%) ] / (8/N)
         N = 結算週期小時數（1h / 4h / 8h）
Step 2: F_final = clamp(F_raw, adjustedFundingRateFloor, adjustedFundingRateCap)`,
      },
      {
        title: 'Premium Index (P)',
        content: `P = [ max(0, ImpactBid − Index) − max(0, Index − ImpactAsk) ] / Index
每 5 秒計算一次
ImpactBid / ImpactAsk：以 IMN 為名目金額的訂單簿衝擊價格

【取得方式】
• 即時溢價：WebSocket wss://fstream.binance.com/stream
  訂閱 {symbol}@markPrice@1s（欄位 p = mark price, i = index price）
  + {symbol}@depth@100ms（訂單簿，用於計算 ImpactBid/Ask）
• 初始快照：REST GET /fapi/v1/depth?symbol=&limit=1000`,
      },
      {
        title: 'Interest Rate (I)',
        content: `固定 0.01%（8h 基準），不隨結算週期 N 縮放

【取得方式】
REST GET /fapi/v1/premiumIndex?symbol=
回應欄位：interestRate`,
      },
      {
        title: 'TWAP 加權',
        content: `結算週期 > 1h：算術級數加權
  P_avg = Σ(Pᵢ × i) / Σ(i)   （越近結算，權重越高）
結算週期 = 1h：等權算術平均
  P_avg = (P₁ + P₂ + … + Pₙ) / n

【採樣頻率說明】
• 官方規格：P 每 5 秒計算一次，TWAP 由這些 5 秒快照累積
• 本系統實作：
  - 初始化（冷啟動）：用 premiumIndexKlines（1m K線）預填窗口
    REST GET /fapi/v1/premiumIndexKlines?symbol=&interval=1m&limit=N
    此為近似值，1m K線的 close 代表該分鐘末尾的 P 快照
  - 即時運行：WebSocket markPriceUpdate stream 每 3 秒推送，即時累積 P 快照
    實際採樣頻率（~3s）略低於官方（5s），屬合理近似`,
      },
      {
        title: 'Impact Margin Notional (IMN)',
        content: `IMN = 200 USDT / Initial Margin Rate at maximum leverage
     = 200 × initialLeverage（最大槓桿倍數）
注意：是 Initial Margin Rate，非 Maintenance Margin Rate

【取得方式】
優先：REST GET /fapi/v1/leverageBracket?symbol=（需 API Key）
  Tier-1（brackets[0]）→ initialLeverage → IMN = 200 × initialLeverage
  例：BTCUSDT 125x → IMN = 200 × 125 = 25,000 USDT
Fallback：exchangeInfo 的 liquidationFee（近似，誤差較大）`,
      },
      {
        title: '結算週期 & Cap',
        content: `N=1h → F ÷ 8 ；N=4h → F ÷ 2 ；N=8h → 不變

【取得方式】
REST GET /fapi/v1/fundingInfo
回應欄位：fundingIntervalHours（結算週期）
          adjustedFundingRateCap / adjustedFundingRateFloor（費率上下限）`,
      },
    ],
  },
  bybit: {
    label: 'Bybit', cls: 'bg-orange-600 text-white',
    docUrl: 'https://www.bybit.com/zh-TW/help-center/article/Introduction-to-Funding-Rate',
    sections: [
      {
        title: '核心公式',
        content: `Step 1: F_raw = P + clamp(I − P, ±0.05%)
Step 2: F_final = clamp(F_raw, lowerFundingRate, upperFundingRate)

無 result_scale 除法，透過縮放 I 來適應不同結算週期`,
      },
      {
        title: 'Premium Index (P)',
        content: `P = [ max(0, ImpactBid − Index) − max(0, Index − ImpactAsk) ] / Index
每分鐘採樣一次

【取得方式】
• 即時溢價計算：需訂閱兩個 WS 頻道
  - WebSocket wss://stream.bybit.com/v5/public/linear
  - tickers.{symbol}：取得 markPrice, indexPrice, fundingRate, nextFundingTime
  - orderbook.50.{symbol}：取得訂單簿，本地計算 ImpactBid/ImpactAsk
• 歷史預填 TWAP：REST GET /v5/market/premium-index-price-kline
  ?category=linear&symbol=&interval=1&limit=N
  （注意：需用 premium-index-price-kline，非 mark-price-kline）`,
      },
      {
        title: 'Interest Rate (I) 縮放 & clamp_inner (δ)',
        content: `每日基礎利率 0.03%，I 依結算週期等比縮放：
8h 週期：I = 0.03% / 3  = 0.01%
4h 週期：I = 0.03% / 6  = 0.005%
1h 週期：I = 0.03% / 24 = 0.00125%

δ（clamp_inner）= 固定 ±0.05%，不隨結算週期縮放
（官方文檔明確確認：動態結算頻率切換只改 I，不改 δ）

【取得方式】
REST GET /v5/market/tickers?category=linear&symbol=
回應欄位：fundingRate（當前費率），可反推 I；
或從 WebSocket tickers stream 的 fundingRate 欄位取得`,
      },
      {
        title: 'TWAP 加權',
        content: `算術級數加權（Arithmetic Progression Weighting），所有結算週期均適用
P_avg = Σ(Pᵢ × i) / Σ(i)
越接近結算點，權重越高

8h 結算（480 樣本）：第 480 分鐘權重 = 第 1 分鐘的 480 倍
1h 結算（60 樣本）：第 60 分鐘權重 = 第 1 分鐘的 60 倍
（Bybit 所有週期均用加權，不像 Binance 1h 例外改為等權）

【取得方式】
啟動時以 /v5/market/premium-index-price-kline 預填歷史 P
之後由 WebSocket orderbook.50 每分鐘計算並累積`,
      },
      {
        title: 'Impact Margin Notional (IMN)',
        content: `IMN = 200 USDT / Initial Margin Rate（官方文檔確認）
     = 200 / (1 / maxLeverage) = 200 × maxLeverage

例：BTCUSDT 最大槓桿 125x → IMR = 0.8% → IMN = 25,000 USDT

【取得方式】
REST GET /v5/market/risk-limit?category=linear&symbol=
取 isLowestRisk=1 的 Tier-1，欄位 initialMargin（小數，如 0.008）
IMN = 200 / initialMargin`,
      },
      {
        title: '結算週期 & Cap',
        content: `Cap 為外層 clamp 的邊界：F_final = clamp(F_raw, lower, upper)

【取得方式】
REST GET /v5/market/instruments-info?category=linear&symbol=
回應欄位：fundingInterval（分鐘）
          upperFundingRate（費率上限，即 +cap）
          lowerFundingRate（費率下限，即 −cap）`,
      },
    ],
  },
  bitget: {
    label: 'Bitget', cls: 'bg-blue-600 text-white',
    docUrl: 'https://www.bitget.com/api-doc/common/perpetual/funding-rate',
    sections: [
      {
        title: '核心公式',
        content: `F = clamp(P + clamp(I − P, ±0.05%), ±cap) × (N/8)
顯式以 N/8 縮放最終費率`,
      },
      {
        title: 'Premium Index (P)',
        content: `每分鐘採樣，公式與 Bybit 相同
P = [ max(0, ImpactBid − Index) − max(0, Index − ImpactAsk) ] / Index

【取得方式】
• 即時：WebSocket wss://ws.bitget.com/v2/ws/public
  訂閱 ticker（productType=USDT-FUTURES），含 markPrice、indexPrice
• 歷史預填：REST GET /api/v2/mix/market/history-candles
  （Bitget 無 premiumIndex K線，改用 markPrice K線近似計算）`,
      },
      {
        title: 'Interest Rate (I)',
        content: `固定 0.01%（8h 基準，不縮放）
由最終 result_scale = N/8 統一處理結算週期差異

【取得方式】
固定常數，無需 API 取得`,
      },
      {
        title: 'TWAP 加權',
        content: `算術級數加權（Arithmetic Progression Weighting），所有結算週期均適用
P_avg = Σ(Pᵢ × i) / Σ(i)   （越近結算，權重越高）

8h 結算（480 樣本）：第 480 分鐘權重 = 第 1 分鐘的 480 倍

【取得方式】
歷史預填：REST GET https://api.bitget.com/api/v3/market/candles
          ?symbol=&interval=1m&type=PREMIUM_INDEX&category=USDT-FUTURES&limit=N
          （V3 API，最多 1000 筆，kline[4] = 收盤 P）`,
      },
      {
        title: 'Impact Margin Notional (IMN)',
        content: `IMN = 200 USDT / 最小維持保證金率（MMR）
取 Tier-1（最低倉位層級）的 keepMarginRate

【取得方式】
REST GET /api/v2/mix/market/query-position-lever
  ?symbol=&productType=USDT-FUTURES&marginCoin=USDT
回應欄位：data[level="1"].keepMarginRate
若端點回傳錯誤（可能需 API Key），fallback 使用 IMN = 200 USDT`,
      },
      {
        title: '結算週期 & Cap',
        content: `N=1h → × (1/8)；N=4h → × (1/2)；N=8h → × 1

【取得方式】
REST GET /api/v2/mix/market/current-fund-rate
  ?symbol=&productType=USDT-FUTURES
回應欄位：data[0].fundingRateInterval（結算小時數）
          data[0].maxFundingRate（費率上限 cap）`,
      },
    ],
  },
  gateio: {
    label: 'Gate.io', cls: 'bg-teal-600 text-white',
    docUrl: 'https://www.gate.io/zh/help/futures/perpetual/16645',
    sections: [
      {
        title: '核心公式',
        content: `F = clamp( [ P + clamp(I − P, ±0.05%) ] / (8/N), ±cap )
N = 結算週期小時數（1h / 4h / 8h）
※ 2026-05-27 Gate 演算法升級（公告 51275）：改為與 Binance 同構，
   基礎利率固定 0.01%（8h 等效），週期縮放由 ÷(8/N) 處理`,
      },
      {
        title: 'Premium Index (P)',
        content: `每 60 秒計算一次
P = [ max(0, ImpactBid − Index) − max(0, Index − ImpactAsk) ] / Index

【取得方式】
• 即時：WebSocket wss://fx-ws.gateio.ws/v4/ws/usdt
  訂閱 futures.tickers（含 mark_price、index_price、funding_rate）
  + futures.order_book_update（訂單簿，計算 ImpactBid/Ask）
• 歷史預填：REST GET /api/v4/futures/usdt/premium_index?contract=&interval=1m&limit=N`,
      },
      {
        title: 'Interest Rate (I)',
        content: `基礎利率固定 0.01%（8h 等效，clamp 內用；週期縮放另由 ÷(8/N) 處理）
= 每日利率 × 8/24（API interest_rate 日利率，如 0.03%/day → 0.01%）
  commodities（日利率 0）→ 0；週期縮放不再改動此利率

【取得方式】
REST GET /api/v4/futures/usdt/contracts/{contract} 的 interest_rate（日利率）×8/24`,
      },
      {
        title: 'TWAP 加權',
        content: `結算週期內 1 分鐘 Premium Index 的「時間加權平均」（算術級數加權）
2026-05-27 升級：由算術平均改為時間加權平均（越接近結算權重越高）

【取得方式】
歷史 premium_index K線預填，後由 WebSocket 即時採樣累積`,
      },
      {
        title: 'Impact Margin Notional (IMN)',
        content: `Gate.io 以 funding_impact_value（USDT 名義金額）作為 Impact Margin
ImpactBid/ImpactAsk = 走訂單簿累積到該 USDT 名義金額後的 VWAP
（每張合約名義 = price × quanto_multiplier）
注意：不同幣種 IMN 差異大（BTC=30000 USDT，HOOK=2000 USDT，AIGENSYN=5000 USDT）

費率上限（cap）：
  cap = min(funding_rate_limit, cap_ratio × maintenance_rate)

【取得方式】
REST GET /api/v4/futures/usdt/contracts/{contract}
回應欄位：funding_impact_value（USDT 名義金額，用於 VWAP 計算）
          funding_rate_limit（每結算週期費率上限）
          funding_cap_ratio（費率上限係數）
          maintenance_rate（維持保證金率，與 cap_ratio 相乘取 cap）`,
      },
    ],
  },
  aster: {
    label: 'Aster', cls: 'bg-purple-600 text-white',
    docUrl: 'https://docs.asterdex.com/trading/perpetuals/fees-and-specs/funding-rate',
    sections: [
      {
        title: '核心公式（永續合約）',
        content: `與 Binance 公式完全相同（兩步驟）：
Step 1: F_raw = [ P + clamp(I − P, ±0.05%) ] / (8/N)
        N = 結算週期小時數（1h / 4h / 8h）
Step 2: F_final = clamp(F_raw, fundingFeeFloor, fundingFeeCap)`,
      },
      {
        title: 'Premium Index (P)',
        content: `P = [ max(0, ImpactBid − IndexPrice) − max(0, IndexPrice − ImpactAsk) ] / IndexPrice

【取得方式】
• 即時：WebSocket wss://fstream.asterdex.com/stream
  訂閱 {symbol}@markPrice@1s（格式與 Binance 完全相同）
  + {symbol}@depth@100ms（訂單簿，計算 ImpactBid/Ask）
• 初始快照：REST GET https://fapi.asterdex.com/fapi/v1/depth?symbol=&limit=1000`,
      },
      {
        title: 'Interest Rate (I)',
        content: `固定 0.01%（大多數幣種）
BNBUSDT 為 0%

【取得方式】
REST GET https://fapi.asterdex.com/fapi/v1/premiumIndex?symbol=
回應欄位：interestRate（格式與 Binance 相同）`,
      },
      {
        title: 'TWAP 加權',
        content: `結算週期 > 1h：算術級數加權（與 Bybit 相同）
  P_avg = Σ(Pᵢ × i) / Σ(i)
結算週期 = 1h：等權算術平均

【取得方式】
歷史預填：REST GET https://fapi.asterdex.com/fapi/v1/premiumIndexKlines
          ?symbol=&interval=1m&limit=N（格式與 Binance 完全相同，kline[4] = 收盤 P）`,
      },
      {
        title: 'Impact Margin Notional (IMN)',
        content: `IMN = 200 USDT / Initial Margin Rate at Maximum Leverage
     = 200 × maxLeverage（如支援 50x → IMN = 10,000 USDT）

【取得方式】
優先：REST GET https://fapi.asterdex.com/fapi/v1/leverageBracket?symbol=（需 API Key）
回應欄位：brackets[0].initialLeverage（Tier-1 最大槓桿）
IMN = 200 × initialLeverage

Fallback（無 API Key）：REST GET https://fapi.asterdex.com/fapi/v1/exchangeInfo
回應欄位：symbols[].liquidationFee（清算費率，作為 IMR 近似）
IMN = 200 / liquidationFee（誤差較大）`,
      },
      {
        title: '結算週期 & Cap',
        content: `分母 (8/N) 直接縮放最終費率（同 Binance）
N=1h → F ÷ 8；N=8h → F ÷ 1

【取得方式】
REST GET https://fapi.asterdex.com/fapi/v1/fundingInfo
回應欄位：fundingIntervalHours（結算週期小時）
          fundingFeeCap / fundingFeeFloor（費率上下限）
注意：欄位名稱與 Binance 不同（Binance 用 adjustedFundingRateCap）`,
      },
      {
        title: 'Shield Mode（特殊模式）',
        content: `F = clamp(Floor, |[(Long OI − Short OI) × Funding Fee Per Hour / M%]| / max(LongOI, ShortOI), Cap)
每小時結算，持倉必須跨整點才計費
文檔：https://docs.asterdex.com/trading/shield-mode/funding-fee-rate`,
      },
    ],
  },
  okx: {
    label: 'OKX', cls: 'bg-gray-200 text-black',
    docUrl: 'https://www.okx.com/help/how-to-calculate-funding-rate',
    sections: [
      {
        title: '核心公式',
        content: `F = clamp(AvgPremiumIndex + clamp(I − AvgPremiumIndex, −0.05%, +0.05%), −cap, +cap)

I 依結算週期等比縮放（同 Bybit），δ(±0.05%) 固定不縮放
無 result_scale 乘法`,
      },
      {
        title: 'Premium Index (P)',
        content: `P = [ max(0, ImpactBid − Index) − max(0, Index − ImpactAsk) ] / Index

【取得方式】
• 即時：WebSocket wss://ws.okx.com:8443/ws/v5/public
  訂閱 5 個頻道合併：
  - tickers（bidPx, askPx）
  - mark-price（markPx）
  - index-tickers（idxPx，用 BTC-USDT 格式）
  - funding-rate（fundingRate, nextFundingTime）
  - books（訂單簿 400 檔，計算 ImpactBid/Ask）`,
      },
      {
        title: 'Interest Rate (I) & clamp_inner (δ)',
        content: `每日基礎利率 0.03%，I 依結算週期等比縮放：
8h 週期：I = 0.03% / 3  = 0.01%
4h 週期：I = 0.03% / 6  = 0.005%
1h 週期：I = 0.03% / 24 = 0.00125%

δ（clamp_inner）= 固定 ±0.05%，不隨結算週期縮放`,
      },
      {
        title: 'TWAP 加權',
        content: `算術級數加權（WMA），最新分鐘權重最高
P_avg = Σ(Pᵢ × i) / Σ(i)
窗口為當前結算週期全部分鐘（固定窗口，結算重置）

【取得方式】
歷史預填：mark-price-candles + index-candles 相減計算
  GET /api/v5/market/mark-price-candles?instId=BTC-USDT-SWAP&bar=1m
  GET /api/v5/market/index-candles?instId=BTC-USDT&bar=1m`,
      },
      {
        title: 'Impact Margin Notional (IMN)',
        content: `IMN = 200 USDT / Initial Margin Rate（Tier-1）

【取得方式】
REST GET /api/v5/public/position-tiers
  ?instType=SWAP&instId=BTC-USDT-SWAP&tdMode=cross
回應欄位：data[0].imr（Initial Margin Rate）
例：BTC 100x → IMR=0.01 → IMN=20,000 USDT`,
      },
      {
        title: '結算週期 & Cap',
        content: `大多數 8h（00:00/08:00/16:00 UTC），部分幣種 4h/2h/1h
Cap 由交易所動態設定

【取得方式】
結算週期：GET /api/v5/public/instruments?instType=SWAP&instId=
  回應欄位：fundingInterval（毫秒，如 28800000=8h）
Cap：GET /api/v5/public/funding-rate?instId=
  回應欄位：maxFundingRate / minFundingRate`,
      },
      {
        title: 'Symbol 格式',
        content: `永續合約：BTC-USDT-SWAP（帶 -SWAP 後綴）
Index：BTC-USDT（不帶 -SWAP）
本系統自動轉換：BTCUSDT → BTC-USDT-SWAP`,
      },
    ],
  },
}

function FormulaPage({ onBack }) {
  const [activeEx, setActiveEx] = useState('binance')
  const exs = Object.entries(FORMULA_DATA)
  const d = FORMULA_DATA[activeEx]
  return (
    <div className="min-h-screen bg-gray-950 text-white px-3 sm:px-6 py-4 sm:py-6">
      <div className="flex items-center gap-3 mb-6">
        <button onClick={onBack} className="text-xs text-gray-500 hover:text-gray-300 px-2 py-1 rounded bg-gray-800 hover:bg-gray-700">← 返回</button>
        <h1 className="text-base font-bold text-gray-200">資金費率計算方法</h1>
      </div>
      {/* 交易所切換 */}
      <div className="flex gap-2 flex-wrap mb-6">
        {exs.map(([id, info]) => (
          <button key={id} onClick={() => setActiveEx(id)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${activeEx === id ? info.cls : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}`}
          >{info.label}</button>
        ))}
      </div>
      {/* 官方文檔連結 */}
      <div className="mb-5">
        <a href={d.docUrl} target="_blank" rel="noopener noreferrer"
          className="text-xs text-blue-400 hover:text-blue-300 underline underline-offset-2 break-all">
          官方文檔：{d.docUrl}
        </a>
      </div>
      {/* 公式區塊 */}
      <div className="space-y-4">
        {d.sections.map(s => (
          <div key={s.title} className="rounded-xl bg-gray-900 border border-gray-800 p-4">
            <div className="text-xs font-semibold text-gray-400 mb-2 uppercase tracking-wide">{s.title}</div>
            <pre className="text-sm text-gray-200 whitespace-pre-wrap font-mono leading-relaxed">{s.content}</pre>
          </div>
        ))}
      </div>
    </div>
  )
}

// ─── Premium Mini Chart ────────────────────────────────────────────────────────
const CHART_H = 56   // 總高度 px
const HALF_H  = 26   // 0軸上下各 26px（留 2px 給 0軸線）

function PremiumMiniChart({ chartData }) {
  const [hovered, setHovered] = useState(null)
  if (!Array.isArray(chartData) || chartData.length === 0)
    return <div className="text-xs text-gray-600 text-center py-4">等待資料...</div>
  const values = chartData.map(d => d.premium_pct)
  const maxVal = Math.max(...values)
  const minVal = Math.min(...values)
  const maxAbs = Math.max(Math.abs(minVal), Math.abs(maxVal), 0.01)
  const fmtY = v => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
  return (
    <div className="relative" style={{ height: CHART_H }}>
      {/* tooltip */}
      {hovered && (
        <div className="absolute -top-8 left-1/2 -translate-x-1/2 bg-gray-800 border border-gray-600 rounded px-2 py-1 text-[10px] whitespace-nowrap z-10 pointer-events-none">
          <span className="text-gray-400 mr-1">{new Date(hovered.ts * 1000).toLocaleTimeString('zh-TW', { hour12: false, hour: '2-digit', minute: '2-digit' })}</span>
          <span className={hovered.v >= 0 ? 'text-red-400' : 'text-green-400'}>{hovered.v >= 0 ? '+' : ''}{hovered.v.toFixed(4)}%</span>
        </div>
      )}
      {/* Y軸標籤 */}
      <div className="absolute right-0 top-0 text-[9px] text-gray-600 leading-none">{fmtY(maxAbs)}</div>
      <div className="absolute right-0 text-[9px] text-gray-600 leading-none" style={{ top: HALF_H - 5 }}>0</div>
      <div className="absolute right-0 bottom-0 text-[9px] text-gray-600 leading-none">{fmtY(-maxAbs)}</div>
      {/* 0軸線 */}
      <div className="absolute left-0 right-6 border-t border-gray-600" style={{ top: HALF_H }} />
      {/* 柱狀圖 */}
      <div className="absolute inset-0 flex gap-px pr-6" onMouseLeave={() => setHovered(null)}>
        {values.map((v, i) => {
          const barH = Math.max(1, Math.abs(v) / maxAbs * HALF_H)
          const isPos = v >= 0
          return (
            <div key={i} className="flex-1 relative cursor-crosshair"
              onMouseEnter={() => setHovered({ i, v, ts: chartData[i].ts })}>
              <div
                className={isPos
                  ? `absolute left-0 right-0 bottom-1/2 ${hovered?.i === i ? 'bg-red-400' : 'bg-red-500/70'}`
                  : `absolute left-0 right-0 top-1/2 ${hovered?.i === i ? 'bg-green-400' : 'bg-green-500/70'}`}
                style={{ height: barH }}
              />
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─── 主元件 ───────────────────────────────────────────────────────────────────
export default function App() {
  const [page, setPage] = useState('main')
  const [exchange, setExchange] = useState('binance')
  const [symbol, setSymbol] = useState('BTCUSDT')

  const monitors = usePolling('/api/symbols', 2000)
  const allData = useAllPairsWS(monitors)
  const currentKey = `${exchange.toLowerCase()}:${symbol.toUpperCase()}`
  const data = allData[currentKey] ?? null
  const chart = useResetPolling(`/api/chart?exchange=${exchange.toLowerCase()}&symbol=${symbol.toUpperCase()}`, 60000)
  const stats = data?.stats ?? null

  // pendingRow：PRE-SAMPLE 觸發後凍結預測值，直到結算確認才清掉，不用時間 timeout
  const [pendingRow, setPendingRow] = useState(null)
  useEffect(() => {
    if (data?.pre_sample_rate_pct != null && data?.pre_sample_ts) {
      setPendingRow({ rate: data.pre_sample_rate_pct, ts: data.pre_sample_ts })
    }
  }, [data?.pre_sample_ts])
  useEffect(() => {
    if (!pendingRow || !stats?.recent?.length) return
    if (stats.recent[0].ts >= pendingRow.ts) setPendingRow(null)
  }, [stats?.count])
  useEffect(() => { setPendingRow(null) }, [currentKey])

  // 切換幣種時隱藏舊預測，直到收到切換後的新資料
  const switchedAtRef = useRef(0)
  useEffect(() => { switchedAtRef.current = Date.now() / 1000 }, [currentKey])
  const predData = (data && (data.ts ?? 0) > switchedAtRef.current) ? data : null

  // 本地倒計時：每秒 -1，poll 到新資料時重新同步
  const [countdown, setCountdown] = useState(null)
  useEffect(() => {
    if (data?.remain_sec != null) setCountdown(data.remain_sec)
  }, [data?.remain_sec])
  useEffect(() => {
    const t = setInterval(() => setCountdown(c => c != null && c > 0 ? c - 1 : c), 1000)
    return () => clearInterval(t)
  }, [])

  // 下一分鐘採樣倒計時（系統時鐘算，與 Binance 分鐘邊界對齊）
  const [nextMinuteSec, setNextMinuteSec] = useState(null)
  useEffect(() => {
    const update = () => setNextMinuteSec(60 - Math.floor(Date.now() / 1000) % 60)
    update()
    const t = setInterval(update, 1000)
    return () => clearInterval(t)
  }, [])
  const [customSymbol, setCustomSymbol] = useState('')
  const [switching, setSwitching] = useState(false)
  const [switchError, setSwitchError] = useState('')
  const [addSymbol, setAddSymbol] = useState('')
  const [adding, setAdding] = useState(false)

  // 拖拉排序
  const [monitorOrder, setMonitorOrder] = useState(() => {
    try { return JSON.parse(localStorage.getItem('monitor_order') || '[]') } catch { return [] }
  })
  const dragKey = useRef(null)
  const dragOverKey = useRef(null)

  // 同步 monitorOrder：新加的排後面，已刪的移除
  useEffect(() => {
    if (!monitors) return
    const keys = monitors.map(m => m.key)
    setMonitorOrder(prev => {
      const merged = [...prev.filter(k => keys.includes(k)), ...keys.filter(k => !prev.includes(k))]
      localStorage.setItem('monitor_order', JSON.stringify(merged))
      return merged
    })
  }, [monitors?.map(m => m.key).join(',')])
  const [customTarget, setCustomTarget] = useState('')
  const [customNeeded, setCustomNeeded] = useState(null)
  const [customTargetPending, setCustomTargetPending] = useState(false)

  const [history, setHistory] = useState(() => {
    try { return JSON.parse(localStorage.getItem('pair_history') || '[]') } catch { return [] }
  })
  const addHistory = useCallback((ex, sym) => {
    setHistory(prev => {
      const filtered = prev.filter(h => !(h.exchange === ex && h.symbol === sym))
      const next = [{ exchange: ex, symbol: sym }, ...filtered].slice(0, 20)
      localStorage.setItem('pair_history', JSON.stringify(next))
      return next
    })
  }, [])
  const removeHistory = useCallback((ex, sym) => {
    setHistory(prev => {
      const next = prev.filter(h => !(h.exchange === ex && h.symbol === sym))
      localStorage.setItem('pair_history', JSON.stringify(next))
      return next
    })
  }, [])

  const handleSwitch = useCallback(async (ex, sym) => {
    setSwitching(true)
    setSwitchError('')
    try {
      const res = await fetch('/api/switch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ exchange: ex, symbol: sym }),
      })
      const result = await res.json()
      if (result.error) setSwitchError(result.error)
      else {
        setExchange(result.exchange)
        setSymbol(result.symbol)
        addHistory(result.exchange, result.symbol)
      }
    } catch (e) {
      setSwitchError(e.message)
    }
    setSwitching(false)
  }, [addHistory])

  const handleAdd = useCallback(async (ex, sym) => {
    if (!sym) return
    setAdding(true)
    try {
      await fetch('/api/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ exchange: ex, symbol: sym }),
      })
      setAddSymbol('')
      // 加入後自動切換到該幣種
      setExchange(ex)
      setSymbol(sym.toUpperCase())
    } catch {}
    setAdding(false)
  }, [])

  const handleRemove = useCallback(async (ex, sym) => {
    await fetch('/api/remove', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ exchange: ex, symbol: sym }),
    })
  }, [])

  const handleSelectMonitor = useCallback(async (ex, sym) => {
    // 樂觀更新：立刻切換 state，data URL 立刻變，不等 API 回來
    setExchange(ex)
    setSymbol(sym)
    setSwitching(true)
    try {
      await fetch('/api/switch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ exchange: ex, symbol: sym }),
      })
    } catch {}
    setSwitching(false)
  }, [])

  if (page === 'formula') return <FormulaPage onBack={() => setPage('main')} />

  const isLoading = !data || data.error
  const dataAge = data?.ts ? Math.round(Date.now() / 1000 - data.ts) : null
  const isStale = dataAge != null && dataAge > 15

  // P2: pre_sample 僅在採樣前 5 秒內有效（之後切回即時 next_minute_rate）
  const preSampleAge = predData?.pre_sample_ts ? Date.now() / 1000 - predData.pre_sample_ts : 999
  const isPreSample = predData?.pre_sample_rate_pct != null && preSampleAge < 5
  const nextRatePct = isPreSample ? predData.pre_sample_rate_pct : predData?.next_minute_rate_pct

  // 結算進度條
  const settlePct = data?.window_minutes && data?.elapsed_min != null
    ? Math.min(100, (data.elapsed_min / data.window_minutes) * 100)
    : 0

  return (
    <div className="min-h-screen bg-gray-950 text-white px-3 sm:px-6 py-4 sm:py-6">
      <div className="w-full flex gap-4 items-start">
      <div className="flex-1 min-w-0 space-y-4">

        {/* ─── Header ─── */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-base sm:text-lg font-bold text-gray-200">資金費率預測器</h1>
            <div className="text-xs text-gray-500 mt-0.5">
              {exchange.toUpperCase()} · {symbol} · {data?.window_minutes ?? '--'}min
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage('formula')}
              className="text-xs px-2.5 py-1 rounded-lg bg-gray-800 text-gray-400 hover:bg-gray-700 hover:text-gray-200 transition-colors"
            >計算方法</button>
            <div className="flex items-center gap-1.5 text-xs text-gray-500">
              <span className={`w-2 h-2 rounded-full ${isLoading ? 'bg-yellow-500 animate-pulse' : isStale ? 'bg-red-500' : 'bg-green-500'}`} />
              {switching ? '切換中...' : isLoading ? '初始化...' : isStale ? `STALE ${dataAge}s` : `${data?.history_count ?? 0}/${data?.window_minutes ?? '--'}`}
            </div>
          </div>
        </div>

        {/* ─── 選幣面板 ─── */}
        <div className="rounded-xl p-3 bg-gray-900 border border-gray-700">
          <div className="flex flex-col sm:flex-row gap-2">
            <div className="flex gap-1.5 flex-wrap">
              {[
                { id: 'binance', label: 'Binance', cls: 'bg-yellow-600 text-black' },
                { id: 'bybit',   label: 'Bybit',   cls: 'bg-orange-600 text-white' },
                { id: 'bitget',  label: 'Bitget',  cls: 'bg-blue-600 text-white' },
                { id: 'gateio',  label: 'Gate.io', cls: 'bg-teal-600 text-white' },
                { id: 'aster',   label: 'Aster',   cls: 'bg-purple-600 text-white' },
                { id: 'okx',     label: 'OKX',     cls: 'bg-gray-200 text-black' },
              ].map(({ id, label, cls }) => (
                <button key={id}
                  onClick={() => setExchange(id)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${exchange === id ? cls : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}`}
                >{label}</button>
              ))}
            </div>
            <div className="flex gap-1.5 ml-auto">
              <input
                type="text"
                value={customSymbol}
                onChange={e => setCustomSymbol(e.target.value.toUpperCase())}
                onKeyDown={e => e.key === 'Enter' && handleSwitch(exchange, customSymbol.trim() || symbol)}
                placeholder="e.g. LINKUSDT"
                className="bg-gray-800 border border-gray-700 rounded px-2.5 py-1.5 text-xs text-white placeholder-gray-600 w-32 focus:outline-none focus:border-blue-500"
              />
              <button
                onClick={() => handleSwitch(exchange, customSymbol.trim() || symbol)}
                disabled={switching}
                className="px-2.5 py-1.5 bg-blue-700 text-white rounded text-xs hover:bg-blue-600 disabled:opacity-40"
              >監測</button>
            </div>
          </div>

          {history.length > 0 && (
            <div className="flex flex-wrap gap-1 mt-2 pt-2 border-t border-gray-800">
              <span className="text-[10px] text-gray-600 self-center mr-0.5">最近：</span>
              {history.map(h => {
                const isActive = exchange === h.exchange && symbol === h.symbol
                const exColor = h.exchange === 'binance' ? 'text-yellow-400' : h.exchange === 'bybit' ? 'text-orange-400' : h.exchange === 'gateio' ? 'text-teal-400' : h.exchange === 'aster' ? 'text-purple-400' : h.exchange === 'okx' ? 'text-gray-200' : 'text-blue-400'
                return (
                  <div key={`${h.exchange}:${h.symbol}`}
                    className={`flex items-center gap-1 rounded text-xs pl-1.5 pr-1 py-0.5 ${isActive ? 'bg-blue-700' : 'bg-gray-800 hover:bg-gray-700'}`}>
                    <button onClick={() => handleSwitch(h.exchange, h.symbol)} disabled={switching} className="flex items-center gap-1">
                      <span className={`text-[10px] ${exColor}`}>{{ binance: 'BN', bybit: 'BY', bitget: 'BG', gateio: 'GT', aster: 'AS', okx: 'OK' }[h.exchange] ?? h.exchange.toUpperCase().slice(0, 2)}</span>
                      <span className="text-gray-200">{h.symbol.replace('USDT', '')}</span>
                    </button>
                    <button onClick={() => removeHistory(h.exchange, h.symbol)} className="text-gray-600 hover:text-red-400 leading-none">×</button>
                  </div>
                )
              })}
            </div>
          )}
          {switchError && <div className="mt-1.5 text-xs text-red-400">錯誤: {switchError}</div>}
        </div>

        {/* ══════════════════════════════════════════
            P1 HERO：幣價 + 即時費率 + 即時溢價
        ══════════════════════════════════════════ */}
        <div className="grid grid-cols-3 gap-3">

          {/* 幣價 */}
          <div className="rounded-2xl p-3 sm:p-5 bg-gray-900 border border-gray-700 flex flex-col gap-1">
            <div className="text-xs text-gray-500 uppercase tracking-wide">標記價格</div>
            <div className="text-2xl sm:text-3xl font-bold tabular-nums leading-none mt-1 text-gray-100">
              {fmtPrice(data?.mark_price)}
            </div>
            <div className="text-[10px] text-gray-600 mt-1">
              idx {fmtPrice(data?.index_price)}
            </div>
          </div>

          {/* 即時費率 + 倒計時 */}
          <div className={`rounded-2xl p-3 sm:p-5 bg-gray-900 flex flex-col gap-1 ${data?.funding_rate_feed_down ? 'border border-yellow-600' : 'border border-gray-700'}`}>
            <div className="flex items-center gap-1.5">
              <div className="text-xs text-gray-500 uppercase tracking-wide">即時費率</div>
              {data?.funding_rate_feed_down ? (
                <span className="relative group">
                  <span className="text-[10px] font-bold text-yellow-400 bg-yellow-900/40 px-1.5 py-0.5 rounded cursor-help">
                    FR 缺失
                  </span>
                  <div className="absolute left-0 top-full mt-1 z-50 hidden group-hover:block w-56 bg-gray-900 border border-yellow-700/50 rounded-lg p-2 text-xs text-gray-300 shadow-xl space-y-1">
                    <div className="text-yellow-400 font-semibold">交易所持續缺失</div>
                    <div>last_funding_rate = 0 超過 3 分鐘</div>
                    <div>TWAP 歷史已停止更新，預測基於過期資料</div>
                  </div>
                </span>
              ) : data?.fr_initializing ? (
                <span className="relative group">
                  <span className="text-[10px] text-gray-500 bg-gray-800 px-1.5 py-0.5 rounded cursor-help">
                    初始化中
                  </span>
                  <div className="absolute left-0 top-full mt-1 z-50 hidden group-hover:block w-48 bg-gray-900 border border-gray-700 rounded-lg p-2 text-xs text-gray-300 shadow-xl space-y-1">
                    <div>WS 剛連線，費率資料尚未就緒</div>
                    <div className="text-gray-500">通常 30 秒內自動恢復</div>
                  </div>
                </span>
              ) : null}
            </div>
            <div className={`text-2xl sm:text-3xl font-bold tabular-nums leading-none mt-1 ${rateColor(data?.official_rate_pct)}`}>
              {pct(data?.official_rate_pct, 5)}
            </div>
            {data?.cap_pct != null && (
              <div className="text-[11px] text-gray-500 tabular-nums mt-0.5">
                極限 ±{data.cap_pct}%
              </div>
            )}
            <div className="text-sm font-semibold text-yellow-400 mt-1 tabular-nums">
              {fmtCountdown(countdown)}
            </div>
            <div className="text-[10px] text-gray-600">結算倒計時</div>
          </div>

          {/* 即時溢價 */}
          <div className={`rounded-2xl p-3 sm:p-5 bg-gray-900 flex flex-col gap-1 ${data?.premium_feed_down ? 'border border-yellow-600' : 'border border-gray-700'}`}>
            <div className="flex items-center gap-1.5">
              <div className="text-xs text-gray-500 uppercase tracking-wide">即時溢價</div>
              {data?.premium_feed_down && (
                <span className="relative group">
                  <span className="text-[10px] font-bold text-yellow-400 bg-yellow-900/40 px-1.5 py-0.5 rounded cursor-help">
                    報價失靈
                  </span>
                  <div className="absolute left-0 top-full mt-1 z-50 hidden group-hover:block w-auto bg-gray-900 border border-yellow-700/50 rounded-lg p-2 text-xs text-gray-300 shadow-xl space-y-1">
                    {data?.exchange === 'bitget' ? (
                      <>
                        <div className="text-yellow-500 font-mono whitespace-nowrap">official_rate = min_rate 持續 &gt;5min，mark-index P 顯著</div>
                        <div>Bitget indicative 費率未反映即時溢價（報價失靈）</div>
                        <div>預測已切換為追蹤官方費率模式</div>
                      </>
                    ) : (
                      <>
                        <div className="text-yellow-500 font-mono whitespace-nowrap">funding_rate_indicative = interest_rate / periods_per_day</div>
                        <div>代表 premium_index 恆為 0（交易所外部報價失靈）</div>
                        <div>預測已切換為追蹤官方費率模式，不使用訂單簿溢價</div>
                      </>
                    )}
                  </div>
                </span>
              )}
            </div>
            <div className={`text-2xl sm:text-3xl font-bold tabular-nums leading-none mt-1 ${premiumColor(data?.instant_premium_pct)}`}>
              {pct(data?.instant_premium_pct, 4)}
            </div>
            <div className="text-[10px] text-gray-600 mt-1">
              {data?.premium_feed_down ? 'premium_index = 0，費率退回利率 I' : '(mark − idx) / idx'}
            </div>
          </div>
        </div>

        {/* ══════════════════════════════════════════
            P2 + P3：預測區
        ══════════════════════════════════════════ */}
        <div className="grid grid-cols-2 gap-3">

          {/* P2: 下一分鐘費率預測 */}
          <div className="rounded-2xl p-4 sm:p-5 bg-gray-900 border border-blue-800 flex flex-col gap-1">
            <div className="text-xs text-blue-400 uppercase tracking-wide">下一分鐘費率</div>
            <div className={`text-2xl sm:text-3xl font-bold tabular-nums leading-none mt-1 ${rateColor(nextRatePct)}`}>
              {pctPlain(nextRatePct, 5)}
            </div>
            {/* 本分鐘採樣進度條（tick 蒐集量 = 預測可信度） */}
            {(() => {
              const elapsed = nextMinuteSec != null ? 60 - nextMinuteSec : 0
              const confPct = Math.min(100, Math.round(elapsed / 60 * 100))
              const tickCount = data?.tick_count ?? 0
              return (
                <>
                  <div className="w-full h-1.5 bg-gray-800 rounded-full mt-2 overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all duration-1000 ${confPct >= 80 ? 'bg-green-500' : confPct >= 40 ? 'bg-yellow-500' : 'bg-gray-600'}`}
                      style={{ width: `${confPct}%` }}
                    />
                  </div>
                  <div className="text-[10px] mt-0.5 flex justify-between">
                    {isPreSample
                      ? <span className="text-green-500">採樣前精確預測</span>
                      : <span className={confPct >= 80 ? 'text-green-600' : confPct >= 40 ? 'text-yellow-600' : 'text-gray-600'}>
                          可信度 {confPct}%（{tickCount} ticks）
                        </span>
                    }
                    <span className="text-gray-600">{nextMinuteSec != null ? `${nextMinuteSec}s` : '--'}</span>
                  </div>
                </>
              )
            })()}
          </div>

          {/* P3: 結算費率預測 */}
          <div className="rounded-2xl p-4 sm:p-5 bg-gray-900 border border-purple-800 flex flex-col gap-1">
            <div className="flex items-center gap-1.5">
              <div className="text-xs text-purple-400 uppercase tracking-wide">結算費率預測</div>
              {/* 預測可信度警示 */}
              {(predData?.high_volatility || predData?.is_noisy_window) && (
                <span className="relative group cursor-help">
                  <span className="text-yellow-400 text-xs leading-none">⚠ 預測精度下降</span>
                  <div className="absolute left-0 top-full mt-1 z-50 hidden group-hover:block bg-gray-900 border border-yellow-700/50 rounded-lg p-3 text-xs text-gray-300 shadow-xl space-y-2" style={{minWidth:'260px'}}>
                    {predData?.high_volatility && (
                      <div>
                        <div className="text-yellow-400 font-medium mb-0.5">溢價劇烈波動</div>
                        <div className="text-gray-400">近 20 筆溢價的標準差 &gt; 0.3%，市場溢價不穩定，TWAP 預測誤差會偏大。</div>
                      </div>
                    )}
                    {predData?.is_noisy_window && (
                      <div>
                        <div className="text-yellow-400 font-medium mb-0.5">結算窗口後期（已過 70%）</div>
                        <div className="text-gray-400">結算週期剩餘時間少，費率已大致確定，但此時每分鐘推算溢價的公式誤差會被放大。結算越近，預測數字越可能受雜訊影響。</div>
                      </div>
                    )}
                  </div>
                </span>
              )}
            </div>
            <div className={`text-2xl sm:text-3xl font-bold tabular-nums leading-none mt-1 ${rateColor(predData?.predicted_rate_pct)}`}>
              {pctPlain(predData?.predicted_rate_pct, 5)}
            </div>
            {/* 進度條 */}
            <div className="w-full h-1.5 bg-gray-800 rounded-full mt-2 overflow-hidden">
              <div
                className="h-full bg-purple-600 rounded-full transition-all duration-1000"
                style={{ width: `${settlePct}%` }}
              />
            </div>
            <div className="text-[10px] text-gray-600 mt-0.5">
              {fmt(data?.elapsed_min, 0)}/{data?.window_minutes ?? '--'}min 已過
            </div>
          </div>
        </div>

        {/* ══════════════════════════════════════════
            詳細資料
        ══════════════════════════════════════════ */}
        <div className="rounded-xl bg-gray-900 border border-gray-800 overflow-hidden">
            <div className="px-4 pb-4 space-y-4 pt-4">

              {/* TWAP 資訊 */}
              <div>
                <div className="text-xs text-gray-500 mb-2">TWAP 溢價 / 計算費率</div>
                <div className="grid grid-cols-3 gap-2 text-xs">
                  <div className="bg-gray-800 rounded-lg p-2 text-center">
                    <div className="text-[10px] text-gray-600">TWAP 溢價</div>
                    <div className={`font-bold ${premiumColor(data?.twap_premium_pct)}`}>{pct(data?.twap_premium_pct, 6)}</div>
                  </div>
                  <div className="bg-gray-800 rounded-lg p-2 text-center">
                    <div className="text-[10px] text-gray-600">計算費率</div>
                    <div className={`font-bold ${rateColor(data?.calc_rate_pct)}`}>{pctPlain(data?.calc_rate_pct, 4)}</div>
                  </div>
                  <div className="bg-gray-800 rounded-lg p-2 text-center">
                    <div className="text-[10px] text-gray-600">樣本數</div>
                    <div className="font-bold text-gray-300">{data?.history_count ?? 0}/{data?.window_minutes ?? '--'}</div>
                  </div>
                </div>
                {exchange === 'bitget' && (data?.history_count ?? 0) < (data?.window_minutes ?? 480) && (
                  <div className="text-[10px] text-yellow-600 mt-1">⚠ Bitget 無歷史 premiumIndex，TWAP 即時累積中</div>
                )}
              </div>

              {/* 逆推面板 */}
              <div>
                <div className="text-xs text-gray-500 mb-2">
                  逆推：剩餘 {fmt(data?.remain_min, 0)}min 需維持的平均溢價
                </div>
                <div className="grid grid-cols-3 gap-2 text-xs">
                  {[
                    { label: '溢價→0', val: data?.rate_if_zero_pct, isRate: true },
                    { label: `→ +${fmt(data?.cap_pct, 2)}%`, val: data?.needed_for_cap_pct },
                    { label: `→ -${fmt(data?.cap_pct, 2)}%`, val: data?.needed_for_neg_cap_pct },
                  ].map(({ label, val, isRate }) => {
                    const color = val == null ? 'text-gray-500'
                      : isRate ? (val > 0 ? 'text-green-400' : val < 0 ? 'text-red-400' : 'text-gray-400')
                      : (data?.instant_premium_pct != null && data.instant_premium_pct <= val ? 'text-green-400' : 'text-red-400')
                    return (
                      <div key={label} className="bg-gray-800 rounded-lg p-2 text-center">
                        <div className="text-[10px] text-gray-600">{label}</div>
                        <div className={`font-bold ${color}`}>
                          {pct(val, 4)}
                        </div>
                      </div>
                    )
                  })}
                </div>
                {/* 自訂目標逆推 */}
                <div className="mt-2 flex gap-2 items-center">
                  <span className="text-[10px] text-gray-500 shrink-0">→</span>
                  <input
                    type="number"
                    step="0.01"
                    placeholder="自訂目標%"
                    value={customTarget}
                    onChange={e => setCustomTarget(e.target.value)}
                    onKeyDown={async e => {
                      if (e.key !== 'Enter') return
                      const t = parseFloat(customTarget)
                      if (isNaN(t)) return
                      setCustomTargetPending(true)
                      try {
                        const res = await fetch(`/api/inverse?target=${t / 100}`)
                        const d = await res.json()
                        setCustomNeeded(d.needed_pct ?? null)
                      } catch {}
                      setCustomTargetPending(false)
                    }}
                    className="w-28 bg-gray-800 text-white text-xs rounded px-2 py-1 border border-gray-700 focus:border-blue-500 outline-none"
                  />
                  <button
                    onClick={async () => {
                      const t = parseFloat(customTarget)
                      if (isNaN(t)) return
                      setCustomTargetPending(true)
                      try {
                        const res = await fetch(`/api/inverse?target=${t / 100}`)
                        const d = await res.json()
                        setCustomNeeded(d.needed_pct ?? null)
                      } catch {}
                      setCustomTargetPending(false)
                    }}
                    disabled={!customTarget || customTargetPending}
                    className="text-[10px] px-2 py-1 bg-blue-700 hover:bg-blue-600 disabled:bg-gray-700 text-white rounded"
                  >
                    {customTargetPending ? '…' : '計算'}
                  </button>
                  {customNeeded != null && (
                    <div className={`text-xs font-bold ${data?.instant_premium_pct != null && data.instant_premium_pct <= customNeeded ? 'text-green-400' : 'text-red-400'}`}>
                      需 {customNeeded.toFixed(4)}%
                    </div>
                  )}
                </div>
                <div className="text-[10px] text-gray-700 mt-1">
                  F = clamp(P + clamp(I−P, ±{data?.clamp_inner_pct ?? '--'}%){data?.result_scale != null && data.result_scale !== 1 ? ` ×${fmt(data.result_scale, 4)}` : ''}, ±{data?.cap_pct ?? '--'}%)
                  ｜I={data?.interest_rate_pct ?? '--'}%
                </div>
              </div>

              {/* Premium 圖表 */}
              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <div className="text-xs text-gray-500">Premium 歷史（最近 60min）</div>
                  <div className="flex gap-3 text-[10px] text-gray-600">
                    <span className="flex items-center gap-1"><span className="w-2 h-1.5 bg-red-500 inline-block rounded" />正</span>
                    <span className="flex items-center gap-1"><span className="w-2 h-1.5 bg-green-500 inline-block rounded" />負</span>
                  </div>
                </div>
                <PremiumMiniChart chartData={chart} />
                {Array.isArray(chart) && chart.length > 0 && (
                  <div className="flex justify-between text-[10px] text-gray-700 mt-1">
                    <span>{new Date(chart[0].ts * 1000).toLocaleTimeString()}</span>
                    <span>{new Date(chart[chart.length - 1].ts * 1000).toLocaleTimeString()}</span>
                  </div>
                )}
              </div>

              {/* 預測準確度統計（摘要，詳細見右側表格）*/}
              {stats && stats.count > 0 && (
                <div>
                  <div className="text-xs text-gray-500 mb-2">預測準確度（共 {stats.count} 筆）</div>
                  <div className="grid grid-cols-3 gap-2">
                    {[
                      { label: 'MAE', val: stats.mae },
                      { label: 'Bias', val: stats.bias },
                      { label: 'RMSE', val: stats.rmse },
                    ].map(({ label, val }) => {
                      const abv = Math.abs(val ?? 0)
                      const color = abv < 0.003 ? 'text-green-400' : abv < 0.01 ? 'text-yellow-400' : 'text-red-400'
                      return (
                        <div key={label} className="bg-gray-800 rounded-lg p-2 text-center">
                          <div className="text-[10px] text-gray-600">{label}</div>
                          <div className={`font-bold text-xs tabular-nums ${color}`}>
                            {val != null ? `${(val * 100).toFixed(2)}bp` : '--'}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}

              {/* 完整數據 */}
              <div>
                <div className="text-xs text-gray-500 mb-2">完整數據</div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-1 text-[11px]">
                  {[
                    ['指數價格', fmtPrice(data?.index_price)],
                    ['標記價格', fmtPrice(data?.mark_price)],
                    ['Bid1', fmtPrice(data?.bid1)],
                    ['Ask1', fmtPrice(data?.ask1)],
                    ['即時溢價', pct(data?.instant_premium_pct, 6)],
                    ['TWAP 溢價', pct(data?.twap_premium_pct, 6)],
                    ['計算費率', pctPlain(predData?.calc_rate_pct, 6), '本地 TWAP 直接套公式得出的費率，僅供診斷。與官方費率差距越大，代表本地 kline 採樣越不準。預測邏輯不使用此值。'],
                    ['官方費率', pctPlain(data?.official_rate_pct, 6)],
                    ['下分鐘費率', pctPlain(predData?.next_minute_rate_pct, 6)],
                    ['預測 TWAP', pct(predData?.predicted_twap_pct, 6)],
                    ['預測結算', pctPlain(predData?.predicted_rate_pct, 6)],
                    ['剩餘秒數', `${countdown ?? '--'}s`],
                  ].map(([label, value, tip]) => (
                    <div key={label} className="flex justify-between gap-1">
                      <span className={`text-gray-600 ${tip ? 'cursor-help border-b border-dotted border-gray-700' : ''} relative group`}>
                        {label}
                        {tip && (
                          <div className="absolute bottom-full left-0 mb-1 hidden group-hover:block bg-gray-800 border border-gray-600 rounded px-2 py-1.5 text-[10px] text-gray-300 z-20 pointer-events-none" style={{ width: 220 }}>
                            {tip}
                          </div>
                        )}
                      </span>
                      <span className="text-gray-400 font-mono">{value}</span>
                    </div>
                  ))}
                </div>
              </div>

            </div>
        </div>

      </div>{/* end left column */}

      {/* ─── 右側：預測準確度表格 ─── */}
      <div className="flex-1 min-w-[360px] sticky top-4">
        <div className="rounded-xl bg-gray-900 border border-gray-800 overflow-hidden">
          <div className="px-3 py-2.5 border-b border-gray-800 flex items-center justify-between">
            <span className="text-xs font-medium text-gray-400">預測準確度</span>
            {stats && stats.count > 0 && (
              <span className="text-[10px] text-gray-600">
                {stats.count}筆 · MAE {(stats.mae * 100).toFixed(2)}bp
              </span>
            )}
          </div>
          <div className="overflow-auto max-h-[calc(100vh-8rem)]">
            {(!stats || stats.error || !stats.count) ? (
              <div className="text-xs text-gray-600 text-center py-6">等待資料...</div>
            ) : (
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-gray-600 border-b border-gray-800 sticky top-0 bg-gray-900">
                    <th className="text-left px-2 py-1.5 whitespace-nowrap">時間</th>
                    <th className="text-right px-2 py-1.5 whitespace-nowrap">@秒</th>
                    <th className="text-right px-2 py-1.5 whitespace-nowrap">預測</th>
                    <th className="text-right px-2 py-1.5 whitespace-nowrap">實際</th>
                    <th className="text-right px-2 py-1.5 whitespace-nowrap">絕對誤差</th>
                    <th className="text-right px-2 py-1.5 whitespace-nowrap">相對誤差</th>
                  </tr>
                </thead>
                <tbody>
                  {/* 待確認行：PRE-SAMPLE 後凍結顯示，結算確認才消失 */}
                  {pendingRow && (
                    <tr className="border-b border-blue-900/50 bg-blue-950/30">
                      <td className="px-2 py-1 text-blue-500 tabular-nums whitespace-nowrap">
                        {new Date(pendingRow.ts * 1000).toLocaleTimeString('zh-TW', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                      </td>
                      <td className="px-2 py-1 text-right text-gray-600">--</td>
                      <td className={`px-2 py-1 text-right tabular-nums font-medium whitespace-nowrap ${rateColor(pendingRow.rate)}`}>
                        {pendingRow.rate.toFixed(5)}%
                      </td>
                      <td className="px-2 py-1 text-right text-gray-600">--</td>
                      <td className="px-2 py-1 text-right text-gray-600">--</td>
                      <td className="px-2 py-1 text-right text-gray-600">--</td>
                    </tr>
                  )}
                  {(stats.recent || []).map((r, i) => {
                    const errBp = r.error * 100
                    const errColor = Math.abs(errBp) < 0.3 ? 'text-green-400' : Math.abs(errBp) < 1.0 ? 'text-yellow-400' : 'text-red-400'
                    const relErr = r.actual !== 0 ? r.error / Math.abs(r.actual) * 100 : null
                    const relErrColor = relErr == null ? 'text-gray-500' : Math.abs(relErr) < 5 ? 'text-green-400' : Math.abs(relErr) < 15 ? 'text-yellow-400' : 'text-red-400'
                    const isLatest = i === 0
                    return (
                      <tr key={r.ts} className="border-b border-gray-900 hover:bg-gray-800/50">
                        <td className={`px-2 py-1 tabular-nums whitespace-nowrap ${isLatest ? 'text-blue-400' : 'text-gray-500'}`}>
                          {(() => {
                            const d = new Date(r.ts * 1000)
                            const today = new Date()
                            const isToday = d.toDateString() === today.toDateString()
                            const timeStr = d.toLocaleTimeString('zh-TW', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
                            return isToday ? timeStr : `${(d.getMonth()+1).toString().padStart(2,'0')}/${d.getDate().toString().padStart(2,'0')} ${timeStr}`
                          })()}
                        </td>
                        <td className={`px-2 py-1 text-right tabular-nums whitespace-nowrap ${isLatest ? 'text-blue-600' : 'text-gray-600'}`}>
                          {r.sample_sec != null ? `${r.sample_sec}s` : '--'}
                        </td>
                        <td className={`px-2 py-1 text-right tabular-nums whitespace-nowrap ${isLatest ? 'text-blue-300' : 'text-gray-300'}`}>
                          {r.predicted.toFixed(5)}%
                        </td>
                        <td className={`px-2 py-1 text-right tabular-nums whitespace-nowrap ${isLatest ? 'text-blue-300' : 'text-gray-300'}`}>
                          {r.actual.toFixed(5)}%
                        </td>
                        <td className={`px-2 py-1 text-right tabular-nums font-medium whitespace-nowrap ${errColor}`}>
                          {errBp >= 0 ? '+' : ''}{errBp.toFixed(2)}bp
                        </td>
                        <td className={`px-2 py-1 text-right tabular-nums font-medium whitespace-nowrap ${relErrColor}`}>
                          {relErr != null ? `${relErr >= 0 ? '+' : ''}${relErr.toFixed(2)}%` : '--'}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>

      {/* ─── 右側：多幣監控列表 ─── */}
      <div className="w-52 shrink-0 space-y-2 sticky top-4">
        <div className="text-xs text-gray-500 font-medium px-1">監控列表</div>

        {/* 已監控幣種（可拖拉排序）*/}
        <div className="space-y-1">
          {(monitors || []).slice().sort((a, b) => {
            const ai = monitorOrder.indexOf(a.key)
            const bi = monitorOrder.indexOf(b.key)
            return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi)
          }).map(m => {
            const isSelected = exchange === m.exchange && symbol === m.symbol
            const rPct = m.next_minute_rate_pct
            const rColor = rPct == null ? 'text-gray-500' : rPct > 0 ? 'text-green-400' : rPct < 0 ? 'text-red-400' : 'text-gray-400'
            const exLabel = { binance: 'BN', bybit: 'BY', bitget: 'BG', gateio: 'GT', aster: 'AS', okx: 'OK' }[m.exchange] ?? '?'
            const exColor = { binance: 'bg-yellow-600 text-black', bybit: 'bg-orange-600 text-white', bitget: 'bg-blue-600 text-white', gateio: 'bg-teal-600 text-white', aster: 'bg-purple-600 text-white', okx: 'bg-gray-200 text-black' }[m.exchange] ?? 'bg-gray-600 text-white'
            return (
              <div
                key={m.key}
                draggable
                onDragStart={() => { dragKey.current = m.key }}
                onDragOver={e => { e.preventDefault(); dragOverKey.current = m.key }}
                onDrop={() => {
                  const from = dragKey.current
                  const to = dragOverKey.current
                  if (!from || !to || from === to) return
                  setMonitorOrder(prev => {
                    const next = [...prev]
                    const fi = next.indexOf(from)
                    const ti = next.indexOf(to)
                    if (fi === -1 || ti === -1) return prev
                    next.splice(fi, 1)
                    next.splice(ti, 0, from)
                    localStorage.setItem('monitor_order', JSON.stringify(next))
                    return next
                  })
                  dragKey.current = null
                  dragOverKey.current = null
                }}
                onDragEnd={() => { dragKey.current = null; dragOverKey.current = null }}
                onClick={() => handleSelectMonitor(m.exchange, m.symbol)}
                className={`flex items-center gap-1.5 rounded-lg px-2 py-1.5 cursor-pointer group transition-colors ${isSelected ? 'bg-gray-700 border border-gray-600' : 'bg-gray-900 border border-gray-800 hover:bg-gray-800'}`}
              >
                <span
                  className="text-gray-600 hover:text-gray-400 cursor-grab active:cursor-grabbing text-xs leading-none shrink-0 select-none"
                  onMouseDown={e => e.stopPropagation()}
                  title="拖拉排序"
                >⠿</span>
                <span className={`text-[9px] font-bold px-1 py-0.5 rounded ${exColor} shrink-0`}>{exLabel}</span>
                <div className="flex-1 min-w-0">
                  <div className="text-xs font-medium text-gray-200 truncate">{m.symbol.replace('USDT', '')}</div>
                  <div className={`text-[10px] tabular-nums ${rColor}`}>
                    {rPct != null ? `${rPct >= 0 ? '+' : ''}${rPct.toFixed(4)}%` : '--'}
                  </div>
                </div>
                <div className="flex items-center gap-1.5 shrink-0">
                  <span className={`w-1.5 h-1.5 rounded-full ${m.ws_ready ? 'bg-green-500' : 'bg-red-500'}`} />
                  <button
                    onClick={e => { e.stopPropagation(); handleRemove(m.exchange, m.symbol) }}
                    className="text-xs text-gray-500 hover:text-red-400 hover:bg-red-900/30 rounded px-1 py-0.5 transition-colors"
                    title="移除"
                  >✕</button>
                </div>
              </div>
            )
          })}
        </div>

      </div>

      </div>{/* end max-w flex */}
    </div>
  )
}
