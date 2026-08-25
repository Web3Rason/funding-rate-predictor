# -*- coding: utf-8 -*-
"""
雙交易所結算監控：BN + BY 同時監控 RIVERUSDT
每分鐘採樣，04:00 結算後輸出 HTML 報告
"""
import asyncio
import time
import datetime
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

from exchange_client import ExchangeClient
from funding_engine import FundingEngine

SYMBOL = "RIVERUSDT"
REPORT_PATH = os.path.join(os.path.dirname(__file__), "settlement_report_RIVER.html")

records = {"binance": [], "bybit": []}
settled = {"binance": False, "bybit": False}
actual_rate = {"binance": None, "bybit": None}
last_next_ft = {"binance": 0, "bybit": 0}
engines = {}
clients = {}

# ── 初始化 ──────────────────────────────────────────────────────

async def init_exchange(exchange):
    client = ExchangeClient(exchange=exchange, symbol=SYMBOL)
    await client.initialize()
    window = client.window_minutes
    engine = FundingEngine(exchange=exchange, window_minutes=window, cap_override=client.cap)

    ticker = await client.get_ticker()
    next_ft_ms = ticker.get("next_funding_time", 0)
    start_ms = next_ft_ms - window * 60 * 1000 if next_ft_ms else None

    klines = await client.get_klines_history(limit=window, start_ms=start_ms)
    if klines:
        engine.preload_history(klines)
        print(f"[{exchange.upper()}] 預填 {len(klines)} 筆 klines，窗口 {window}min")
    else:
        print(f"[{exchange.upper()}] 無歷史 klines，從即時累積")

    last_next_ft[exchange] = next_ft_ms
    clients[exchange] = client
    engines[exchange] = engine

# ── 採樣 ─────────────────────────────────────────────────────────

async def sample(exchange):
    client = clients[exchange]
    engine = engines[exchange]

    ticker = await client.get_ticker()
    next_ft = ticker.get("next_funding_time", 0)

    # 偵測結算
    if last_next_ft[exchange] and next_ft != last_next_ft[exchange] and not settled[exchange]:
        settled[exchange] = True
        # 查歷史 API 取真實結算費率
        try:
            import httpx
            if exchange == "binance":
                url = "https://fapi.binance.com/fapi/v1/fundingRate"
                params = {"symbol": SYMBOL, "limit": 1}
                async with httpx.AsyncClient(timeout=8) as c:
                    r = await c.get(url, params=params)
                    data = r.json()
                    actual_rate[exchange] = float(data[0]["fundingRate"]) * 100 if data else None
            else:
                url = "https://api.bybit.com/v5/market/funding/history"
                params = {"category": "linear", "symbol": SYMBOL, "limit": 1}
                async with httpx.AsyncClient(timeout=8) as c:
                    r = await c.get(url, params=params)
                    data = r.json()["result"]["list"]
                    actual_rate[exchange] = float(data[0]["fundingRate"]) * 100 if data else None
        except Exception as e:
            actual_rate[exchange] = None
            print(f"[{exchange.upper()}] 查結算費率失敗: {e}")
        print(f"[{exchange.upper()}] 結算！actual = {actual_rate[exchange]:+.6f}%" if actual_rate[exchange] else f"[{exchange.upper()}] 結算！費率查詢失敗")
        engine.reset_history()

    last_next_ft[exchange] = next_ft

    # 更新 kline P
    p = ticker.get("instant_premium", 0)
    try:
        klines = await client.get_klines_history(limit=1)
        if klines:
            p = klines[0]["premium"]
    except Exception:
        pass

    current_minute = int(time.time() // 60)
    if not records[exchange] or records[exchange][-1].get("_minute") != current_minute:
        engine.add_sample(p)

    result = engine.compute(ticker, next_ft)
    now = datetime.datetime.now()
    ts = now.strftime("%H:%M:%S")
    remain_sec = result["remain_sec"]

    rec = {
        "_minute": current_minute,
        "ts": ts,
        "remain_sec": remain_sec,
        "calc": result["calc_rate_pct"],
        "official": result["official_rate_pct"],
        "predicted": result["predicted_rate_pct"],
        "forecast_p": result["forecast_p_pct"],
        "instant_p": result["instant_premium_pct"],
    }
    records[exchange].append(rec)

    return ts, remain_sec, result["calc_rate_pct"], result["official_rate_pct"], result["predicted_rate_pct"], result["forecast_p_pct"]

# ── 報告生成 ──────────────────────────────────────────────────────

def milestone_records(exchange, targets=(3600, 3000, 2400, 1800, 1200, 600, 300, 120, 60, 10)):
    pre = [r for r in records[exchange] if not (settled[exchange] and r["remain_sec"] > 3500)]
    result = []
    seen = set()
    for t in targets:
        closest = min(pre, key=lambda r: abs(r["remain_sec"] - t), default=None)
        if closest and abs(closest["remain_sec"] - t) < 90 and t not in seen:
            seen.add(t)
            result.append((t // 60, closest))
    return result

def color(val, threshold_warn=0.1, threshold_good=0.05):
    if val is None:
        return "text-white"
    av = abs(val)
    if av <= threshold_good:
        return "good"
    elif av <= threshold_warn:
        return "warn"
    return "neg"

def gen_report():
    bn_actual = actual_rate["binance"]
    by_actual = actual_rate["bybit"]
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    def rows(exchange, actual):
        ms = milestone_records(exchange)
        out = ""
        for (min_left, r) in ms:
            err = round(r["predicted"] - actual, 4) if actual is not None else None
            err_str = f"{err:+.4f}%" if err is not None else "--"
            err_cls = color(err) if err is not None else ""
            out += f'<tr><td>{r["ts"]}</td><td>{min_left}min</td>'
            out += f'<td class="neg">{r["predicted"]:+.4f}%</td>'
            out += f'<td class="{err_cls}">{err_str}</td>'
            out += f'<td>{r["forecast_p"]:+.4f}%</td>'
            out += f'<td class="neg">{r["official"]:+.4f}%</td></tr>\n'
        # 最終
        pre = [r for r in records[exchange] if not (settled[exchange] and r["remain_sec"] > 3500)]
        if pre:
            last = pre[-1]
            err = round(last["predicted"] - actual, 4) if actual is not None else None
            err_str = f"{err:+.4f}%" if err is not None else "--"
            err_cls = color(err) if err is not None else ""
            out += f'<tr style="background:#0a1628"><td><b>{last["ts"]}</b></td><td><b>~0s</b></td>'
            out += f'<td class="neg"><b>{last["predicted"]:+.4f}%</b></td>'
            out += f'<td class="{err_cls}"><b>{err_str}</b></td>'
            out += f'<td>{last["forecast_p"]:+.4f}%</td>'
            out += f'<td class="neg">{last["official"]:+.4f}%</td></tr>\n'
        return out

    bn_actual_str = f"{bn_actual:+.6f}%" if bn_actual is not None else "查詢失敗"
    by_actual_str = f"{by_actual:+.6f}%" if by_actual is not None else "查詢失敗"

    # 最終誤差
    def final_err(exchange, actual):
        pre = [r for r in records[exchange] if not (settled[exchange] and r["remain_sec"] > 3500)]
        if pre and actual is not None:
            return round(pre[-1]["predicted"] - actual, 4)
        return None

    bn_err = final_err("binance", bn_actual)
    by_err = final_err("bybit", by_actual)
    bn_err_str = f"{bn_err:+.4f}%" if bn_err is not None else "--"
    by_err_str = f"{by_err:+.4f}%" if by_err is not None else "--"

    html = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<title>5021 雙交易所結算報告 RIVER</title>
<style>
  body {{ font-family: monospace; background: #0f0f0f; color: #e0e0e0; padding: 24px; }}
  h1 {{ color: #7dd3fc; margin-bottom: 4px; }}
  h2 {{ color: #a78bfa; margin-top: 28px; margin-bottom: 8px; font-size: 14px; }}
  .meta {{ color: #6b7280; font-size: 12px; margin-bottom: 24px; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 16px; font-size: 13px; }}
  th {{ background: #1e1e2e; color: #94a3b8; text-align: right; padding: 6px 12px; border-bottom: 1px solid #374151; }}
  th:first-child {{ text-align: left; }}
  td {{ padding: 5px 12px; text-align: right; border-bottom: 1px solid #1f2937; }}
  td:first-child {{ text-align: left; color: #94a3b8; }}
  .pos {{ color: #34d399; }} .neg {{ color: #f87171; }} .warn {{ color: #fbbf24; }} .good {{ color: #34d399; }}
  .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }}
  .box {{ background: #1e1e2e; border: 1px solid #374151; border-radius: 8px; padding: 16px; }}
  .box .label {{ color: #6b7280; font-size: 11px; }}
  .box .value {{ font-size: 18px; font-weight: bold; margin-top: 2px; }}
  .box .note {{ font-size: 11px; color: #4b5563; margin-top: 4px; }}
  .bn {{ border-color: #78350f; }} .by {{ border-color: #1e3a5f; }}
  .section {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  hr {{ border: none; border-top: 1px solid #1f2937; margin: 20px 0; }}
</style>
</head>
<body>
<h1>5021 雙交易所結算報告</h1>
<div class="meta">RIVERUSDT · 2026-03-26 04:00 結算 · 生成時間 {now_str}</div>

<div class="grid">
  <div class="box bn">
    <div class="label">BN 實際結算</div>
    <div class="value neg">{bn_actual_str}</div>
    <div class="note">Binance fundingRate history</div>
  </div>
  <div class="box bn">
    <div class="label">BN 最終預測誤差</div>
    <div class="value good">{bn_err_str}</div>
    <div class="note">結算前最後一筆 predicted</div>
  </div>
  <div class="box by">
    <div class="label">BY 實際結算</div>
    <div class="value neg">{by_actual_str}</div>
    <div class="note">Bybit funding/history</div>
  </div>
  <div class="box by">
    <div class="label">BY 最終預測誤差</div>
    <div class="value good">{by_err_str}</div>
    <div class="note">結算前最後一筆 predicted</div>
  </div>
</div>

<div class="section">
  <div>
    <h2>Binance RIVERUSDT</h2>
    <table>
      <thead><tr><th>時間</th><th>剩餘</th><th>predicted</th><th>誤差</th><th>forecast_p</th><th>官方(即時)</th></tr></thead>
      <tbody>{rows("binance", bn_actual)}</tbody>
    </table>
  </div>
  <div>
    <h2>Bybit RIVERUSDT</h2>
    <table>
      <thead><tr><th>時間</th><th>剩餘</th><th>predicted</th><th>誤差</th><th>forecast_p</th><th>官方(即時)</th></tr></thead>
      <tbody>{rows("bybit", by_actual)}</tbody>
    </table>
  </div>
</div>

</body>
</html>"""

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n報告已寫入: {REPORT_PATH}")

# ── 結算重置 ──────────────────────────────────────────────────────

def reset_cycle():
    """每次結算後重置狀態，準備下一個週期"""
    global records, settled, actual_rate, last_next_ft
    records = {"binance": [], "bybit": []}
    settled = {"binance": False, "bybit": False}
    actual_rate = {"binance": None, "bybit": None}
    last_next_ft = {"binance": 0, "bybit": 0}

# ── 主循環 ────────────────────────────────────────────────────────

async def main():
    cycle = 0
    print(f"[{datetime.datetime.now():%H:%M:%S}] 初始化 BN + BY RIVERUSDT...")
    await asyncio.gather(
        init_exchange("binance"),
        init_exchange("bybit"),
    )

    while True:
        cycle += 1
        target_time = datetime.datetime.fromtimestamp(
            min(last_next_ft["binance"], last_next_ft["bybit"]) / 1000
        ).strftime("%H:%M") if min(last_next_ft["binance"], last_next_ft["bybit"]) else "下次"
        print(f"[{datetime.datetime.now():%H:%M:%S}] 開始監控週期 #{cycle}，目標 {target_time} 結算\n")

        while True:
            try:
                # 結算後兩邊都完成才生成報告
                if settled["binance"] and settled["bybit"]:
                    await asyncio.sleep(10)
                    gen_report()
                    import subprocess
                    subprocess.Popen(f'start "" "{REPORT_PATH}"', shell=True)
                    # 印出本次結算摘要
                    bn_a = actual_rate["binance"]
                    by_a = actual_rate["bybit"]
                    bn_recs = [r for r in records["binance"] if not (settled["binance"] and r["remain_sec"] > 3500)]
                    by_recs = [r for r in records["bybit"] if not (settled["bybit"] and r["remain_sec"] > 3500)]
                    if bn_recs and bn_a is not None:
                        bn_err = bn_recs[-1]["predicted"] - bn_a
                        print(f"[BN 週期#{cycle}] pred={bn_recs[-1]['predicted']:+.4f}%  actual={bn_a:+.4f}%  誤差={bn_err:+.4f}%")
                    if by_recs and by_a is not None:
                        by_err = by_recs[-1]["predicted"] - by_a
                        print(f"[BY 週期#{cycle}] pred={by_recs[-1]['predicted']:+.4f}%  actual={by_a:+.4f}%  誤差={by_err:+.4f}%")
                    print(f"\n==== 週期 #{cycle} 完成，重新初始化下一週期 ====\n")
                    break

                # 採樣
                bn_ts, bn_remain, bn_calc, bn_off, bn_pred, bn_fp = await sample("binance")
                by_ts, by_remain, by_calc, by_off, by_pred, by_fp = await sample("bybit")

                print(f"[{bn_ts}] BN calc={bn_calc:+.4f}%  off={bn_off:+.4f}%  pred={bn_pred:+.4f}%  fp={bn_fp:+.4f}%  remain={bn_remain//60}m{bn_remain%60}s")
                print(f"[{by_ts}] BY calc={by_calc:+.4f}%  off={by_off:+.4f}%  pred={by_pred:+.4f}%  fp={by_fp:+.4f}%  remain={by_remain//60}m{by_remain%60}s")
                print()

            except Exception as e:
                print(f"[ERROR] {e}")

            # 最後 2 分鐘每 10 秒，其他每 55 秒
            bn_remain = records["binance"][-1]["remain_sec"] if records["binance"] else 9999
            by_remain = records["bybit"][-1]["remain_sec"] if records["bybit"] else 9999
            min_remain = min(bn_remain, by_remain)
            await asyncio.sleep(10 if min_remain < 120 else 55)

        # 重置並重新初始化
        reset_cycle()
        await asyncio.gather(
            init_exchange("binance"),
            init_exchange("bybit"),
        )

    await asyncio.gather(
        clients["binance"].close(),
        clients["bybit"].close(),
    )

if __name__ == "__main__":
    asyncio.run(main())
