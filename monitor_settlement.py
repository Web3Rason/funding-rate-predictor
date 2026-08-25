# -*- coding: utf-8 -*-
"""
BY SXP 結算監控腳本
每分鐘記錄一次，等結算後輸出報告
"""
import time
import json
import httpx
import datetime

API = "http://127.0.0.1:3021/api/data"
RECORDS = []
SETTLED = False
ACTUAL_RATE = None

print(f"[{datetime.datetime.now():%H:%M:%S}] 開始監控 BY-SXP 結算...")
print("="*70)

last_next_ft = None

while True:
    try:
        resp = httpx.get(API, timeout=5)
        d = resp.json()
    except Exception as e:
        print(f"[{datetime.datetime.now():%H:%M:%S}] API 錯誤: {e}")
        time.sleep(10)
        continue

    now = datetime.datetime.now()
    ts = now.strftime("%H:%M:%S")
    remain_sec = d.get("remain_sec", 0)
    calc = d.get("calc_rate_pct")
    official = d.get("official_rate_pct")
    predicted = d.get("predicted_rate_pct")
    forecast_p = d.get("forecast_p_pct")
    instant_p = d.get("instant_premium_pct")
    twap_p = d.get("twap_premium_pct")
    next_min = d.get("next_minute_rate_pct")
    history_count = d.get("history_count", 0)
    window = d.get("window_minutes", 480)

    # 偵測結算：remain_sec 回彈到大值（> 3500s）且已有前次紀錄
    if RECORDS and not SETTLED:
        prev_remain = RECORDS[-1]["remain_sec"]
        # 結算：前一秒剩餘很少(<120s)，現在剩很多(>3500s)
        if prev_remain < 300 and remain_sec > 3500:
            SETTLED = True
            ACTUAL_RATE = official  # 結算後官方費率 = 實際結算費率
            print(f"\n{'='*70}")
            print(f"[{ts}] *** 偵測到結算！actual_rate = {official:+.6f}% ***")
            print(f"{'='*70}\n")

    record = {
        "ts": ts,
        "remain_sec": remain_sec,
        "calc": calc,
        "official": official,
        "predicted": predicted,
        "forecast_p": forecast_p,
        "instant_p": instant_p,
        "twap_p": twap_p,
        "next_min": next_min,
        "history_count": history_count,
        "window": window,
    }
    RECORDS.append(record)

    # 每筆都印
    sign = ""
    if not SETTLED:
        sign = f"  remain={remain_sec//60}m{remain_sec%60}s"
    print(f"[{ts}] calc={calc:+.4f}%  official={official:+.4f}%  predicted={predicted:+.4f}%  forecast_p={forecast_p:+.4f}%{sign}")

    # 結算後再等 2 分鐘確認新費率穩定，然後輸出報告
    if SETTLED and remain_sec > 100:
        # 確認新一期已開始
        print(f"\n結算後穩定，開始生成報告...\n")

        # 找最後一筆結算前的預測紀錄（remain_sec 最小的那筆）
        pre_settle = [r for r in RECORDS if not (RECORDS.index(r) >= len(RECORDS)-3 and r["remain_sec"] > 3500)]
        # 取結算前各時間點的預測
        milestones = []
        for target_remain in [3600, 3000, 2400, 1800, 1200, 600, 300, 120, 60, 30]:
            closest = min(pre_settle, key=lambda r: abs(r["remain_sec"] - target_remain), default=None)
            if closest and abs(closest["remain_sec"] - target_remain) < 120:
                milestones.append((target_remain // 60, closest))

        print("="*70)
        print("【結算預測精準度報告】BY-SXPUSDT")
        print(f"實際結算費率: {ACTUAL_RATE:+.6f}%")
        print(f"{'='*70}")
        print(f"{'剩餘':>6}  {'當時預測':>12}  {'誤差':>10}  {'即時P':>10}  {'forecast_p':>12}")
        print(f"{'-'*70}")
        seen = set()
        for (target_min, r) in milestones:
            key = target_min
            if key in seen:
                continue
            seen.add(key)
            err = r["predicted"] - ACTUAL_RATE if r["predicted"] is not None else None
            err_str = f"{err:+.4f}%" if err is not None else "--"
            print(f"{target_min:>4}min  {r['predicted']:>+12.6f}%  {err_str:>10}  {r['instant_p']:>+10.4f}%  {r['forecast_p']:>+12.4f}%")

        # 找最後結算前的 calc_rate
        last_pre = next((r for r in reversed(pre_settle) if r["remain_sec"] < 120), pre_settle[-1] if pre_settle else None)
        if last_pre:
            calc_err = last_pre["calc"] - ACTUAL_RATE
            print(f"\n結算前最後 calc_rate: {last_pre['calc']:+.6f}%  誤差: {calc_err:+.4f}%")
            print(f"結算前最後 predicted: {last_pre['predicted']:+.6f}%  誤差: {(last_pre['predicted']-ACTUAL_RATE):+.4f}%")

        print(f"\n{'='*70}")
        print("改善建議：")

        # 分析誤差趨勢
        errors = []
        for r in pre_settle[-10:]:
            if r["predicted"] is not None:
                errors.append(abs(r["predicted"] - ACTUAL_RATE))
        if errors:
            avg_err = sum(errors) / len(errors)
            print(f"- 最後10筆預測平均絕對誤差: {avg_err:.4f}%")
            if avg_err < 0.01:
                print("- ✅ 精準度高（<0.01%）")
            elif avg_err < 0.05:
                print("- ⚠️  中等誤差（0.01~0.05%）")
            else:
                print("- ❌ 誤差偏大（>0.05%），建議改善 forecast_p 預測方式")

        print("="*70)
        break

    # 結算後不到 100s，等待穩定
    if SETTLED:
        time.sleep(15)
        continue

    # 結算前：每分鐘採樣一次
    if remain_sec > 120:
        time.sleep(55)
    else:
        # 最後 2 分鐘每 10 秒採樣
        time.sleep(10)
