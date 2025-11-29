from supabase import create_client
import os, requests, threading, time, datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Request
import numpy as np
import schedule

load_dotenv()

SUPABASE_URL     = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY")
COINGECKO_KEY    = os.getenv("COINGECKO_KEY")
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CHAT_ID")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI()
#
OI_FLAT_TH = 3  # ±3% 이내 → 횡보로 판정

def send_telegram(msg):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}
    )


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OI 수집 (4H)
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def fetch_OI(symbol="ETH"):
    data = requests.get(
        "https://api.coingecko.com/api/v3/derivatives",
        headers={"x-cg-demo-api-key": COINGECKO_KEY}
    ).json()

    item = next(x for x in data if x["index_id"].upper() == symbol)
    oi = float(item["open_interest"])

    now = (datetime.datetime.utcnow() + datetime.timedelta(hours=9)).isoformat()
    supabase.table("oi_logs").insert({"timestamp": now,"symbol": symbol,"oi": oi}).execute()
    return oi


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Engulfing detection (이전 방식 유지)
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def detect_engulf(prev, curr):
    prev_body = abs(prev["close"] - prev["open"])
    curr_body = abs(curr["close"] - curr["open"])
    if curr_body >= prev_body * 2:
        return "Bullish Engulfing" if curr["close"] > curr["open"] else "Bearish Engulfing"
    return None


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔥 AI 레벨 업그레이드된 분석 로직
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def analyze_signal():

    candles = supabase.table("candle_logs").select("*").order("id", desc=True).limit(50).execute().data
    if len(candles) < 30: return

    curr  = candles[0]
    prev  = candles[1]

    vol_now  = curr["volume"]
    vol_hist = [c["volume"] for c in candles[:20]]
    vol_base = np.mean(vol_hist)
    vol_chg  = ((vol_now - vol_base) / vol_base) * 100


    # 🔥 OI도 단기(6봉) vs 중기(30봉) 평균 비교
    oi_data = supabase.table("oi_logs").select("*").order("id", desc=True).limit(40).execute().data
    if len(oi_data) < 30: return

    oi_short = np.mean([x["oi"] for x in oi_data[:6]])
    oi_long  = np.mean([x["oi"] for x in oi_data[:30]])
    oi_trend = ((oi_short - oi_long) / oi_long) * 100

    engulf = detect_engulf(prev, curr)
    dia = supabase.table("diamond_logs").select("*").order("id", desc=True).limit(1).execute().data
    dia_sig = dia[0]["color"] if dia else None


    #━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🔥 출력 조건 최종 적용
    #━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # 1) 볼륨 ↑ + OI ↑
    if vol_chg > 0 and oi_trend > 0:

        if engulf == "Bullish Engulfing":
            if dia_sig == "green":
                msg = f"[🚀 매수 강세]\nVol +{vol_chg:.1f}% | OI 상승 확장\nBull Engulf + Green DIA"
            else:
                msg = f"[📈 매수 우위]\nVol +{vol_chg:.1f}% | OI 유입\nBull Engulf"

        elif engulf == "Bearish Engulfing":
            if dia_sig == "red":
                msg = f"[🔥 매도 강세]\nVol +{vol_chg:.1f}% | OI 증가 but Bear Engulf + Red DIA"
            else:
                msg = f"[📉 매도 우위]\nVol +{vol_chg:.1f}% | OI 유입 but Bear Engulf"

        else:
            msg = f"[⚠ 강한 체결 but 캔들 중립]\nVol +{vol_chg:.1f}% | OI 상승"

    # 2) 볼륨 ↑ + OI ↓ → SQUEEZE
    elif vol_chg > 0 and oi_trend < 0:
        if engulf == "Bullish Engulfing": msg = f"[⚡ 숏 스퀴즈]\nBull Engulf | Vol + | OI 감소"
        elif engulf == "Bearish Engulfing": msg = f"[⚡ 롱 스퀴즈]\nBear Engulf | Vol + | OI 감소"
        else: msg = "[⚠ 청산발생 + 무방향]"

    # 3) OI Flat → 매집/분배
    elif vol_chg > 0 and abs(oi_trend) <= OI_FLAT_TH:
        if engulf == "Bullish Engulfing": msg = f"[🟢 매집 흐름]\nVol + / OI Flat({oi_trend:.2f}%) → 상방 준비"
        elif engulf == "Bearish Engulfing": msg = f"[🔻 분배 흐름]\nVol + / OI Flat({oi_trend:.2f}%) → 하방 경계"
        else: msg = f"[⚠ 변동성↑ but 중립]\nOI Flat={oi_trend:.2f}%"

    else:
        msg = "[💤 방향 미약 — 관망]"

    send_telegram(msg)


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4H 반복 처리
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4H 주기 실행
schedule.every(4).hours.do(lambda: (fetch_OI("ETH"), analyze_signal()))

def scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1) 

threading.Thread(target=scheduler, daemon=True).start()


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Webhook → OHLC 저장
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/webhook")
async def webhook_receiver(req: Request):
    data = await req.json()
    print("🔥 RECEIVED:", data)   # ★ 가장 중요

    now = (datetime.datetime.utcnow() + datetime.timedelta(hours=9)).isoformat()

    # candle 저장
    supabase.table("candle_logs").insert({
        "timestamp": now,
        "symbol": data.get("symbol"),
        "open":   float(data.get("open")),
        "close":  float(data.get("close")),
        "high":   float(data.get("high")),
        "low":    float(data.get("low")),
        "volume": float(data.get("volume")),
        "time":   data.get("time")
    }).execute()

    # diamond 저장
    t = data.get("type")
    if t is not None and t != "":
        print("💎 DIAMOND DETECTED:", t)
        supabase.table("diamond_logs").insert({
            "timestamp": now,
            "symbol": data.get("symbol"),
            "signal": t,         
            "color": data.get("color", None),
            "time": data.get("time") 
        }).execute()
    else:
        print("❌ type 값 없음 → diamond 미저장")

    return {"status": "ok"}
