from supabase import create_client
import os, requests, time, datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Request
import numpy as np
import schedule
from threading import Thread

load_dotenv()

SUPABASE_URL     = os.getenv("SUPABASE_URL")
SUPABASE_KEY     = os.getenv("SUPABASE_KEY")
COINGECKO_KEY    = os.getenv("COINGECKO_KEY")
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CHAT_ID")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI()

# ±3% 이내 → 횡보로 판정
OI_FLAT_TH = 3  


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 텔레그램 전송
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def send_telegram(msg: str):
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}
        )
        if not res.ok:
            print("❌ Telegram send error:", res.text)
    except Exception as e:
        print("❌ Telegram exception:", e)


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OI 수집 (4H)
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def fetch_OI(symbol: str = "ETH"):
    try:
        res = requests.get(
            "https://api.coingecko.com/api/v3/derivatives",
            headers={"x-cg-demo-api-key": COINGECKO_KEY}
        )
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        print("❌ OI 요청 실패:", e)
        send_telegram("⚠ OI 데이터 요청 실패")
        return None

    try:
        # index_id 가 "ETH" 인 것 찾기 (대소문자 무시)
        item = next(x for x in data if x.get("index_id", "").upper() == symbol.upper())
    except StopIteration:
        print("⚠ OI 데이터 없음:", symbol)
        send_telegram(f"⚠ OI 데이터 없음: {symbol}")
        return None

    try:
        oi = float(item["open_interest"])
    except Exception as e:
        print("❌ open_interest 파싱 실패:", e)
        send_telegram("⚠ OI 데이터 파싱 실패")
        return None

    now = (datetime.datetime.utcnow() + datetime.timedelta(hours=9)).isoformat()
    supabase.table("oi_logs").insert({
        "timestamp": now,
        "symbol": symbol,
        "oi": oi
    }).execute()

    print(f"📊 OI 저장 완료: {symbol} = {oi}")
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
# 로그 정리
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def trim_logs(table, keep=40):
    rows = supabase.table(table).select("id").order("id", desc=True).execute().data
    if len(rows) > keep:
        delete_ids = [r["id"] for r in rows[keep:]]
        supabase.table(table).delete().in_("id", delete_ids).execute()
        print(f"🧹 {table} {len(delete_ids)}개 정리 완료 (keep={keep})")


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔥 AI 레벨 업그레이드된 분석 로직
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def analyze_signal():
    print("🔍 analyze_signal 실행")

    # 최근 캔들 50개 조회
    candles = supabase.table("candle_logs").select("*").order("id", desc=True).limit(50).execute().data
    if len(candles) < 30:
        msg = f"⚠ 분석 실패: candle_logs < 30개 (현재 {len(candles)}개)"
        print(msg)
        send_telegram(msg)
        return

    # 최근 OI 40개 조회
    oi_data = supabase.table("oi_logs").select("*").order("id", desc=True).limit(40).execute().data
    if len(oi_data) < 30:
        msg = f"⚠ 분석 실패: oi_logs < 30개 (현재 {len(oi_data)}개)"
        print(msg)
        send_telegram(msg)
        return

    curr = candles[0]
    prev = candles[1]

    # 볼륨 변화
    vol_now = curr["volume"]
    vol_hist = [c["volume"] for c in candles[:20]]
    vol_base = np.mean(vol_hist)
    vol_chg = ((vol_now - vol_base) / vol_base) * 100 if vol_base != 0 else 0

    # OI 단기(6) vs 중기(30)
    oi_short = np.mean([x["oi"] for x in oi_data[:6]])
    oi_long  = np.mean([x["oi"] for x in oi_data[:30]])
    oi_trend = ((oi_short - oi_long) / oi_long) * 100 if oi_long != 0 else 0

    engulf = detect_engulf(prev, curr)

    dia = supabase.table("diamond_logs").select("*").order("id", desc=True).limit(1).execute().data
    dia_sig = dia[0]["color"] if dia else None

    print(f"📊 vol_chg={vol_chg:.2f}%, oi_trend={oi_trend:.2f}%, engulf={engulf}, dia={dia_sig}")

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
        if engulf == "Bullish Engulfing":
            msg = f"[⚡ 숏 스퀴즈]\nBull Engulf | Vol + | OI 감소"
        elif engulf == "Bearish Engulfing":
            msg = f"[⚡ 롱 스퀴즈]\nBear Engulf | Vol + | OI 감소"
        else:
            msg = "[⚠ 청산발생 + 무방향]"

    # 3) OI Flat → 매집/분배
    elif vol_chg > 0 and abs(oi_trend) <= OI_FLAT_TH:
        if engulf == "Bullish Engulfing":
            msg = f"[🟢 매집 흐름]\nVol + / OI Flat({oi_trend:.2f}%) → 상방 준비"
        elif engulf == "Bearish Engulfing":
            msg = f"[🔻 분배 흐름]\nVol + / OI Flat({oi_trend:.2f}%) → 하방 경계"
        else:
            msg = f"[⚠ 변동성↑ but 중립]\nOI Flat={oi_trend:.2f}%"

    else:
        msg = "[💤 방향 미약 — 관망]"

    print("📲 텔레그램 전송:", msg)
    send_telegram(msg)


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4H 주기 실행 (OI + 분석 한 세트)
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def run_4h_cycle():
    print("⏱ 4H 주기 작업 시작")
    send_telegram("⏱ 4H UPDATE: 분석 시작")

    fetch_OI("ETH")        # OI 저장 시도
    analyze_signal()       # 현재 candle + OI 가지고 분석

    send_telegram("✅ 4H UPDATE: 분석 완료")


schedule.every(4).hours.do(run_4h_cycle)


def scheduler():
    print("🟢 Scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(1)


@app.on_event("startup")
def launch_scheduler():
    print("🟢 Scheduler activated")
    Thread(target=scheduler, daemon=True).start()


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Webhook → OHLC 저장
#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/webhook")
async def webhook_receiver(req: Request):
    data = await req.json()
    print("🔥 RECEIVED:", data)

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

    trim_logs("candle_logs", keep=50)
    trim_logs("diamond_logs", keep=50)
    trim_logs("oi_logs",     keep=50)

    return {"status": "ok"}
