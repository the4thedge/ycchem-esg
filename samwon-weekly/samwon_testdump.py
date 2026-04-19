"""
samwon_testdump.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
삼원폴리텍 ThingsBoard 7일 데이터 1회성 덤프 (MCP 우회, REST 직접)
목적: 분석 파이프라인 프로토타입 + 코웍 #1 구현 전 실물 데이터 확보
작성: 2026-04-19

실행:
  1. pip install requests pandas pyarrow python-dateutil
  2. TB_USER / TB_PASS 환경변수 설정 (또는 아래 상수 직접 수정)
  3. python samwon_testdump.py

산출:
  - samwon_1w_testdump_{YYYYMMDD_HHMM}.parquet
  - samwon_1w_testdump_{YYYYMMDD_HHMM}_inventory.json
  - samwon_1w_testdump_{YYYYMMDD_HHMM}_report.txt
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import sys
import json
import time
import math
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

import requests
import pandas as pd

# ═══════════════════════════════════════════════════════════
#  설정
# ═══════════════════════════════════════════════════════════

TB_HOST = "http://141.164.58.193:2080"
TB_USER = os.getenv("TB_USER", "sysadmin@samwonpt.com")
TB_PASS = os.getenv("TB_PASS", "")  # 환경변수 권장

KST = ZoneInfo("Asia/Seoul")
OUT_DIR = Path(".")  # 현재 디렉토리에 출력

# 집계 설정
INTERVAL_MS = 300_000   # 5분
AGG = "AVG"
REQUEST_TIMEOUT = 60    # HTTP 타임아웃(초). MCP보다 훨씬 관대

# 청크 전략: 하루 단위로 잘라서 받되, 키는 한 번에 다 넣음
# → 7일 × 23키 한방 요청은 서버 부담, 1일씩 끊으면 안정적
CHUNK_DAYS = 1

# 재시도
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 5, 10]  # 초

# ═══════════════════════════════════════════════════════════
#  화이트리스트
# ═══════════════════════════════════════════════════════════

PAC_KEYS = [
    # 제어 품질
    "RETURN_AIR_TEMPERATURE", "RETURN_AIR_HUMIDITY",
    "TEMPERATURE_SETPOINT", "HUMIDITY_SETPOINT",
    # 운전
    "OPERATION_STATE", "OPERATION_MODE", "FAN_OUTPUT_STATE",
    # 압축기
    "COMPRESSOR_1_OUTPUT_STATE", "COMPRESSOR_2_OUTPUT_STATE", "COMPRESSOR_3_OUTPUT_STATE",
    # 히터
    "HEATING_1_OUTPUT_STATE", "HEATING_2_OUTPUT_STATE", "HEATING_3_OUTPUT_STATE",
    # 가습기
    "HUMIDIFY_1_OUTPUT_STATE", "HUMIDIFY_2_OUTPUT_STATE", "HUMIDIFY_3_OUTPUT_STATE",
    # 부하 제어
    "AO_1_OUTPUT_PERCENT", "AO_2_OUTPUT_PERCENT",
    # 에너지
    "HUMIDIFY_CURRENT", "RATED_CURRENT",
    # 알람
    "TOTAL_ALARM_STATE", "HIGH_TEMPERATURE_ALARM_STATE",
    # 누적
    "RUN_TIME_HOURS",
]  # 23키

EHP_KEYS = [
    "AC_OPERATION_STATE", "AC_OPERATION_MODE",
    "INDOOR_TEMPERATURE", "TEMPERATURE_SETPOINT",
    "INDOOR_FAN_SPEED", "DISCHARGE_TEMPERATURE",
    "COMMUNICATION_STATE", "INDOOR_ERROR_CODE",
    "COOLING_DISCHARGE_SETPOINT", "HEATING_DISCHARGE_SETPOINT",
]  # 10키

WEATHER_KEYS = [
    "outdoor_temp", "outdoor_humidity", "outdoor_rainfall",
    "outdoor_wind_speed", "outdoor_wind_dir", "outdoor_precip_type",
]  # 6키

EXCLUDED_DEVICES = {"AHU GW", "AHUTEST", "PAC_TEST"}

# 매직넘버 → NaN
MAGIC_NUMBERS = {
    "DISCHARGE_TEMPERATURE": {-50},
    "INDOOR_ERROR_CODE": {255},
    "OUTDOOR_ERROR_CODE": {255},
    "OUTDOOR_DEFROST_STATE": {255},
    "RELAY_ERROR_STATE": {255},
}

# ═══════════════════════════════════════════════════════════
#  로깅
# ═══════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("samwon")

# ═══════════════════════════════════════════════════════════
#  ThingsBoard 클라이언트
# ═══════════════════════════════════════════════════════════

class TBClient:
    def __init__(self, host, user, password):
        self.host = host.rstrip("/")
        self.user = user
        self.password = password
        self.session = requests.Session()
        self.token = None
        self.refresh_token = None
        self.token_at = 0  # 발급 시각

    def login(self):
        log.info(f"🔑 로그인 시도: {self.user} @ {self.host}")
        r = self.session.post(
            f"{self.host}/api/auth/login",
            json={"username": self.user, "password": self.password},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        self.token = data["token"]
        self.refresh_token = data["refreshToken"]
        self.token_at = time.time()
        log.info(f"✅ 로그인 성공 (토큰 길이: {len(self.token)})")

    def _refresh(self):
        log.info("🔄 토큰 refresh")
        r = self.session.post(
            f"{self.host}/api/auth/token",
            json={"refreshToken": self.refresh_token},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            log.warning("refresh 실패 — 재로그인")
            self.login()
            return
        data = r.json()
        self.token = data["token"]
        self.refresh_token = data["refreshToken"]
        self.token_at = time.time()

    def _headers(self):
        # 토큰 발급 후 2시간 지났으면 refresh
        if time.time() - self.token_at > 2 * 3600:
            self._refresh()
        return {"X-Authorization": f"Bearer {self.token}"}

    def get(self, path, params=None):
        url = f"{self.host}{path}"
        for attempt, backoff in enumerate([0] + RETRY_BACKOFF):
            if backoff:
                time.sleep(backoff)
            try:
                r = self.session.get(
                    url,
                    headers=self._headers(),
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )
                if r.status_code == 401:
                    log.warning("401 — 토큰 refresh 후 재시도")
                    self._refresh()
                    continue
                r.raise_for_status()
                return r.json()
            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES:
                    raise
                log.warning(f"요청 실패 ({attempt+1}/{MAX_RETRIES+1}): {e}")
        return None

# ═══════════════════════════════════════════════════════════
#  유틸
# ═══════════════════════════════════════════════════════════

def now_kst():
    return datetime.now(KST)

def ts_ms(dt):
    return int(dt.timestamp() * 1000)

def ms_to_kst(ms):
    return datetime.fromtimestamp(ms / 1000, tz=KST)

def clean_value(key, value):
    """매직넘버를 NaN으로"""
    if key in MAGIC_NUMBERS:
        try:
            if float(value) in MAGIC_NUMBERS[key]:
                return math.nan
        except (TypeError, ValueError):
            pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan

def day_chunks(start_dt, end_dt, days=CHUNK_DAYS):
    """시작~끝을 N일씩 잘라서 (chunk_start, chunk_end) 제너레이터"""
    cur = start_dt
    while cur < end_dt:
        nxt = min(cur + timedelta(days=days), end_dt)
        yield cur, nxt
        cur = nxt

# ═══════════════════════════════════════════════════════════
#  수집
# ═══════════════════════════════════════════════════════════

def list_all_devices(client):
    """디바이스 전체 목록"""
    log.info("📦 디바이스 목록 조회")
    data = client.get("/api/tenant/devices", {"pageSize": 200, "page": 0})
    devs = []
    for d in data["data"]:
        created_ms = d.get("createdTime", 0)
        devs.append({
            "id": d["id"]["id"],
            "name": d["name"],
            "type": d["type"],
            "created": ms_to_kst(created_ms).isoformat() if created_ms else None,
        })
    log.info(f"   전체 {len(devs)}대 발견")
    return devs

def get_whitelist(device_type):
    if device_type == "PAC":
        return PAC_KEYS
    if device_type == "EHP":
        return EHP_KEYS
    if device_type == "Weather":
        return WEATHER_KEYS
    return None

def fetch_device_chunk(client, device_id, keys, start_dt, end_dt):
    """하루치 시계열 수집. {key: [{ts, value}, ...]} 반환"""
    try:
        return client.get(
            f"/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries",
            {
                "keys": ",".join(keys),
                "startTs": ts_ms(start_dt),
                "endTs": ts_ms(end_dt),
                "interval": INTERVAL_MS,
                "agg": AGG,
                "limit": 50000,
            },
        )
    except Exception as e:
        log.error(f"  ✗ {device_id} {start_dt:%m-%d}: {e}")
        return {}

def fetch_device_week(client, device, start_dt, end_dt):
    """디바이스 1개 1주치 (1일 청크 × 7)"""
    keys = get_whitelist(device["type"])
    if not keys:
        return [], "skipped_type"

    rows = []
    failed_days = 0
    for ch_start, ch_end in day_chunks(start_dt, end_dt):
        data = fetch_device_chunk(client, device["id"], keys, ch_start, ch_end)
        if not data:
            failed_days += 1
            continue
        for key, points in data.items():
            for p in points:
                rows.append({
                    "ts_ms": p["ts"],
                    "device_id": device["id"],
                    "device_name": device["name"],
                    "device_type": device["type"],
                    "key": key,
                    "value": clean_value(key, p["value"]),
                })

    if not rows:
        return rows, "no_data"
    if failed_days > 0:
        return rows, f"partial_{failed_days}days_failed"
    return rows, "ok"

# ═══════════════════════════════════════════════════════════
#  메인
# ═══════════════════════════════════════════════════════════

def main():
    if not TB_PASS:
        log.error("TB_PASS 환경변수 또는 상수 설정 필요")
        sys.exit(1)

    start_at = now_kst()
    end_dt = start_at
    start_dt = end_dt - timedelta(days=7)

    tag = start_at.strftime("%Y%m%d_%H%M")
    out_parquet = OUT_DIR / f"samwon_1w_testdump_{tag}.parquet"
    out_inventory = OUT_DIR / f"samwon_1w_testdump_{tag}_inventory.json"
    out_report = OUT_DIR / f"samwon_1w_testdump_{tag}_report.txt"

    log.info("═" * 60)
    log.info(f" 삼원폴리텍 1주치 덤프 시작")
    log.info(f" 기간: {start_dt:%Y-%m-%d %H:%M} ~ {end_dt:%Y-%m-%d %H:%M} KST")
    log.info(f" 집계: {AGG}, interval={INTERVAL_MS//1000}s, 청크={CHUNK_DAYS}일")
    log.info("═" * 60)

    # 1) 로그인 + 디바이스 목록
    client = TBClient(TB_HOST, TB_USER, TB_PASS)
    client.login()
    all_devices = list_all_devices(client)

    # 2) 타겟 필터
    targets = [d for d in all_devices if d["name"] not in EXCLUDED_DEVICES and get_whitelist(d["type"])]
    excluded = [d for d in all_devices if d["name"] in EXCLUDED_DEVICES or not get_whitelist(d["type"])]

    by_type = {}
    for d in targets:
        by_type[d["type"]] = by_type.get(d["type"], 0) + 1
    log.info(f"🎯 수집 대상 {len(targets)}대: {by_type}")
    log.info(f"🚫 제외 {len(excluded)}대: {[d['name'] for d in excluded]}")

    # 3) 수집
    all_rows = []
    inventory_devs = []
    for i, dev in enumerate(targets, 1):
        t0 = time.time()
        rows, status = fetch_device_week(client, dev, start_dt, end_dt)
        elapsed = time.time() - t0
        log.info(f"[{i:2d}/{len(targets)}] {dev['name']:<22s} — {len(rows):>8,}pt  {status:<15s}  {elapsed:5.1f}s")
        all_rows.extend(rows)
        inventory_devs.append({
            **dev,
            "points_collected": len(rows),
            "status": status,
        })
        # 서버 배려 (과도한 연속 요청 방지)
        time.sleep(0.3)

    # 4) DataFrame → parquet
    log.info("")
    log.info(f"💾 parquet 저장 중...")
    df = pd.DataFrame(all_rows)
    if df.empty:
        log.error("⚠️ 수집 0행. 종료.")
        sys.exit(1)

    df["timestamp_kst"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.tz_convert("Asia/Seoul")
    df = df[["timestamp_kst", "ts_ms", "device_id", "device_name", "device_type", "key", "value"]]
    df = df.sort_values(["device_name", "key", "ts_ms"]).reset_index(drop=True)

    df.to_parquet(out_parquet, engine="pyarrow", compression="snappy", index=False)
    file_size_mb = out_parquet.stat().st_size / 1024 / 1024

    # 5) 인벤토리 저장
    inventory = {
        "fetched_at": start_at.isoformat(),
        "server": TB_HOST,
        "period_start": start_dt.isoformat(),
        "period_end": end_dt.isoformat(),
        "interval_ms": INTERVAL_MS,
        "agg": AGG,
        "chunk_days": CHUNK_DAYS,
        "whitelist": {"PAC": len(PAC_KEYS), "EHP": len(EHP_KEYS), "Weather": len(WEATHER_KEYS)},
        "total_devices_found": len(all_devices),
        "devices_targeted": len(targets),
        "devices_excluded": len(excluded),
        "by_type": by_type,
        "devices": inventory_devs,
    }
    with open(out_inventory, "w", encoding="utf-8") as f:
        json.dump(inventory, f, ensure_ascii=False, indent=2, default=str)

    # 6) 통계 리포트
    end_at = now_kst()
    elapsed_total = (end_at - start_at).total_seconds()
    null_rate = df["value"].isna().mean() * 100
    by_type_counts = df.groupby("device_type").size().to_dict()

    no_data = [d["name"] for d in inventory_devs if d["status"] == "no_data"]
    partial = [d["name"] for d in inventory_devs if d["status"].startswith("partial")]

    report = f"""═════════════════════════════════════════════════════════════
 삼원폴리텍 1주치 덤프 완료 리포트
═════════════════════════════════════════════════════════════
 시작:       {start_at:%Y-%m-%d %H:%M:%S} KST
 종료:       {end_at:%Y-%m-%d %H:%M:%S} KST
 소요:       {elapsed_total/60:.1f}분 ({elapsed_total:.0f}초)

 기간:       {start_dt:%Y-%m-%d %H:%M} ~ {end_dt:%Y-%m-%d %H:%M} KST
 해상도:     {AGG} × {INTERVAL_MS//1000}초
 청크:       {CHUNK_DAYS}일 단위

 대상:       {len(targets)}대 (디바이스 타입별: {by_type})
 제외:       {len(excluded)}대 ({[d['name'] for d in excluded]})

 총 레코드:  {len(df):,}행
 결측:       {df["value"].isna().sum():,}개 ({null_rate:.2f}%)
 파일 크기:  {file_size_mb:.2f} MB

 타입별 포인트:
{chr(10).join(f'   {t}: {c:,}pt' for t, c in by_type_counts.items())}

 수집 0건 디바이스 ({len(no_data)}대):
{chr(10).join(f'   - {n}' for n in no_data) if no_data else '   (없음)'}

 부분 실패 디바이스 ({len(partial)}대):
{chr(10).join(f'   - {n}' for n in partial) if partial else '   (없음)'}

 출력:
   {out_parquet.name}
   {out_inventory.name}
   {out_report.name}
═════════════════════════════════════════════════════════════
"""
    print(report)
    with open(out_report, "w", encoding="utf-8") as f:
        f.write(report)

    log.info(f"✅ 완료: {out_parquet}")


if __name__ == "__main__":
    main()
