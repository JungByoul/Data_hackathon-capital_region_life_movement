"""
0812_2admi_preprocess.py
-----------------------
기능:
  - 월별 행정동 코드(ADMI_YYYYMM.csv) 파일들을 읽어,
    수도권(서울·경기·인천)만 필터링 후,
    각 행정동 코드별 최신(가장 최근 연월) 데이터만 추출하여
    매핑용 Parquet 파일(LATEST_ADMI_CODES.parquet)로 저장한다.

주요 처리 단계:
  1. ADMI_YYYYMM.csv 파일 수집 및 수도권 필터링
  2. 파일명에서 연월(YYYYMM) 추출하여 BASE_YM 컬럼으로 추가
  3. 같은 ADMI_CD에 대해 가장 최근 연월의 데이터만 남김
  4. 매핑에 필요한 컬럼만 보존하여 Parquet 파일로 저장
"""

import os, glob
import pandas as pd

# 1) 경로/설정
DATA_DIR = "/home1/bismarck/transit_seoul/dataset/ADMI_RE"   # ADMI CSV들이 있는 폴더
KEEP_SIDO = ["서울특별시", "경기도", "인천광역시"]

# 2) 파일 수집 (2024-06 ~ 2025-05 범위면 ADMI_*.csv 전체로 충분)
files = sorted(glob.glob(os.path.join(DATA_DIR, "ADMI_*.csv")))
if not files:
    raise FileNotFoundError("No ADMI_*.csv files found")

dfs = []
for fp in files:
    ym = os.path.basename(fp).split("_")[1].split(".")[0]  # YYYYMM from filename
    # 🔸 인코딩은 환경에 맞춰 선택: cp949 또는 utf-8-sig
    df = pd.read_csv(fp, dtype=str, encoding="utf-8-sig")
    df = df[df["SIDO_NM"].isin(KEEP_SIDO)].copy()
    df["BASE_YM"] = ym  # 파일명 기준 연월 보강(있으면 덮어씀)
    # 공백 처리(선택)
    df["FULL_NM"] = df["FULL_NM"].str.replace(" ", "_", regex=False)
    dfs.append(df)

all_codes = pd.concat(dfs, ignore_index=True)

# 3) 최신(가장 큰 BASE_YM)만 남기기
all_codes["BASE_YM"] = all_codes["BASE_YM"].astype(int)
all_codes = all_codes.sort_values(["ADMI_CD", "BASE_YM"])
latest = all_codes.drop_duplicates("ADMI_CD", keep="last").reset_index(drop=True)

# 4) 필요한 컬럼만 보존 + 저장 (조인/매핑에 쓸 테이블)
keep_cols = ["ADMI_CD", "SIDO_NM", "SGG_NM", "ADMI_NM", "FULL_NM", "BASE_YM"]
latest = latest[keep_cols]

# 편의상 Parquet로 저장(폴라스 조인/매핑에 유리)
out_map = os.path.join(DATA_DIR, "LATEST_ADMI_CODES.parquet")
latest.to_parquet(out_map, index=False)
print("✅ Saved:", out_map)

