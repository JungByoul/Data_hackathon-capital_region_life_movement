"""
0812_3totally_combine.py
-------------------------
기능:
  - 원본 통합 데이터(merged_all.parquet)에 최신 행정동 매핑 테이블(LATEST_ADMI_CODES.parquet)을
    O_ADMI_CD / D_ADMI_CD 기준으로 left join하여 행정동 메타정보를 추가한다.
  - 매핑에는 수도권(서울·경기·인천)만 포함된 최신 스냅샷(최신 날짜 기준임)을 사용하며,
    수도권 외 지역은 매핑 컬럼이 null로 남는다.(애초에 raw data에 수도권만 있으니 상관x)
  - Polars LazyFrame을 이용해 대용량 데이터를 메모리 효율적으로 처리하고,
    streaming sink 방식으로 결과를 저장한다.
  - 임시 파일(.tmp)에 먼저 저장 후 원자적 교체로 최종본을 생성.

주요 처리 단계:
  1. ADMI 매핑 테이블 로드 및 출발지(o_map), 도착지(d_map)용으로 컬럼명 변경
  2. 원본 통합본 스캔 후 O_ADMI_CD / D_ADMI_CD 타입 변환
  3. 출발지·도착지 각각 매핑 테이블과 left join
  4. 결과를 Parquet(lz4)로 저장
"""

import os
import polars as pl

FACT = "/home1/bismarck/transit_seoul/dataset/merged_all.parquet"
ADMI = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/LATEST_ADMI_CODES.parquet"
OUT  = "/home1/bismarck/transit_seoul/ago_merged_all_admi.parquet"
TMP  = OUT + ".tmp"

# 1) ADMI 매핑 (작으니 eager → lazy로 전환)
admi = (
    pl.read_parquet(ADMI)
      .select(["ADMI_CD","SIDO_NM","SGG_NM","ADMI_NM","FULL_NM"])
      .with_columns(pl.col("ADMI_CD").cast(pl.Utf8))
)

o_map = (
    admi.rename({
        "ADMI_CD":"O_ADMI_CD",
        "SIDO_NM":"O_SIDO_NM",
        "SGG_NM":"O_SGG_NM",
        "ADMI_NM":"O_ADMI_NM",
        "FULL_NM":"O_FULL_NM",
    }).lazy()
)

d_map = (
    admi.rename({
        "ADMI_CD":"D_ADMI_CD",
        "SIDO_NM":"D_SIDO_NM",
        "SGG_NM":"D_SGG_NM",
        "ADMI_NM":"D_ADMI_NM",
        "FULL_NM":"D_FULL_NM",
    }).lazy()
)

# 2) 원본 통합본 스캔 → 조인 → 스트리밍 저장
lf = (
    pl.scan_parquet(FACT)
      .with_columns([
          pl.col("O_ADMI_CD").cast(pl.Utf8),
          pl.col("D_ADMI_CD").cast(pl.Utf8),
      ])
      .join(o_map, on="O_ADMI_CD", how="left")
      .join(d_map, on="D_ADMI_CD", how="left")
)

if os.path.exists(TMP):
    os.remove(TMP)

lf.sink_parquet(TMP, compression="lz4", statistics=False)
os.replace(TMP, OUT)
print(f"✅ saved → {OUT}")
