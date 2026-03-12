# bt_admi_purpose.py
# 요구: polars >= 0.20

import polars as pl

# ===== 경로 =====
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/bt_admi_purpose.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/bt_admi_purpose.csv"

# ===== 설정 =====
ADMI_COL  = "D_ADMI_CD"   # PK 기준: 도착 행정동
YM_FMT    = "%y%m"        # 스샷과 동일: 2501. 필요시 "%Y%m"으로 교체
YM_FROM   = None          # 예: "202501"  (없으면 전체)
YM_TO     = None          # 예: "202505"
WEEKDAY_ONLY = False      # True면 평일만 필터 (퇴근시간 분석용 사전필터)

lf = pl.scan_parquet(IN_PARQ)

# 1) 날짜 파생
lf = lf.with_columns(
    pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE")
)

# 2) 선택적 년월 범위 필터
if YM_FROM and YM_TO:
    lf = lf.filter(
        (pl.col("ETL_YMD").str.slice(0, 6) >= YM_FROM) &
        (pl.col("ETL_YMD").str.slice(0, 6) <= YM_TO)
    )

# 3) 파생 컬럼: 년월, 평/주, 목적 이진화
lf = lf.with_columns([
    pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("ETL_YM"),
    pl.col("ETL_DATE").dt.weekday().is_in([5, 6]).cast(pl.Int8).alias("WEEKEND_FLAG"),  # 0=평일, 1=주말
    pl.when(pl.col("MOVE_PURPOSE") == 3).then(0).otherwise(1).cast(pl.Int8).alias("MOVE_PURPOSE"),  # 0=귀가, 1=그외
    pl.col(ADMI_COL).cast(pl.Utf8).alias("ADMI_CD"),
    pl.col("TOTAL_CNT").cast(pl.Float64)
])

# 4) (옵션) 평일만
if WEEKDAY_ONLY:
    lf = lf.filter(pl.col("WEEKEND_FLAG") == 0)

# 5) 집계
out_lf = (
    lf
    .group_by(["ETL_YM", "ADMI_CD", "WEEKEND_FLAG", "MOVE_PURPOSE"])
    .agg(pl.col("TOTAL_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
    .sort(["ETL_YM", "ADMI_CD", "WEEKEND_FLAG", "MOVE_PURPOSE"])
)

# 6) 저장
out_lf.sink_parquet(OUT_PARQ)
out_df = pl.read_parquet(OUT_PARQ)   # CSV 저장을 위해 materialize
out_df.write_csv(OUT_CSV)

# 7) 확인
print(out_df.head(50))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
