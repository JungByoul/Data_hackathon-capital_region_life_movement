# BT SGG purpose 테이블 생성 스크립트 (Polars)
import polars as pl

# === 설정 ===
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"         # ETL_YMD, D_SGG_NM, MOVE_PURPOSE, TOTAL_CNT 포함
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd//bt_sgg_purpose.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/bt_sgg_purpose.csv"
SGG_COL   = "D_SGG_NM"                        # PK 기준: 도착 시군구

# 년월 포맷: "%Y%m" → 202501 / "%y%m" → 2501
YM_FMT = "%Y%m"  # 필요시 "%y%m"으로 변경

lf = pl.scan_parquet(IN_PARQ)

out_lf = (
    lf
    .with_columns([
        # 1) 날짜 파싱
        pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"),
    ])
    .with_columns([
        # 2) 년월, 평/주 플래그, 목적 이진화
        pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("ETL_YM"),
        pl.col("ETL_DATE").dt.weekday().is_in([5, 6]).cast(pl.Int8).alias("WEEKEND_FLAG"),  # 0=평일, 1=주말
        pl.when(pl.col("MOVE_PURPOSE") == 3).then(0).otherwise(1).cast(pl.Int8).alias("PURPOSE_BIN"),  # 0=귀가, 1=그외
        pl.col(SGG_COL).alias("SGG_NM"),
        pl.col("TOTAL_CNT").cast(pl.Float64)
    ])
    # 3) 집계
    .group_by(["ETL_YM", "SGG_NM", "WEEKEND_FLAG", "PURPOSE_BIN"])
    .agg(pl.col("TOTAL_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
    .sort(["ETL_YM", "SGG_NM", "WEEKEND_FLAG", "PURPOSE_BIN"])
)

# 저장
out_lf.sink_parquet(OUT_PARQ)
out_df = pl.read_parquet(OUT_PARQ)     # csv 저장은 즉시 materialize 필요
out_df.write_csv(OUT_CSV)

# 간단 검증
print(out_df.head(100))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
