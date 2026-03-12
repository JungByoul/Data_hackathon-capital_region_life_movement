# bt_admi_gotowork.py
# polars >= 0.20

import polars as pl

# === 경로 ===
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/BT_ADM_gotowork.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/BT_ADM_gotowork.csv"

COMMUTE_ST_CODES = [700, 720, 740, 800, 820, 840, 900, 920, 940]  # 07:00~09:59

# 1) 원본 스캔
lf = (
    pl.scan_parquet(IN_PARQ)
      .with_columns(
          pl.col("ETL_YMD").cast(pl.Utf8),
          pl.col("O_ADMI_CD").cast(pl.Utf8),
          pl.col("D_ADMI_CD").cast(pl.Utf8),
          pl.col("ST_TIME_CD").cast(pl.Int32),
      )
)

# 2) 파생 컬럼 (년월, 도착지, 출근 시간대 여부)
out_lf = (
    lf
    .with_columns(
        pl.col("ETL_YMD").str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"),
    )
    .with_columns(
        pl.col("ETL_DATE").dt.strftime("%Y%m").alias("년월"),
        pl.col("O_ADMI_CD").str.zfill(8).alias("_O8"),
        pl.col("D_ADMI_CD").str.zfill(8).alias("_D8"),
    )
    .with_columns(
        pl.col("O_ADMI_CD").alias("ADMI_CD"),  # PK: 출발 행정동
        pl.when(pl.col("_O8") == pl.col("_D8"))
          .then(pl.lit("동일 행정동"))
          .otherwise(pl.lit("다른 행정동"))
          .alias("도착지"),
        pl.when(pl.col("ST_TIME_CD").is_in(COMMUTE_ST_CODES))
          .then(pl.lit(0))  # 0: 07~09시59분
          .otherwise(pl.lit(1))  # 1: 그외 시간대
          .cast(pl.Int8)
          .alias("출근 시간대 여부"),
        pl.col("TOTAL_CNT").cast(pl.Float64),
    )
    # 3) 집계
    .group_by(["년월", "ADMI_CD", "도착지", "출근 시간대 여부"])
    .agg(pl.col("TOTAL_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
    .sort(["년월", "ADMI_CD", "도착지", "출근 시간대 여부"])
)

# 4) 저장 & 확인
out_lf.sink_parquet(OUT_PARQ)
out_df = pl.read_parquet(OUT_PARQ)
out_df.write_csv(OUT_CSV)

print(out_df.head(40))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
