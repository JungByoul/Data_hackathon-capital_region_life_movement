# bt_admi_time_outflow.py
# polars >= 0.20

import polars as pl

# === 경로 ===
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/BT_ADM_time_outflow.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/BT_ADM_time_outflow.csv"

# 시간대 범주(6개) 매핑 집합
BUCKET0 = [0, 1, 2, 3, 4, 5]  # 0000~0559
BUCKET1 = [6, 700, 720, 740, 800, 820, 840, 900, 920, 940, 10]  # 0600~1059
BUCKET2 = [11, 12, 13]  # 1100~1359
BUCKET3 = [14, 15, 16]  # 1400~1659
BUCKET4 = [1700, 1720, 1740, 1800, 1820, 1840, 1900, 1920, 1940, 20]  # 1700~2059
BUCKET5 = [21, 22, 23]  # 2100~2359

def time_bucket():
    cd = pl.col("ST_TIME_CD")
    return (
        pl.when(cd.is_in(BUCKET0)).then(pl.lit(0))
         .when(cd.is_in(BUCKET1)).then(pl.lit(1))
         .when(cd.is_in(BUCKET2)).then(pl.lit(2))
         .when(cd.is_in(BUCKET3)).then(pl.lit(3))
         .when(cd.is_in(BUCKET4)).then(pl.lit(4))
         .when(cd.is_in(BUCKET5)).then(pl.lit(5))
         .otherwise(pl.lit(None))
         .cast(pl.Int8)
         .alias("시간대 범주")
    )

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

# 2) 파생/필터(평일만)
lf = (
    lf.with_columns(pl.col("ETL_YMD").str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"))
      .with_columns(
          pl.col("ETL_DATE").dt.strftime("%Y%m").alias("년월"),
          pl.col("O_ADMI_CD").str.zfill(8).alias("_O8"),
          pl.col("D_ADMI_CD").str.zfill(8).alias("_D8"),
      )
      .filter(pl.col("ETL_DATE").dt.weekday().is_in([0,1,2,3,4]))  # 평일만
      .with_columns(
          pl.col("O_ADMI_CD").alias("ADMI_CD"),
          pl.when(pl.col("_O8") == pl.col("_D8"))
            .then(pl.lit("동일 행정동"))
            .otherwise(pl.lit("다른 행정동"))
            .alias("도착지"),
          time_bucket(),
          pl.col("TOTAL_CNT").cast(pl.Float64),
      )
)

# 3) 집계
out_lf = (
    lf.group_by(["년월", "ADMI_CD", "도착지", "시간대 범주"])
      .agg(pl.col("TOTAL_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
      .sort(["년월", "ADMI_CD", "도착지", "시간대 범주"])
)

# 4) 저장
out_lf.sink_parquet(OUT_PARQ)
out_df = pl.read_parquet(OUT_PARQ)
out_df.write_csv(OUT_CSV)

# 5) 확인
print(out_df.head(40))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
