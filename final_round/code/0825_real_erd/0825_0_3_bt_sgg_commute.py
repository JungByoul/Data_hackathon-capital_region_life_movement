# BT SGG commute 테이블 생성 (출발 시군구 기준)
import polars as pl

# === 설정 ===
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/bt_sgg_commute.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/bt_sgg_commute.csv"
SGG_COL   = "O_SGG_NM"   # PK: 출발 시군구
YM_FMT    = "%Y%m"       # '년월' 형식

# 1) 원본 스캔
lf = (
    pl.scan_parquet(IN_PARQ)
      .with_columns(
          pl.col("ETL_YMD").cast(pl.Utf8),
          pl.col("O_ADMI_CD").cast(pl.Utf8),
          pl.col("D_ADMI_CD").cast(pl.Utf8),
      )
)

# 2) 이동시간 버킷팅
def move_time_bucket(col):
    return (
        pl.when(col <= 20).then(20)
        .when(col <= 40).then(40)
        .when(col <= 60).then(60)
        .when(col <= 90).then(90)
        .when(col <= 120).then(120)
        .otherwise(240)
        .cast(pl.Int16)
    )

# 3) 동일 시군구/다른 시군구 플래그 (zfill + slice(0,5))
def same_sgg_flag():
    return (
        pl.when(
            pl.col("_O_SGG5") == pl.col("_D_SGG5")
        )
        .then(pl.lit("동일 시군구"))
        .otherwise(pl.lit("다른 시군구"))
    )

out_lf = (
    lf
    # 날짜 파싱 및 보조코드
    .with_columns(
        pl.col("ETL_YMD").str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"),
    )
    .with_columns(
        pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("년월"),
        pl.col("O_ADMI_CD").str.zfill(8).str.slice(0, 5).alias("_O_SGG5"),
        pl.col("D_ADMI_CD").str.zfill(8).str.slice(0, 5).alias("_D_SGG5"),
    )
    # 파생 컬럼
    .with_columns(
        pl.col(SGG_COL).alias("SGG_NM"),                     # 출발 시군구
        move_time_bucket(pl.col("MOVE_TIME")).alias("MOVE_TIME"),
        same_sgg_flag().alias("도착지"),
        # 목적 재코딩: 0=통근(원본 1:출근), 1=통학(원본 2:등교), 2=그외
        pl.when(pl.col("MOVE_PURPOSE") == 1).then(0)
         .when(pl.col("MOVE_PURPOSE") == 2).then(1)
         .otherwise(2)
         .cast(pl.Int8)
         .alias("MOVE_PURPOSE"),
        pl.col("TOTAL_CNT").cast(pl.Float64)
    )
    # 집계
    .group_by(["년월", "SGG_NM", "MOVE_TIME", "도착지", "MOVE_PURPOSE"])
    .agg(pl.col("TOTAL_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
    .sort(["년월", "SGG_NM", "MOVE_TIME", "도착지", "MOVE_PURPOSE"])
)

# 저장
out_lf.sink_parquet(OUT_PARQ)
out_df = pl.read_parquet(OUT_PARQ)
out_df.write_csv(OUT_CSV)

# 검증
print(out_df.head(50))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
