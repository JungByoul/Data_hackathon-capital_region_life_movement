# bt_admi_outadm.py
# polars >= 0.20

import polars as pl

# === 경로 ===
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
DICT_CSV  = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"  # ['ADMI_CD','ADMI_NM']
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/BT/BT_ADM_outadm.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/BT/BT_ADM_outadm.csv"

# === 이동시간 6버킷 ===
def move_time_bucket(col: pl.Series | pl.Expr) -> pl.Expr:
    return (
        pl.when(col <= 20).then(20)
        .when(col <= 40).then(40)
        .when(col <= 60).then(60)
        .when(col <= 90).then(90)
        .when(col <= 120).then(120)
        .otherwise(240)
        .cast(pl.Int16)
        .alias("MOVE_TIME")
    )

# 1) 원본 스캔 (컬럼형 유지)
lf = (
    pl.scan_parquet(IN_PARQ)
      .with_columns(
          pl.col("ETL_YMD").cast(pl.Utf8),
          pl.col("O_ADMI_CD").cast(pl.Utf8),
          pl.col("D_ADMI_CD").cast(pl.Utf8),
          pl.col("MOVE_PURPOSE").cast(pl.Int32),
          pl.col("MOVE_TIME").cast(pl.Int32),
          pl.col("TOTAL_CNT").cast(pl.Float64),
      )
)

# 2) 파생/필터
lf = (
    lf.with_columns(
          pl.col("ETL_YMD").str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"),
      )
      .with_columns(
          pl.col("ETL_DATE").dt.strftime("%Y%m").alias("ETL_YM"),
          pl.col("O_ADMI_CD").str.zfill(8).str.slice(0, 5).alias("_O5"),
          pl.col("D_ADMI_CD").str.zfill(8).str.slice(0, 5).alias("_D5"),
      )
      # 목적=출근만
      .filter(pl.col("MOVE_PURPOSE") == 1)
      # 동일 시군구 제거(앞 5자리 동일 행 제거)
      .filter(pl.col("_O5") != pl.col("_D5"))
      # 이동시간 6버킷으로 변환
      .with_columns(move_time_bucket(pl.col("MOVE_TIME")))
)

# 3) 월별 집계 (PK: O_ADMI_CD 기준, D_ADMI_CD·MOVE_TIME 포함)
agg_lf = (
    lf.group_by(["ETL_YM", "O_ADMI_CD", "D_ADMI_CD", "MOVE_TIME"])
      .agg(pl.col("TOTAL_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
      .sort(["ETL_YM", "O_ADMI_CD", "D_ADMI_CD", "MOVE_TIME"])
)

# 4) ADMI_NM 조인 (교체)

# 4-0) 사전: 필요한 컬럼만, 타입 통일
dict_base = (
    pl.scan_csv(DICT_CSV)
      .select(
          pl.col("ADMI_CD").cast(pl.Utf8),
          pl.col("ADMI_NM").cast(pl.Utf8),
      )
      .unique(subset=["ADMI_CD"])
)

# 출발/도착용으로 키 이름 변경
dict_o = dict_base.rename({"ADMI_CD": "O_ADMI_CD", "ADMI_NM": "O_ADMI_NM"})
dict_d = dict_base.rename({"ADMI_CD": "D_ADMI_CD", "ADMI_NM": "D_ADMI_NM"})

# 4-1) 출발명 조인
with_origin = agg_lf.join(dict_o, on="O_ADMI_CD", how="left")

# 4-2) 도착명 조인
with_dest = with_origin.join(dict_d, on="D_ADMI_CD", how="left")

# 4-3) 최종 컬럼 정리(+정렬)
with_names = (
    with_dest
      .select([
          "ETL_YM",
          "O_ADMI_CD", "O_ADMI_NM",
          "D_ADMI_CD", "D_ADMI_NM",
          "MOVE_TIME",
          "TOTAL_CNT",
      ])
      .sort(["ETL_YM", "O_ADMI_CD", "D_ADMI_CD", "MOVE_TIME"])
)



# 5) 저장 & 검증
with_names.sink_parquet(OUT_PARQ)
out_df = pl.read_parquet(OUT_PARQ)
out_df.write_csv(OUT_CSV)

print(out_df.head(40))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
