# hp_admi_dow.py  (행정동 기준: 요일별 유입 수)
import polars as pl

# ===== 경로 =====
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_ADM_dow.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_ADM_dow.csv"
DICT_CSV  = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"   # ['ADMI_CD','ADMI_NM']

# ===== 설정 =====
ADMI_COL  = "D_ADMI_CD"   # PK 기준: 도착 행정동
YM_FMT    = "%y%m"        # 필요 시 "%Y%m"

lf = pl.scan_parquet(IN_PARQ)

# 1) 날짜 파싱 & 키 파생
lf = (
    lf.with_columns(
        pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE")
    )
    .with_columns([
        pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("ETL_YM"),
        pl.col(ADMI_COL).cast(pl.Utf8).alias("ADMI_CD"),
        pl.col("ETL_DATE").dt.weekday().cast(pl.Int8).alias("DOW_INT"),  # 0=월, ... , 6=일
    ])
)

# 2) 집계 (년월 × 행정동 × 요일)
g = (
    lf.group_by(["ETL_YM", "ADMI_CD", "DOW_INT"])
      .agg(pl.col("TOTAL_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
)

# 3) 요일 라벨 부여 (예시 표 형식: "1:월" ~ "7:일") -> 헐 이전 주말평일코드 싹 고쳐야함
g = g.with_columns([
    pl.when(pl.col("DOW_INT") == 1).then(pl.lit("1:월"))
     .when(pl.col("DOW_INT") == 2).then(pl.lit("2:화"))
     .when(pl.col("DOW_INT") == 3).then(pl.lit("3:수"))
     .when(pl.col("DOW_INT") == 4).then(pl.lit("4:목"))
     .when(pl.col("DOW_INT") == 5).then(pl.lit("5:금"))
     .when(pl.col("DOW_INT") == 6).then(pl.lit("6:토"))
     .otherwise(pl.lit("7:일"))
     .alias("요일")
])

# 4) ADMI_NM 조인
dict_lf = (
    pl.scan_csv(DICT_CSV)
      .select(pl.col("ADMI_CD").cast(pl.Utf8), pl.col("ADMI_NM").cast(pl.Utf8))
      .unique(subset=["ADMI_CD"])
)

final_lf = (
    g.join(dict_lf, on="ADMI_CD", how="left")
     .sort(["ETL_YM", "ADMI_CD", "DOW_INT"])              # 요일 순서 보장
     .select(["ETL_YM", "ADMI_CD", "ADMI_NM", "요일", "TOTAL_CNT"])  # 출력 컬럼
)

# 5) 저장 & 검증
final_lf.sink_parquet(OUT_PARQ)
final_df = pl.read_parquet(OUT_PARQ)
final_df.write_csv(OUT_CSV)
print(final_df.head(50))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
