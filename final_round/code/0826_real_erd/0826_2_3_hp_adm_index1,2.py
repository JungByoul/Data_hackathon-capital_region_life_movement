# hp_admi_index12.py  (행정동 기준, 베드타운 형식 + 연령대/목적 유지)
import polars as pl

# ===== 경로 =====
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_ADM_index12.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_ADM_index12.csv"
DICT_CSV = '/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv'
# ===== 설정 =====
ADMI_COL  = "D_ADMI_CD"   # PK 기준: 도착 행정동
YM_FMT    = "%y%m"        # 필요시 "%Y%m"으로 교체
YM_FROM   = None
YM_TO     = None
WEEKDAY_ONLY = False

# 연령대 매핑(20~39 vs 그외)
AGE_ALL  = ["00","10","15","20","25","30","35","40","45","50","55","60","65","70","75","80","85"]
AGE_2039 = ["20","25","30","35"]
AGE_ELSE = [a for a in AGE_ALL if a not in AGE_2039]

def age_cols(codes):
    return [f"MALE_{c}_CNT" for c in codes] + [f"FEML_{c}_CNT" for c in codes]

AGE2039_COLS = age_cols(AGE_2039)
AGEELSE_COLS = age_cols(AGE_ELSE)

lf = pl.scan_parquet(IN_PARQ)

# 1) 날짜 파생
lf = lf.with_columns(
    pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE")
)


# 3) 파생 컬럼: 년월, 평/주, 목적 재매핑(4/5 유지, 나머지→0), 키/타입 정리
lf = lf.with_columns([
    pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("ETL_YM"),
    # pl.col("ETL_DATE").dt.weekday().is_in([5, 6]).cast(pl.Int8).alias("WEEKEND_FLAG"),  # 0=평일, 1=주말
    pl.when(pl.col("MOVE_PURPOSE").cast(pl.Int16).is_in([4, 5]))
      .then(pl.col("MOVE_PURPOSE").cast(pl.Int16))
      .otherwise(pl.lit(0))
      .alias("PURPOSE_BIN"),  # 최종 {0,4,5}
    pl.col(ADMI_COL).cast(pl.Utf8).alias("ADMI_CD"),


])


# 5) 연령대별 카운트(행 기준 합)
lf = lf.with_columns([
    pl.sum_horizontal(*(pl.col(c) for c in AGE2039_COLS)).alias("AGE_2039_CNT"),
    pl.sum_horizontal(*(pl.col(c) for c in AGEELSE_COLS)).alias("AGE_ELSE_CNT"),
])

# 6) 집계 (20~39)
g1 = (
    lf
    .group_by(["ETL_YM", "ADMI_CD",  "PURPOSE_BIN"])
    .agg(pl.col("AGE_2039_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
    .with_columns(pl.lit("0:20~39세").alias("연령대"))
    .rename({"PURPOSE_BIN": "MOVE_PURPOSE"})  # 출력 컬럼명은 기존과 동일하게
)

# 7) 집계 (그외)
g2 = (
    lf
    .group_by(["ETL_YM", "ADMI_CD", "PURPOSE_BIN"])
    .agg(pl.col("AGE_ELSE_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
    .with_columns(pl.lit("1: 그외 모든 연령대").alias("연령대"))
    .rename({"PURPOSE_BIN": "MOVE_PURPOSE"})
)

# ===== 사전 경로 =====
DICT_CSV = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"  # ['ADMI_CD','ADMI_NM'] 가정

# 8) 결합/정렬
final_lf = pl.concat([g1, g2]).sort(["ETL_YM", "ADMI_CD", "연령대", "MOVE_PURPOSE"])

# 8-1) ADMI 사전 Lazy 로드 후 left join
dict_lf = (
    pl.scan_csv(DICT_CSV)
      .select(
          pl.col("ADMI_CD").cast(pl.Utf8),
          pl.col("ADMI_NM").cast(pl.Utf8)
      )
      .unique(subset=["ADMI_CD"])
)

final_lf = (
    final_lf
      .join(dict_lf, on="ADMI_CD", how="left")
      # 컬럼 순서: ADMI_CD 바로 뒤에 ADMI_NM 배치
      .select(["ETL_YM", "ADMI_CD", "ADMI_NM", "연령대", "MOVE_PURPOSE", "TOTAL_CNT"])
)

# 저장
final_lf.sink_parquet(OUT_PARQ)
final_df = pl.read_parquet(OUT_PARQ)
final_df.write_csv(OUT_CSV)

# 검증
print(final_df.head(40))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
