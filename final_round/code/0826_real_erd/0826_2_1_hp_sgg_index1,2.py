# HP SGG hotplace(1,2) 테이블 생성 스크립트 (Polars)
import polars as pl

# === 설정 ===
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
# DICT_CSV  = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_SGG_index12.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_SGG_index12.csv"

YM_FMT    = "%Y%m"           # 202501~202505 내장
SGG_COL   = "D_SGG_NM"       # 도착 시군구

# 연령대 매핑: 0=20~39세, 1=그외 모든 연령대
# ※ 실제 컬럼 접두어는 MALE_/FEML_ 로 가정
AGE_ALL = ["00","10","15","20","25","30","35","40","45","50","55","60","65","70","75","80","85"]
AGE_2039 = ["20","25","30","35"]
AGE_ELSE = [a for a in AGE_ALL if a not in AGE_2039]

def age_cols(codes):
    return [f"MALE_{c}_CNT" for c in codes] + [f"FEML_{c}_CNT" for c in codes]

AGE2039_COLS = age_cols(AGE_2039)
AGEELSE_COLS = age_cols(AGE_ELSE)

lf = pl.scan_parquet(IN_PARQ)

out_lf = (
    lf
    # 1) 날짜 파싱 및 기본 파생
    .with_columns([
        pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"),
    ])
    .with_columns([
        pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("ETL_YM"),
        pl.col(SGG_COL).alias("SGG_NM"),
    ])
    # (추가) 목적 매핑: 4/5는 원값 유지, 나머지(1~3,6,7)는 0으로
    .with_columns([
        pl.when(pl.col("MOVE_PURPOSE").is_in([4, 5]))
          .then(pl.col("MOVE_PURPOSE"))
          .otherwise(0)
          .cast(pl.Int8)
          .alias("MOVE_PURPOSE")
    ])
    # 2) 목적 필터: 0(그외), 4(쇼핑), 5(관광)만
    .filter(pl.col("MOVE_PURPOSE").is_in([0, 4, 5]))
    # 3) 연령대별 카운트 생성 (행 기준 합)
    .with_columns([
        pl.sum_horizontal(*(pl.col(c) for c in AGE2039_COLS)).alias("AGE_2039_CNT"),
        pl.sum_horizontal(*(pl.col(c) for c in AGEELSE_COLS)).alias("AGE_ELSE_CNT"),
    ])
)

# 4) 집계(20~39)
g1 = (
    out_lf
    .group_by(["ETL_YM","SGG_NM","MOVE_PURPOSE"])
    .agg(pl.col("AGE_2039_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
    .with_columns(pl.lit("0:20~39세").alias("연령대"))
)

# 5) 집계(그외)
g2 = (
    out_lf
    .group_by(["ETL_YM","SGG_NM","MOVE_PURPOSE"])
    .agg(pl.col("AGE_ELSE_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
    .with_columns(pl.lit("1: 그외 모든 연령대").alias("연령대"))
)

# 6) 결합 및 정렬
final_lf = pl.concat([g1, g2]).sort(["ETL_YM","SGG_NM","연령대","MOVE_PURPOSE"])

# 저장
final_lf.sink_parquet(OUT_PARQ)
final_df = pl.read_parquet(OUT_PARQ)
final_df.write_csv(OUT_CSV)

# 검증
print(final_df.head(30))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
