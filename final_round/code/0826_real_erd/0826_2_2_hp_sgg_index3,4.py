# HP SGG hotplace(3,4) 테이블 생성 스크립트 (Polars)
import polars as pl

# === 설정 ===
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_SGG_index34.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_SGG_index34.csv"

YM_FMT   = "%Y%m"
SGG_COL  = "D_SGG_NM"  # 도착 시군구

# 연령대 코드 정의
Y00 = ["00","40","45","50","55","60","65","70","75","80","85"]  # 0~9세 + 40세이상
Y10 = ["10","15"]  # 10~19
Y20 = ["20","25"]  # 20~29
Y30 = ["30","35"]  # 30~39

def cols(prefix, codes):
    return [f"{prefix}_{c}_CNT" for c in codes]

# 남/녀 컬럼 목록
M_00, F_00 = cols("MALE", Y00), cols("FEML", Y00)
M_10, F_10 = cols("MALE", Y10), cols("FEML", Y10)
M_20, F_20 = cols("MALE", Y20), cols("FEML", Y20)
M_30, F_30 = cols("MALE", Y30), cols("FEML", Y30)

lf = pl.scan_parquet(IN_PARQ)

base = (
    lf
    .with_columns([
        # 1) 날짜 파싱
        pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"),
    ])
    .with_columns([
        # 2) 년월, 평/주 플래그, SGG 명
        pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("ETL_YM"),
        pl.col("ETL_DATE").dt.weekday().is_in([5, 6]).cast(pl.Int8).alias("WEEKEND_FLAG"),  # 0=평일,1=주말
        pl.col(SGG_COL).alias("SGG_NM"),
    ])
    # 3) 성별×연령대별 카운트 생성
    .with_columns([
        pl.sum_horizontal(*(pl.col(c) for c in M_00)).alias("M_00"),
        pl.sum_horizontal(*(pl.col(c) for c in M_10)).alias("M_10"),
        pl.sum_horizontal(*(pl.col(c) for c in M_20)).alias("M_20"),
        pl.sum_horizontal(*(pl.col(c) for c in M_30)).alias("M_30"),
        pl.sum_horizontal(*(pl.col(c) for c in F_00)).alias("F_00"),
        pl.sum_horizontal(*(pl.col(c) for c in F_10)).alias("F_10"),
        pl.sum_horizontal(*(pl.col(c) for c in F_20)).alias("F_20"),
        pl.sum_horizontal(*(pl.col(c) for c in F_30)).alias("F_30"),
    ])
)

# 남성 4버킷
gm00 = (base.group_by(["ETL_YM","SGG_NM","WEEKEND_FLAG"])
             .agg(pl.col("M_00").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
             .with_columns([pl.lit("0:남성").alias("성별"),
                            pl.lit("00:0~10세&40세이상").alias("연령대")]))
gm10 = (base.group_by(["ETL_YM","SGG_NM","WEEKEND_FLAG"])
             .agg(pl.col("M_10").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
             .with_columns([pl.lit("0:남성").alias("성별"),
                            pl.lit("10:10대").alias("연령대")]))
gm20 = (base.group_by(["ETL_YM","SGG_NM","WEEKEND_FLAG"])
             .agg(pl.col("M_20").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
             .with_columns([pl.lit("0:남성").alias("성별"),
                            pl.lit("20:20대").alias("연령대")]))
gm30 = (base.group_by(["ETL_YM","SGG_NM","WEEKEND_FLAG"])
             .agg(pl.col("M_30").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
             .with_columns([pl.lit("0:남성").alias("성별"),
                            pl.lit("30:30대").alias("연령대")]))

# 여성 4버킷
gf00 = (base.group_by(["ETL_YM","SGG_NM","WEEKEND_FLAG"])
             .agg(pl.col("F_00").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
             .with_columns([pl.lit("1:여성").alias("성별"),
                            pl.lit("00:0~10세&40세이상").alias("연령대")]))
gf10 = (base.group_by(["ETL_YM","SGG_NM","WEEKEND_FLAG"])
             .agg(pl.col("F_10").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
             .with_columns([pl.lit("1:여성").alias("성별"),
                            pl.lit("10:10대").alias("연령대")]))
gf20 = (base.group_by(["ETL_YM","SGG_NM","WEEKEND_FLAG"])
             .agg(pl.col("F_20").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
             .with_columns([pl.lit("1:여성").alias("성별"),
                            pl.lit("20:20대").alias("연령대")]))
gf30 = (base.group_by(["ETL_YM","SGG_NM","WEEKEND_FLAG"])
             .agg(pl.col("F_30").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT"))
             .with_columns([pl.lit("1:여성").alias("성별"),
                            pl.lit("30:30대").alias("연령대")]))

final_lf = (
    pl.concat([gm00, gm10, gm20, gm30, gf00, gf10, gf20, gf30])
      .sort(["ETL_YM","SGG_NM","WEEKEND_FLAG","성별","연령대"])
)

# 저장
final_lf.sink_parquet(OUT_PARQ)
final_df = pl.read_parquet(OUT_PARQ)
final_df.write_csv(OUT_CSV)

# 검증
print(final_df.head(50))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
