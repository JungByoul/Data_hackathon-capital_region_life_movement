# hp_admi_cohort.py  (행정동 기준: 성별 × 연령대{00,10,20,30,40,50,60})
import polars as pl

# ===== 경로 =====
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_ADM_cohort.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_ADM_cohort.csv"
DICT_CSV  = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"  # ['ADMI_CD','ADMI_NM']

# ===== 설정 =====
ADMI_COL  = "D_ADMI_CD"   # PK 기준: 도착 행정동
YM_FMT    = "%y%m"        # 필요 시 "%Y%m"

# 연령대 원시 코드(원데이터 기준)
Y00 = ["00"]                  # 0~10세
Y10 = ["10","15"]             # 10대
Y20 = ["20","25"]             # 20대
Y30 = ["30","35"]             # 30대
Y40 = ["40","45"]             # 40대
Y50 = ["50","55"]             # 50대
Y60 = ["60","65","70","75","80","85"]  # 60대 이상

def cols(prefix, codes):
    return [f"{prefix}_{c}_CNT" for c in codes]

# 남/녀 컬럼 묶음
M_00, F_00 = cols("MALE", Y00), cols("FEML", Y00)
M_10, F_10 = cols("MALE", Y10), cols("FEML", Y10)
M_20, F_20 = cols("MALE", Y20), cols("FEML", Y20)
M_30, F_30 = cols("MALE", Y30), cols("FEML", Y30)
M_40, F_40 = cols("MALE", Y40), cols("FEML", Y40)
M_50, F_50 = cols("MALE", Y50), cols("FEML", Y50)
M_60, F_60 = cols("MALE", Y60), cols("FEML", Y60)

lf = pl.scan_parquet(IN_PARQ)

# 1) 날짜 파싱 및 키 정리
lf = lf.with_columns(
    pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE")
).with_columns([
    pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("ETL_YM"),
    pl.col(ADMI_COL).cast(pl.Utf8).alias("ADMI_CD"),
])

# 2) 성별×연령대 버킷 합산 컬럼 생성
lf = lf.with_columns([
    pl.sum_horizontal(*(pl.col(c) for c in M_00)).alias("M_00"),
    pl.sum_horizontal(*(pl.col(c) for c in M_10)).alias("M_10"),
    pl.sum_horizontal(*(pl.col(c) for c in M_20)).alias("M_20"),
    pl.sum_horizontal(*(pl.col(c) for c in M_30)).alias("M_30"),
    pl.sum_horizontal(*(pl.col(c) for c in M_40)).alias("M_40"),
    pl.sum_horizontal(*(pl.col(c) for c in M_50)).alias("M_50"),
    pl.sum_horizontal(*(pl.col(c) for c in M_60)).alias("M_60"),

    pl.sum_horizontal(*(pl.col(c) for c in F_00)).alias("F_00"),
    pl.sum_horizontal(*(pl.col(c) for c in F_10)).alias("F_10"),
    pl.sum_horizontal(*(pl.col(c) for c in F_20)).alias("F_20"),
    pl.sum_horizontal(*(pl.col(c) for c in F_30)).alias("F_30"),
    pl.sum_horizontal(*(pl.col(c) for c in F_40)).alias("F_40"),
    pl.sum_horizontal(*(pl.col(c) for c in F_50)).alias("F_50"),
    pl.sum_horizontal(*(pl.col(c) for c in F_60)).alias("F_60"),
])

# 3) 년월×행정동 기준 집계
keys = ["ETL_YM","ADMI_CD"]
g = (
    lf.group_by(keys).agg([
        pl.col("M_00").sum().alias("M_00"),
        pl.col("M_10").sum().alias("M_10"),
        pl.col("M_20").sum().alias("M_20"),
        pl.col("M_30").sum().alias("M_30"),
        pl.col("M_40").sum().alias("M_40"),
        pl.col("M_50").sum().alias("M_50"),
        pl.col("M_60").sum().alias("M_60"),
        pl.col("F_00").sum().alias("F_00"),
        pl.col("F_10").sum().alias("F_10"),
        pl.col("F_20").sum().alias("F_20"),
        pl.col("F_30").sum().alias("F_30"),
        pl.col("F_40").sum().alias("F_40"),
        pl.col("F_50").sum().alias("F_50"),
        pl.col("F_60").sum().alias("F_60"),
    ])
)

# 4) 성별×연령대 14버킷 → 행 확장
def melt_gender_age(df: pl.LazyFrame, sex_label: str, prefix: str) -> pl.LazyFrame:
    # prefix: "M" 또는 "F"
    pairs = [
        (f"{prefix}_00","00:0~10세"),
        (f"{prefix}_10","10:10대"),
        (f"{prefix}_20","20:20대"),
        (f"{prefix}_30","30:30대"),
        (f"{prefix}_40","40:40대"),
        (f"{prefix}_50","50:50대"),
        (f"{prefix}_60","60:60대 이상"),
    ]
    pieces = []
    for col_name, age_label in pairs:
        pieces.append(
            df.select(keys + [pl.col(col_name).round(0).cast(pl.Int64).alias("TOTAL_CNT")])
              .with_columns([
                  pl.lit(sex_label).alias("성별"),
                  pl.lit(age_label).alias("연령대"),
              ])
        )
    return pl.concat(pieces)

male_lf   = melt_gender_age(g, "0:남성", "M")
female_lf = melt_gender_age(g, "1:여성", "F")

final_lf = pl.concat([male_lf, female_lf])

# 5) ADMI_NM 조인 + 정렬 + 출력 컬럼 선택
dict_lf = (
    pl.scan_csv(DICT_CSV)
      .select(pl.col("ADMI_CD").cast(pl.Utf8), pl.col("ADMI_NM").cast(pl.Utf8))
      .unique(subset=["ADMI_CD"])
)

final_lf = (
    final_lf.join(dict_lf, on="ADMI_CD", how="left")
            # 슬라이드 표기 순서(이미지 기준): 년월, ADMI_NM, ADMI_CD, 성별, 연령대, TOTAL_CNT
            .select(["ETL_YM","ADMI_NM","ADMI_CD","성별","연령대","TOTAL_CNT"])
            .sort(["ETL_YM","ADMI_NM","ADMI_CD","성별","연령대"])
)

# 6) 저장 및 검증
final_lf.sink_parquet(OUT_PARQ)
final_df = pl.read_parquet(OUT_PARQ)
final_df.write_csv(OUT_CSV)
print(final_df.head(40))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
