# hp_admi_keyword.py  (행정동 기준: 평/주 × 성별 × 연령대{10~39/그외} × 목적{쇼핑/그외})
import polars as pl

# ===== 경로 =====
IN_PARQ   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
OUT_PARQ  = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_ADM_keyword.parquet"
OUT_CSV   = "/home1/bismarck/transit_seoul/dataset/total_erd/HP/HP_ADM_keyword.csv"
DICT_CSV  = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"  # ['ADMI_CD','ADMI_NM']

# ===== 설정 =====
ADMI_COL  = "D_ADMI_CD"      # PK 기준: 도착 행정동
YM_FMT    = "%y%m"           # 필요 시 "%Y%m"
# 목적 규칙: 원데이터에서 쇼핑=4 → 출력에서는 0:쇼핑 / 나머지 → 1:그외 목적

# 연령대 코드(10~39 vs 그외)
AGE_ALL  = ["00","10","15","20","25","30","35","40","45","50","55","60","65","70","75","80","85"]
AGE_1039 = ["10","15","20","25","30","35"]
AGE_ELSE = [a for a in AGE_ALL if a not in AGE_1039]

def mcols(codes): return [f"MALE_{c}_CNT" for c in codes]
def fcols(codes): return [f"FEML_{c}_CNT" for c in codes]

lf = pl.scan_parquet(IN_PARQ)

# 1) 날짜 파싱
lf = lf.with_columns(
    pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE")
)

# 2) 파생 컬럼: 년월, 평/주, 목적 이진화(쇼핑=0, 그외=1), 키 정리
lf = lf.with_columns([
    pl.col("ETL_DATE").dt.strftime(YM_FMT).alias("ETL_YM"),
    pl.col("ETL_DATE").dt.weekday().is_in([5, 6]).cast(pl.Int8).alias("WEEKEND_FLAG"),  # 0=평일, 1=주말
    pl.when(pl.col("MOVE_PURPOSE").cast(pl.Int16) == 4).then(pl.lit(0)).otherwise(pl.lit(1)).alias("MOVE_PURPOSE_BIN"),
    pl.col(ADMI_COL).cast(pl.Utf8).alias("ADMI_CD"),
    
    # [추가:0826 18:55] night_time 플래그(집계 키에 포함될 원시 플래그)
    #  - 0: 18:00~23:59 (FNS_TIME_CD >= 1800 또는 {20,21,22,23})
    #  - 1: 그외 시간대
    pl.when(
        (pl.col("FNS_TIME_CD").cast(pl.Int32) >= 1800) |
        (pl.col("FNS_TIME_CD").cast(pl.Int32).is_in([20, 21, 22, 23]))
    ).then(pl.lit(0)).otherwise(pl.lit(1)).cast(pl.Int8).alias("NIGHT_FLAG"),
])

# 3) 성별×연령대(10~39 / 그외) 합계 컬럼
lf = lf.with_columns([
    pl.sum_horizontal(*(pl.col(c) for c in mcols(AGE_1039))).alias("M_1039"),
    pl.sum_horizontal(*(pl.col(c) for c in mcols(AGE_ELSE))).alias("M_ELSE"),
    pl.sum_horizontal(*(pl.col(c) for c in fcols(AGE_1039))).alias("F_1039"),
    pl.sum_horizontal(*(pl.col(c) for c in fcols(AGE_ELSE))).alias("F_ELSE"),
])

# 4) 키 기준 집계  ← [변경] NIGHT_FLAG 추가
keys = ["ETL_YM", "ADMI_CD", "WEEKEND_FLAG", "MOVE_PURPOSE_BIN", "NIGHT_FLAG"]
g = (
    lf.group_by(keys)
      .agg([
          pl.col("M_1039").sum().alias("M_1039"),
          pl.col("M_ELSE").sum().alias("M_ELSE"),
          pl.col("F_1039").sum().alias("F_1039"),
          pl.col("F_ELSE").sum().alias("F_ELSE"),
      ])
)

# 5) 성별/연령대 축 풀기(4개 버킷 → 행 확장)
gm1039 = (g.select(keys + [pl.col("M_1039").round(0).cast(pl.Int64).alias("TOTAL_CNT")])
            .with_columns([pl.lit("0:남성").alias("성별"), pl.lit("0:10~39세").alias("연령대")]))
gmelse = (g.select(keys + [pl.col("M_ELSE").round(0).cast(pl.Int64).alias("TOTAL_CNT")])
            .with_columns([pl.lit("0:남성").alias("성별"), pl.lit("1:그외 연령대").alias("연령대")]))
gf1039 = (g.select(keys + [pl.col("F_1039").round(0).cast(pl.Int64).alias("TOTAL_CNT")])
            .with_columns([pl.lit("1:여성").alias("성별"), pl.lit("0:10~39세").alias("연령대")]))
gfelse = (g.select(keys + [pl.col("F_ELSE").round(0).cast(pl.Int64).alias("TOTAL_CNT")])
            .with_columns([pl.lit("1:여성").alias("성별"), pl.lit("1:그외 연령대").alias("연령대")]))

final_lf = pl.concat([gm1039, gmelse, gf1039, gfelse])

# 6) 표시 라벨(평일/주말, 목적) 부여 + ADMI_NM 조인
final_lf = final_lf.with_columns([
    pl.when(pl.col("WEEKEND_FLAG") == 1).then(pl.lit("1:주말")).otherwise(pl.lit("0:평일")).alias("평일/주말"),
    pl.when(pl.col("MOVE_PURPOSE_BIN") == 0).then(pl.lit("0:쇼핑")).otherwise(pl.lit("1:그외 목적")).alias("목적"),
    # [추가] night_time 라벨
    pl.when(pl.col("NIGHT_FLAG") == 0)
      .then(pl.lit("0:18:00~23:59"))
      .otherwise(pl.lit("1:그외 시간대"))
      .alias("night_time"),
      
]).drop(["WEEKEND_FLAG", "MOVE_PURPOSE_BIN"])

dict_lf = (
    pl.scan_csv(DICT_CSV)
      .select(pl.col("ADMI_CD").cast(pl.Utf8), pl.col("ADMI_NM").cast(pl.Utf8))
      .unique(subset=["ADMI_CD"])
)

final_lf = (
    final_lf.join(dict_lf, on="ADMI_CD", how="left")
            .select(["ETL_YM","ADMI_CD","ADMI_NM","평일/주말",'night_time',"성별","연령대","목적","TOTAL_CNT"])
            .sort(["ETL_YM","ADMI_CD","평일/주말",'night_time',"성별","연령대","목적"])
)

# 7) 저장 및 검증
final_lf.sink_parquet(OUT_PARQ)
final_df = pl.read_parquet(OUT_PARQ)
final_df.write_csv(OUT_CSV)
print(final_df.head(40))
print("rows:", pl.scan_parquet(OUT_PARQ).select(pl.len()).collect().item())
