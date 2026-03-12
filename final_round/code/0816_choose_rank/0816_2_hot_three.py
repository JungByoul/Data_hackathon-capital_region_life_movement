import polars as pl

DATA_PATH = "/home1/bismarck/transit_seoul/dataset/merged_all_admi.parquet"

# === 기본 파라미터 ===
START_YMD_INT = 20240601   # ETL_YMD가 Int64
END_YMD_INT   = 20250531

# 목적(기본은 넓게: 필요 시 ['쇼핑','관광','기타']로 교체 권장)
PURPOSES_HOT = ['쇼핑','관광','출근','귀가','병원','기타']

# 시간/요일 필터 (None이면 미적용)
HOUR_MIN, HOUR_MAX = None, None        # 예: 18, 23
WEEKDAYS = None                        # 예: [5,6] (토=5, 일=6) 또는 [0,1,2,3,4] (평일)

SIDO_LIST = ["서울특별시", "경기도", "인천광역시"]

# 20·30대 컬럼(남/여, 5세 구간 포함)
YOUNG_COLS = [
    "MALE_20_CNT","MALE_25_CNT","MALE_30_CNT","MALE_35_CNT",
    "FEML_20_CNT","FEML_25_CNT","FEML_30_CNT","FEML_35_CNT",
]

def build_filtered_hot(lf: pl.LazyFrame, debug: bool = True) -> pl.LazyFrame:
    if debug:
        print("== Step0 | Lazy schema (collect_schema) ==")
        print(lf.collect_schema())

    # 분모: TOTAL_CNT 정리
    cnt = pl.col("TOTAL_CNT").cast(pl.Float64).fill_null(0.0)
    cnt = pl.when(cnt < 0).then(0.0).otherwise(cnt).alias("CNT")

    filtered = (
        lf
        # 기간
        .filter((pl.col("ETL_YMD") >= START_YMD_INT) & (pl.col("ETL_YMD") <= END_YMD_INT))
        # 목적
        .filter(pl.col("MOVE_PURPOSE_NM").is_in(PURPOSES_HOT))
        # 날짜 파싱 (요일/시간 필터 대비)
        .with_columns([
            pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"),
            cnt
        ])
    )

    # 시간 필터(있다면)
    if HOUR_MIN is not None and HOUR_MAX is not None:
        filtered = filtered.filter(
            (pl.col("FNS_TIME_CD").cast(pl.Int64) >= HOUR_MIN) &
            (pl.col("FNS_TIME_CD").cast(pl.Int64) <= HOUR_MAX)
        )

    # 요일 필터(있다면) - weekday: 월=0 … 일=6
    if WEEKDAYS is not None:
        filtered = filtered.filter(pl.col("ETL_DATE").dt.weekday().is_in(WEEKDAYS))

    # 시군구 표기 정규화: 언더스코어→공백, 다중 공백, 좌우 공백 제거
    filtered = filtered.with_columns([
        pl.col("D_SGG_NM").cast(pl.Utf8)
          .str.replace_all(r"[_]+", " ")
          .str.replace_all(r"\s+", " ")
          .str.strip_chars()
          .alias("SGG_NORM")
    ])

    # 20·30대 방문자(행 단위) 계산
    young_sum_expr = sum((pl.col(c).cast(pl.Float64).fill_null(0.0) for c in YOUNG_COLS), pl.lit(0.0))
    filtered = filtered.with_columns([
        young_sum_expr.alias("YOUNG_CNT")
    ])

    # 필요한 컬럼만 유지
    filtered = filtered.select(["D_SIDO_NM","SGG_NORM","YOUNG_CNT","CNT","ETL_DATE"])

    if debug:
        print("\n== Step1 | Filtered sample (5 rows) ==")
        print(filtered.fetch(5))

        print("\n== Step1b | PURPOSE sample & date range check ==")
        # 목적 샘플(퍼포즈 확인), 날짜 범위 확인
        print(
            lf.select("MOVE_PURPOSE_NM").unique().limit(20).collect()
        )
        print(
            filtered.select([
                pl.col("ETL_DATE").min().alias("min_date"),
                pl.col("ETL_DATE").max().alias("max_date")
            ]).collect()
        )

    return filtered

def compute_hot_rank_by_sido(filtered: pl.LazyFrame, sido_name: str, debug: bool = True) -> pl.DataFrame:
    ratio_expr = (pl.col("ANNUAL_YOUNG") / (pl.col("ANNUAL_TOTAL") + 1e-9))

    # 디버그: 시/도별 행 수 & 고유 구/군 수
    if debug:
        rows_cnt = (
            filtered
            .filter(pl.col("D_SIDO_NM") == sido_name)
            .select(pl.len().alias("rows"))
            .collect()
            .item()
        )
        uniq_sgg = (
            filtered
            .filter(pl.col("D_SIDO_NM") == sido_name)
            .select(pl.col("SGG_NORM").n_unique().alias("n_sgg"))
            .collect()
            .item()
        )
        sgg_sample = (
            filtered
            .filter(pl.col("D_SIDO_NM") == sido_name)
            .select(pl.col("SGG_NORM"))
            .unique()
            .limit(10)
            .collect()
        )
        print(f"\n== Step2 | {sido_name} rows: {rows_cnt}, unique SGG: {uniq_sgg} ==")
        print("SGG sample (up to 10):")
        print(sgg_sample)

    df = (
        filtered
        .filter(pl.col("D_SIDO_NM") == sido_name)
        .filter(pl.col("SGG_NORM").is_not_null())
        .group_by("SGG_NORM")
        .agg([
            pl.col("YOUNG_CNT").sum().alias("ANNUAL_YOUNG"),
            pl.col("CNT").sum().alias("ANNUAL_TOTAL"),
        ])
        .with_columns([ ratio_expr.alias("YOUNG_RATIO") ])
        .with_columns([ (pl.col("YOUNG_RATIO") * 100).round(2).alias("YOUNG_PCT") ])
        .sort(by="YOUNG_RATIO", descending=True)
        .with_row_index("RANK", offset=1)
        .select(["RANK","SGG_NORM","ANNUAL_YOUNG","ANNUAL_TOTAL","YOUNG_RATIO","YOUNG_PCT"])
        .collect()
    )

    if debug and df.height > 0:
        print(f"\n== Step3 | {sido_name} HOT rank HEAD(3) ==")
        print(df.head(3))
        print(f"\n== Step3 | {sido_name} HOT rank TAIL(3) ==")
        print(df.tail(3))

    return df

def pick_top_bottom_hot(sido_name: str, df: pl.DataFrame) -> str:
    if df.height == 0:
        return f"{sido_name}: 결과 없음"
    top = df.row(0, named=True)
    bottom = df.row(-1, named=True)
    return (
        f"{sido_name} — 1등: {top['SGG_NORM']} "
        f"(20·30대 비율 {top['YOUNG_PCT']}%, 20·30대 {int(top['ANNUAL_YOUNG'])}, 전체 {int(top['ANNUAL_TOTAL'])})\n"
        f"{sido_name} — 꼴등: {bottom['SGG_NORM']} "
        f"(20·30대 비율 {bottom['YOUNG_PCT']}%, 20·30대 {int(bottom['ANNUAL_YOUNG'])}, 전체 {int(bottom['ANNUAL_TOTAL'])})"
    )

# ===== 실행 =====
lf = pl.scan_parquet(DATA_PATH)
filtered_hot = build_filtered_hot(lf, debug=True)

seoul_hot    = compute_hot_rank_by_sido(filtered_hot, "서울특별시", debug=True)
gyeonggi_hot = compute_hot_rank_by_sido(filtered_hot, "경기도",     debug=True)
incheon_hot  = compute_hot_rank_by_sido(filtered_hot, "인천광역시", debug=True)

print("\n== 핫플레이스 지표: 20·30대 비율 (2024-06~2025-05, 목적/시간/요일 기본값 적용) ==")
print(pick_top_bottom_hot("서울특별시", seoul_hot))
print(pick_top_bottom_hot("경기도",     gyeonggi_hot))
print(pick_top_bottom_hot("인천광역시", incheon_hot))

# 필요 시 전체 랭킹 저장
# seoul_hot.write_csv("hot_rank_Seoul.csv")
# gyeonggi_hot.write_csv("hot_rank_Gyeonggi.csv")
# incheon_hot.write_csv("hot_rank_Incheon.csv")
