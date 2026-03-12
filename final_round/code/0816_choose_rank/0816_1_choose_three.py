import polars as pl

DATA_PATH = "/home1/bismarck/transit_seoul/dataset/merged_all_admi.parquet"

PURPOSES = ['쇼핑','관광','출근','귀가','병원','기타','등교']
# ETL_YMD가 Int64이므로 정수로 비교
START_YMD_INT = 20240601
END_YMD_INT   = 20250531

SIDO_LIST = ["서울특별시", "경기도", "인천광역시"]

def build_filtered(lf: pl.LazyFrame, debug: bool = True) -> pl.LazyFrame:
    if debug:
        print("== Step0 | Lazy schema (collect_schema) ==")
        # 성능 경고 없이 스키마 확인
        print(lf.collect_schema())

    # TOTAL_CNT 정리
    cnt = pl.col("TOTAL_CNT").cast(pl.Float64).fill_null(0.0)
    cnt = pl.when(cnt < 0).then(0.0).otherwise(cnt).alias("CNT")

    filtered = (
        lf
        # 기간: Int64 비교
        .filter((pl.col("ETL_YMD") >= START_YMD_INT) & (pl.col("ETL_YMD") <= END_YMD_INT))
        # 도착 기준 18~23시
        .filter((pl.col("FNS_TIME_CD").cast(pl.Int64) >= 18) &
                (pl.col("FNS_TIME_CD").cast(pl.Int64) <= 23))
        # 목적 필터
        .filter(pl.col("MOVE_PURPOSE_NM").is_in(PURPOSES))
        # 평일(월=0~금=4) - ETL_YMD가 Int64 → Utf8로 캐스팅 후 날짜 파싱
        .with_columns([
            pl.col("ETL_YMD").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d").alias("ETL_DATE"),
            cnt
        ])
        .filter(pl.col("ETL_DATE").dt.weekday().is_in([0,1,2,3,4]))
        # 시군구 표기 정규화: 언더스코어→공백, 다중 공백 정리, 좌우 공백 제거
        .with_columns([
            pl.col("D_SGG_NM").cast(pl.Utf8)
              .str.replace_all(r"[_]+", " ")
              .str.replace_all(r"\s+", " ")
              .str.strip_chars()                    # <-- strip() 대신 strip_chars()
              .alias("SGG_NORM")
        ])
        .select(["D_SIDO_NM","D_SGG_NM","SGG_NORM","MOVE_PURPOSE_NM","CNT"])
    )

    if debug:
        print("\n== Step1 | Filtered sample (5 rows) ==")
        print(filtered.fetch(5))

        # 목적값 분포 확인
        print("\n== Step1b | MOVE_PURPOSE_NM unique (sample up to 20) ==")
        print(
            filtered.select("MOVE_PURPOSE_NM").unique().limit(20).collect()
        )

    return filtered

def compute_bedtown_rank_by_sido(filtered: pl.LazyFrame, sido_name: str, debug: bool = True) -> pl.DataFrame:
    ratio_expr = (pl.col("ANNUAL_RETURN") / (pl.col("ANNUAL_TOTAL") + 1e-9))

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
            pl.when(pl.col("MOVE_PURPOSE_NM") == "귀가").then(pl.col("CNT")).otherwise(0.0)
              .sum().alias("ANNUAL_RETURN"),
            pl.col("CNT").sum().alias("ANNUAL_TOTAL"),
        ])
        .with_columns([ ratio_expr.alias("RETURN_RATIO") ])
        .with_columns([ (pl.col("RETURN_RATIO") * 100).round(2).alias("RETURN_PCT") ])
        .sort(by="RETURN_RATIO", descending=True)
        .with_row_index("RANK", offset=1)
        .select(["RANK","SGG_NORM","ANNUAL_RETURN","ANNUAL_TOTAL","RETURN_RATIO","RETURN_PCT"])
        .collect()
    )

    if debug and df.height > 0:
        print(f"\n== Step3 | {sido_name} rank HEAD(3) ==")
        print(df.head(3))
        print(f"\n== Step3 | {sido_name} rank TAIL(3) ==")
        print(df.tail(3))

    return df

def pick_top_bottom(sido_name: str, df: pl.DataFrame) -> str:
    if df.height == 0:
        return f"{sido_name}: 결과 없음"
    top = df.row(0, named=True)
    bottom = df.row(-1, named=True)
    return (
        f"{sido_name} — 1등: {top['SGG_NORM']} "
        f"(비율 {top['RETURN_PCT']}%, 분자 {int(top['ANNUAL_RETURN'])}, 분모 {int(top['ANNUAL_TOTAL'])})\n"
        f"{sido_name} — 꼴등: {bottom['SGG_NORM']} "
        f"(비율 {bottom['RETURN_PCT']}%, 분자 {int(bottom['ANNUAL_RETURN'])}, 분모 {int(bottom['ANNUAL_TOTAL'])})"
    )

# ===== 실행 =====
lf = pl.scan_parquet(DATA_PATH)
filtered = build_filtered(lf, debug=True)

seoul_rank    = compute_bedtown_rank_by_sido(filtered, "서울특별시", debug=True)
gyeonggi_rank = compute_bedtown_rank_by_sido(filtered, "경기도",     debug=True)
incheon_rank  = compute_bedtown_rank_by_sido(filtered, "인천광역시", debug=True)

print("\n== 베드타운 지표: 퇴근 시간 귀가인구비율 (평일 18~23시, 2024-06~2025-05) ==")
print(pick_top_bottom("서울특별시", seoul_rank))
print(pick_top_bottom("경기도",     gyeonggi_rank))
print(pick_top_bottom("인천광역시", incheon_rank))

# 필요 시 전체 랭킹 저장
# seoul_rank.write_csv("bedtown_rank_Seoul.csv")
# gyeonggi_rank.write_csv("bedtown_rank_Gyeonggi.csv")
# incheon_rank.write_csv("bedtown_rank_Incheon.csv")
