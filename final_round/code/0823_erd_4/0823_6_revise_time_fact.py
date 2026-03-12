import polars as pl

IN  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_opt.parquet"
OUT = "/home1/bismarck/transit_seoul/dataset/fact_od_mobility_cnt.parquet"

CANDIDATES = {
    "ETL_YMD"     : ["ETL_YMD", "etl_ymd", "DATE_ID", "date_id"],
    "O_ADMI_CD"   : ["O_ADMI_CD", "o_admdong_cd", "O_ADMI_CODE"],
    "D_ADMI_CD"   : ["D_ADMI_CD", "d_admdong_cd", "D_ADMI_CODE"],
    "ST_TIME_CD"  : ["ST_TIME_CD", "st_time_cd", "ST_NORM", "st_norm", "ST_TIME", "st_time"],
    "FNS_TIME_CD" : ["FNS_TIME_CD", "fns_time_cd", "FN_NORM", "fn_norm", "FNS_TIME", "fns_time"],
    "MOVE_PURPOSE": ["MOVE_PURPOSE", "purpose_cd", "move_purpose", "MOVE_PURPOSE_CD"],
    "TOTAL_CNT"   : ["TOTAL_CNT", "total_cnt", "TOT_CNT", "tot_cnt"]
}

def pick(colnames: list[str], lf_cols: list[str]) -> str | None:
    for c in colnames:
        if c in lf_cols:
            return c
    return None

def main():
    lf = pl.scan_parquet(IN)
    avail = lf.columns

    etl_y   = pick(CANDIDATES["ETL_YMD"], avail)
    o_admi  = pick(CANDIDATES["O_ADMI_CD"], avail)
    d_admi  = pick(CANDIDATES["D_ADMI_CD"], avail)
    st_cd   = pick(CANDIDATES["ST_TIME_CD"], avail)
    fn_cd   = pick(CANDIDATES["FNS_TIME_CD"], avail)
    purpose = pick(CANDIDATES["MOVE_PURPOSE"], avail)
    tot_col = pick(CANDIDATES["TOTAL_CNT"], avail)

    missing = [k for k,v in {
        "ETL_YMD": etl_y, "O_ADMI_CD": o_admi, "D_ADMI_CD": d_admi,
        "ST_TIME_CD": st_cd, "FNS_TIME_CD": fn_cd, "MOVE_PURPOSE": purpose,
        "TOTAL_CNT": tot_col
    }.items() if v is None]
    if missing:
        raise ValueError(f"필수 컬럼을 찾지 못했습니다: {missing}\n실제 컬럼 목록: {avail}")

    # ▶ 날짜 정수 범위
    DATE_FROM_I = 20240801
    DATE_TO_I   = 20241030

    # 0) 스키마 확인
    print("[DBG] schema:", lf.schema)

    # 1) ETL_YMD 정규화: 어떤 dtype이든 Int64로 맞춤
    lf2 = lf.with_columns(
        pl.col(etl_y)
        .cast(pl.Utf8)
        .str.replace_all(r"\D", "")   # 하이픈/공백 등 제거
        .cast(pl.Int64)
        .alias(etl_y)
    )

    # 2) 원본의 날짜 범위/연도별 존재여부 빠른 진단
    stats = lf2.select(
        pl.len().alias("rows_all"),
        pl.col(etl_y).min().alias("min_ymd"),
        pl.col(etl_y).max().alias("max_ymd"),
    ).collect()
    print("[DBG] rows/min/max:", stats)

    rows_2025 = lf2.filter((pl.col(etl_y) >= 20250101) & (pl.col(etl_y) <= 20251231)) \
                .select(pl.len().alias("rows_2025")).collect()
    print("[DBG] rows_2025:", rows_2025)

    by_month = (
        lf2.with_columns(ym = (pl.col(etl_y) // 100))
        .group_by("ym").len()
        .sort("ym")
        .collect()
    )
    print("[DBG] by_month (YYYYMM & counts):")
    print(by_month)

    # 3) 안전필터(동적 클리핑): 원본의 min/max와 교집합만 필터
    bounds = stats.to_dicts()[0]
    lo = max(DATE_FROM_I, bounds["min_ymd"]) if bounds["min_ymd"] is not None else DATE_FROM_I
    hi = min(DATE_TO_I,   bounds["max_ymd"]) if bounds["max_ymd"] is not None else DATE_TO_I

    if lo > hi:
        print(f"[WARN] 원본 범위({bounds['min_ymd']}~{bounds['max_ymd']})에 "
            f"요청 범위({DATE_FROM_I}~{DATE_TO_I})가 겹치지 않습니다. 결과는 비게 됩니다.")
        base = lf2.filter(pl.lit(False))  # 빈 결과
    else:
        base = lf2.filter((pl.col(etl_y) >= lo) & (pl.col(etl_y) <= hi))
        print(f"[INFO] 적용 필터 범위: {lo} ~ {hi}")

    # 4) 이후 동일
    keys = [etl_y, o_admi, d_admi, st_cd, fn_cd, purpose]
    out_lf = (
        base.select([pl.col(c) for c in keys] + [pl.col(tot_col)])
            .group_by(keys, maintain_order=False)
            .agg(pl.col(tot_col).fill_null(0).sum().alias("TOTAL_CNT"))
            .rename({
                etl_y: "ETL_YMD",
                o_admi: "O_ADMI_CD",
                d_admi: "D_ADMI_CD",
                st_cd: "ST_TIME_CD",   # 표준명으로 맞췄습니다(원하시면 'st_norm' 유지 가능)
                fn_cd: "FNS_TIME_CD",
                purpose: "MOVE_PURPOSE",
            })
    )

    out_lf.sink_parquet(OUT)
    print(f"[OK] written: {OUT}")

    rc = pl.scan_parquet(OUT).select(pl.len()).collect().item()
    print(f"[INFO] row_count = {rc}")
    print(pl.read_parquet(OUT, n_rows=200))


if __name__ == "__main__":
    main()

# 최초에 그냥 전체 다 뱉은 코드

# # make_fact_od_mobility_cnt_by_time.py
# # - 그루핑 키: ETL_YMD, O_ADMI_CD, D_ADMI_CD, ST_TIME_CD, FNS_TIME_CD, MOVE_PURPOSE
# # - 집계: TOTAL_CNT 합계
# # - 컬럼명이 소문자/대문자/별칭(st_norm, fn_norm, purpose_cd 등)인 경우도 자동 매핑

# import polars as pl

# IN  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_opt.parquet"   # 원본
# OUT = "/home1/bismarck/transit_seoul/dataset/fact_od_mobility_cnt.parquet"  # 출력

# # 후보 컬럼명(가능한 별칭들 포함)
# CANDIDATES = {
#     "ETL_YMD"     : ["ETL_YMD", "etl_ymd", "DATE_ID", "date_id"],
#     "O_ADMI_CD"   : ["O_ADMI_CD", "o_admdong_cd", "O_ADMI_CODE"],
#     "D_ADMI_CD"   : ["D_ADMI_CD", "d_admdong_cd", "D_ADMI_CODE"],
#     "ST_TIME_CD"  : ["ST_TIME_CD", "st_time_cd", "ST_NORM", "st_norm", "ST_TIME", "st_time"],
#     "FNS_TIME_CD" : ["FNS_TIME_CD", "fns_time_cd", "FN_NORM", "fn_norm", "FNS_TIME", "fns_time"],
#     "MOVE_PURPOSE": ["MOVE_PURPOSE", "purpose_cd", "move_purpose", "MOVE_PURPOSE_CD"],
#     "TOTAL_CNT"   : ["TOTAL_CNT", "total_cnt", "TOT_CNT", "tot_cnt"]
# }

# def pick(colnames: list[str], lf_cols: list[str]) -> str:
#     """후보 리스트에서 실제 존재하는 첫 컬럼명을 고릅니다."""
#     for c in colnames:
#         if c in lf_cols:
#             return c
#     return None

# def main():
#     lf = pl.scan_parquet(IN)
#     avail = lf.columns

#     etl_y   = pick(CANDIDATES["ETL_YMD"], avail)
#     o_admi  = pick(CANDIDATES["O_ADMI_CD"], avail)
#     d_admi  = pick(CANDIDATES["D_ADMI_CD"], avail)
#     st_cd   = pick(CANDIDATES["ST_TIME_CD"], avail)
#     fn_cd   = pick(CANDIDATES["FNS_TIME_CD"], avail)
#     purpose = pick(CANDIDATES["MOVE_PURPOSE"], avail)
#     tot_col = pick(CANDIDATES["TOTAL_CNT"], avail)

#     missing = [k for k,v in {
#         "ETL_YMD": etl_y, "O_ADMI_CD": o_admi, "D_ADMI_CD": d_admi,
#         "ST_TIME_CD": st_cd, "FNS_TIME_CD": fn_cd, "MOVE_PURPOSE": purpose,
#         "TOTAL_CNT": tot_col
#     }.items() if v is None]
#     if missing:
#         raise ValueError(f"필수 컬럼을 찾지 못했습니다: {missing}\n실제 컬럼 목록: {avail}")

#     keys = [etl_y, o_admi, d_admi, st_cd, fn_cd, purpose]

#     # 필요한 컬럼만 선택 → 그룹합계
#     out_lf = (
#         lf.select([pl.col(c) for c in keys] + [pl.col(tot_col)])
#           .group_by(keys, maintain_order=False)
#           .agg(pl.col(tot_col).fill_null(0).sum().alias("TOTAL_CNT"))
#           # 최종 컬럼명을 요청하신 표준명으로 정리
#           .rename({
#               etl_y: "ETL_YMD",
#               o_admi: "O_ADMI_CD",
#               d_admi: "D_ADMI_CD",
#               st_cd: "st_norm",
#               fn_cd: "FNS_TIME_CD",
#               purpose: "MOVE_PURPOSE",
#           })
#           # TOTAL_CNT는 합계라 그대로 둡니다(원본이 float이면 float 유지)
#     )

#     # 스트리밍 저장
#     out_lf.sink_parquet(OUT)
#     print(f"[OK] written: {OUT}")

#     # 간단 검증
#     rc = pl.scan_parquet(OUT).select(pl.len()).collect().item()
#     print(f"[INFO] row_count = {rc}")
#     print(pl.read_parquet(OUT, n_rows=200))

# if __name__ == "__main__":
#     main()
