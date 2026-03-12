#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# conditional_tables.py : 조건부 생성 테이블 (목적/팩트/QA/연간 상주인구)
import re
import pathlib
import polars as pl

# ===== 경로 설정 (여기만 고치면 됩니다) =====
IN_PATH  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_select.parquet"   
OUT_DIR  = "/home1/bismarck/transit_seoul/dataset/erd"                 # 출력 폴더
                
TS_PATH  = "/home1/bismarck/transit_seoul/dataset/erd/dim_time_slot.parquet"  # always_dims.py가 만든 시간차원

# 빈 DF도 저장할지 여부
ALWAYS_WRITE_EMPTY = False
# 성·연령 합계 vs TOTAL_CNT 검증 허용 오차
TOL_DIFF = 1e-6

# 목적코드 매핑 (고정)
PURPOSE_MAP = pl.DataFrame({
    "purpose_cd": [1, 2, 3, 4, 5, 6, 7],
    "purpose_nm": ["출근", "등교", "귀가", "쇼핑", "관광", "병원", "기타"]
}).with_columns(pl.col("purpose_cd").cast(pl.Int8))

# ===== 공통 IO =====
def read_lazy(path: str) -> pl.LazyFrame:
    p = path.lower()
    if p.endswith(".parquet"):
        return pl.scan_parquet(path)
    if p.endswith(".csv"):
        return pl.scan_csv(path, infer_schema_length=5000)
    raise ValueError("입력은 .parquet 또는 .csv 여야 합니다.")

def wp(df: pl.DataFrame, path: pathlib.Path):
    if df is None:
        return
    if (not ALWAYS_WRITE_EMPTY) and df.is_empty():
        return
    df.write_parquet(path)

# ===== 메인 =====
def main():
    lf = read_lazy(IN_PATH)
    out = pathlib.Path(OUT_DIR); out.mkdir(parents=True, exist_ok=True)

    # 시간 차원(필수) – 07~09, 17~19는 20분코드만 있어야 함(0700~0940, 1700~1940)
    dim_ts = pl.read_parquet(TS_PATH)
    ts = dim_ts.lazy().select(["time_cd", "minute_of_day"]).rename({"minute_of_day": "st_min"})
    tf = dim_ts.lazy().select(["time_cd", "minute_of_day"]).rename({"minute_of_day": "fn_min"})

    # ---- dim_move_purpose (숫자코드 → 한글명 매핑) ----
    if "MOVE_PURPOSE" in lf.columns:
        # 입력에서 실제 등장한 코드만 추출
        codes = (
            lf.select(pl.col("MOVE_PURPOSE").cast(pl.Int8).alias("purpose_cd"))
              .drop_nulls()
              .unique()
              .collect()
        )
        dim_move_purpose = (
            codes.join(PURPOSE_MAP, on="purpose_cd", how="left")  # 1~7 외는 purpose_nm = null
                 .sort("purpose_cd")
        )
        wp(dim_move_purpose, out / "dim_move_purpose.parquet")

        # 1~7 외의 목적코드 QA
        qa_unknown_purpose = dim_move_purpose.filter(pl.col("purpose_nm").is_null())
        wp(qa_unknown_purpose, out / "qa_unknown_purpose.parquet")

    # ---- 팩트(OD 플로우) + QA ----
    # 성·연령 컬럼 자동 탐지(주의: 0–9세는 10년 폭 → *_05_CNT 없음)
    male_cols = sorted([c for c in lf.columns if re.fullmatch(r"MALE_(\d{2})_CNT", c or "")],
                       key=lambda x: int(re.findall(r"(\d{2})", x)[0]))
    feml_cols = sorted([c for c in lf.columns if re.fullmatch(r"FEML_(\d{2})_CNT", c or "")],
                       key=lambda x: int(re.findall(r"(\d{2})", x)[0]))
    num_cols  = [c for c in ["TOTAL_CNT", "MOVE_DIST", "MOVE_TIME"] + male_cols + feml_cols if c in lf.columns]

    has_move = "MOVE_PURPOSE" in lf.columns

    j = (
        lf.with_columns([
            pl.col("ETL_YMD").cast(pl.Utf8).alias("date_id"),
            pl.col("O_ADMI_CD").cast(pl.Utf8),
            pl.col("D_ADMI_CD").cast(pl.Utf8),
            pl.col("ST_TIME_CD").cast(pl.Utf8).alias("start_time_cd"),
            pl.col("FNS_TIME_CD").cast(pl.Utf8).alias("finish_time_cd"),
            (pl.col("MOVE_PURPOSE").cast(pl.Int8) if has_move else pl.lit(None).cast(pl.Int8)).alias("purpose_cd"),
        ])
        .join(ts, left_on="start_time_cd", right_on="time_cd", how="left")
        .join(tf, left_on="finish_time_cd", right_on="time_cd", how="left")
        .with_columns((pl.col("fn_min") - pl.col("st_min")).alias("slot_minutes"))
    )

    # 시간 매핑 실패/구간 역전 QA
    qa_invalid_time = (
        j.filter((pl.col("st_min").is_null()) | (pl.col("fn_min").is_null()) | (pl.col("slot_minutes") <= 0))
         .select(["date_id","O_ADMI_CD","D_ADMI_CD","start_time_cd","finish_time_cd","purpose_cd","st_min","fn_min","slot_minutes"])
         .collect()
    )
    wp(qa_invalid_time, out / "qa_invalid_time.parquet")

    # 정상 팩트
    fact_od_flow = (
        j.filter(pl.col("slot_minutes") > 0)
         .with_columns(
             pl.concat_str([
                 pl.col("date_id"), pl.lit("|"), pl.col("O_ADMI_CD"), pl.lit("|"),
                 pl.col("D_ADMI_CD"), pl.lit("|"), pl.col("start_time_cd"), pl.lit("|"),
                 pl.col("finish_time_cd"), pl.lit("|"),
                 pl.col("purpose_cd").cast(pl.Utf8)   # flow_id용으로 문자열 결합
             ]).hash().cast(pl.UInt64).alias("flow_id")
         )
         .with_columns([pl.col(c).cast(pl.Float64).fill_null(0.0) for c in num_cols])
         .select(
             ["flow_id","date_id","O_ADMI_CD","D_ADMI_CD","start_time_cd","finish_time_cd","purpose_cd"]
             + [c for c in ["TOTAL_CNT","MOVE_DIST","MOVE_TIME"] if c in num_cols]
             + male_cols + feml_cols
         )
         .collect()
    )
    wp(fact_od_flow, out / "fact_od_flow.parquet")

    # 성·연령 합 vs TOTAL_CNT QA
    if "TOTAL_CNT" in fact_od_flow.columns and (male_cols or feml_cols):
        demo_cols = [c for c in male_cols + feml_cols if c in fact_od_flow.columns]
        qa_demo_mismatch = (
            fact_od_flow.with_columns([
                pl.sum_horizontal([pl.col(c) for c in demo_cols]).alias("sum_demo_cnt"),
                (pl.col("TOTAL_CNT") - pl.col("sum_demo_cnt")).alias("diff_cnt")
            ])
            .filter(pl.col("diff_cnt").abs() > TOL_DIFF)
            .select(["flow_id","date_id","O_ADMI_CD","D_ADMI_CD",
                     "start_time_cd","finish_time_cd","purpose_cd",
                     "TOTAL_CNT","sum_demo_cnt","diff_cnt"])
        )
        wp(qa_demo_mismatch, out / "qa_demo_mismatch.parquet")

    # ---- 연간 상주인구 (연×행정동) ----
    if ("RESIDENTS" in lf.columns) and ("O_ADMI_CD" in lf.columns):
        fact_residents_annual = (
            lf.select([
                pl.col("ETL_YMD").cast(pl.Utf8).str.slice(0, 4).cast(pl.Int16).alias("year"),
                pl.col("O_ADMI_CD").cast(pl.Utf8).alias("admdong_cd"),
                pl.col("RESIDENTS").cast(pl.Float64).alias("residents_cnt")
            ])
            .group_by(["year","admdong_cd"]).agg(pl.max("residents_cnt").alias("residents_cnt"))
            .collect()
        )
        wp(fact_residents_annual, out / "fact_residents_annual.parquet")

    print("[DONE] conditional tables →", out.resolve())

if __name__ == "__main__":
    pl.Config.set_tbl_rows(20)
    main()
