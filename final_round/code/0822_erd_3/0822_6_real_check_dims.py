#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# check_conditionals.py : dim_move_purpose / fact_od_flow / qa_invalid_time 점검용
import os
import polars as pl

OUT_DIR = "/home1/bismarck/transit_seoul/dataset/erd"  # 출력 폴더

pl.Config.set_tbl_cols(-1)  # -1이면 모든 컬럼 출력
# pl.Config.set_tbl_rows(5)   # 표시할 행 수
pl.Config.set_tbl_width_chars(0)  # 터미널 가로폭 제한 해제


# ---- 공통 ----
def peek(df: pl.DataFrame, n: int = 100):
    print(f"  shape = ({df.height:,}, {df.width:,})")
    print("  schema:", {k: str(v) for k, v in df.schema.items()})
    print("  head:")
    print(df.head(n))

# ---- dim_move_purpose ----
def preview_dim_move_purpose(path: str):
    print(f"\n=== {os.path.basename(path)} ===")
    try:
        df = pl.read_parquet(path)
        peek(df, n=20)

        # 목적코드 기본 도메인(1~7)
        valid = set(range(1, 8))
        codes = set(df["purpose_cd"].drop_nulls().to_list())
        unknown = sorted(codes - valid)
        missing_names = int(df.filter(pl.col("purpose_nm").is_null()).height)

        print(f"  purpose codes present: {sorted(codes)}")
        print(f"  unknown codes (not in 1~7): {unknown if unknown else '없음'}")
        print(f"  rows with NULL purpose_nm: {missing_names}")
        if "purpose_nm" in df.columns:
            print("  mapping sample:")
            print(df.sort("purpose_cd").head(10))
    except Exception as e:
        print("  ⚠️", e)

# ---- fact_od_flow ----
def preview_fact_od_flow(path: str):
    print(f"\n=== {os.path.basename(path)} ===")
    try:
        df = pl.read_parquet(path)
        peek(df, n=20)

        # 핵심 키/필수 컬럼 NULL 검사
        key_cols = ["flow_id", "date_id", "O_ADMI_CD", "D_ADMI_CD", "start_time_cd", "finish_time_cd", "purpose_cd"]
        for c in key_cols:
            if c in df.columns:
                n_null = int(df.filter(pl.col(c).is_null()).height)
                if n_null:
                    print(f"  NULL in {c}: {n_null}")

        # 날짜 범위
        if "date_id" in df.columns:
            d2 = df.select(pl.col("date_id").str.to_date(format="%Y%m%d").alias("date")).drop_nulls()
            if d2.height:
                print(f"  date range: {d2['date'].min()} ~ {d2['date'].max()}  (rows with valid date_id: {d2.height:,})")

        # 시간코드/목적코드 커버리지
        for c in ["start_time_cd", "finish_time_cd"]:
            if c in df.columns:
                print(f"  distinct {c}: {df.select(pl.col(c)).n_unique()}")

        if "purpose_cd" in df.columns:
            pc = sorted(df.select("purpose_cd").unique().drop_nulls()["purpose_cd"].to_list())
            print(f"  purpose_cd distinct: {pc}")

        # 성·연령 합계 vs TOTAL_CNT 간단 검증(있을 때만)
        demo_cols = [c for c in df.columns if c.startswith("MALE_") or c.startswith("FEML_")]
        if "TOTAL_CNT" in df.columns and demo_cols:
            chk = (
                df.select([
                    (pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in demo_cols])).alias("sum_demo_cnt"),
                    pl.col("TOTAL_CNT").alias("TOTAL_CNT")
                ])
                .with_columns((pl.col("TOTAL_CNT") - pl.col("sum_demo_cnt")).alias("diff_cnt"))
            )
            n_bad = int(chk.filter(pl.col("diff_cnt").abs() > 1e-6).height)
            print(f"  demo sum vs TOTAL_CNT mismatches: {n_bad} rows (|diff|>1e-6)")
    except Exception as e:
        print("  ⚠️", e)

# ---- qa_invalid_time ----
def preview_qa_invalid_time(path: str):
    print(f"\n=== {os.path.basename(path)} ===")
    try:
        df = pl.read_parquet(path)
        if df.is_empty():
            print("  (비어있음) → 모든 시간 매핑/구간 정상")
            return
        peek(df, n=20)

        # 원인별 집계
        cause = df.with_columns([
            pl.when(pl.col("st_min").is_null()).then(pl.lit("missing_start"))
              .when(pl.col("fn_min").is_null()).then(pl.lit("missing_finish"))
              .when(pl.col("slot_minutes") <= 0).then(pl.lit("non_positive_interval"))
              .otherwise(pl.lit("unknown")).alias("reason")
        ]).group_by("reason").len().sort("len", descending=True)
        print("  reasons:")
        print(cause)
    except Exception as e:
        print("  ⚠️", e)

def main():
    files = [
        ("dim_move_purpose.parquet", preview_dim_move_purpose),
        ("fact_od_flow.parquet",     preview_fact_od_flow),
        ("qa_invalid_time.parquet",  preview_qa_invalid_time),
    ]
    for name, fn in files:
        path = os.path.join(OUT_DIR, name)
        if not os.path.exists(path):
            print(f"\n=== {name} ===\n  (파일 없음)")
            continue
        fn(path)

if __name__ == "__main__":
    pl.Config.set_tbl_rows(80)
    main()
