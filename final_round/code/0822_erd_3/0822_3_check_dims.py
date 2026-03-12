#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import polars as pl
import os

OUT_DIR = '/home1/bismarck/transit_seoul/dataset/erd'  # parquet 저장된 폴더

def peek(df: pl.DataFrame, n: int = 100):
    """공통 출력: shape, schema, head"""
    print(f"  shape = ({df.height:,}, {df.width:,})")
    print("  schema:", {k: str(v) for k, v in df.schema.items()})
    print("  head:")
    print(df.head(n))

def preview_dim_time_slot(path: str):
    print(f"\n=== {os.path.basename(path)} ===")
    try:
        df = pl.read_parquet(path)
        peek(df)
        if {"minute_of_day", "bucket", "commute_flag"}.issubset(df.columns):
            n_total = df.height
            n_morning = df.filter(pl.col("bucket") == "morning").height
            n_evening = df.filter(pl.col("bucket") == "evening").height
            print(f"  slots: total={n_total}, morning={n_morning}, evening={n_evening}")
            print(f"  minute_of_day range: {df['minute_of_day'].min()} ~ {df['minute_of_day'].max()}")
    except Exception as e:
        print("  ⚠️", e)

def preview_dim_date(path: str):
    print(f"\n=== {os.path.basename(path)} ===")
    try:
        df = pl.read_parquet(path)
        peek(df)
        if "date_id" in df.columns:
            d2 = df.with_columns(pl.col("date_id").str.to_date(format="%Y%m%d").alias("date"))
            dmin = d2["date"].min()
            dmax = d2["date"].max()
            years = sorted(df["year"].unique().to_list()) if "year" in df.columns else []
            wkd = int(df.filter(pl.col("is_weekend") == True).height) if "is_weekend" in df.columns else 0
            print(f"  date range: {dmin} ~ {dmax}")
            print(f"  years: {years}")
            print(f"  weekend rows: {wkd}")
    except Exception as e:
        print("  ⚠️", e)

def preview_dim_admin(path: str):
    print(f"\n=== {os.path.basename(path)} ===")
    try:
        df = pl.read_parquet(path)
        peek(df)
        if "sgg_nm" in df.columns:
            top = (
                df.group_by("sgg_nm").len().sort("len", descending=True).head(10)
            )
            print("  top sgg_nm by count:")
            print(top)
    except Exception as e:
        print("  ⚠️", e)

def preview_generic(path: str):
    print(f"\n=== {os.path.basename(path)} ===")
    try:
        df = pl.read_parquet(path)
        peek(df)
    except Exception as e:
        print("  ⚠️", e)

def main():
    files = [
        "dim_time_slot.parquet",
        "dim_date.parquet",
        "dim_admin.parquet"
        # "dim_move_purpose.parquet",
        # "fact_od_flow.parquet",
        # "fact_residents_annual.parquet",
        # "qa_invalid_time.parquet",
        # "qa_demo_mismatch.parquet",
    ]
    for name in files:
        path = os.path.join(OUT_DIR, name)
        if not os.path.exists(path):  # ✅ 문자열 기반 체크
            print(f"\n=== {name} ===\n  (파일 없음)")
            continue
        if name == "dim_time_slot.parquet":
            preview_dim_time_slot(path)
        elif name == "dim_date.parquet":
            preview_dim_date(path)
        elif name == "dim_admin.parquet":
            preview_dim_admin(path)
        else:
            preview_generic(path)

if __name__ == "__main__":
    pl.Config.set_tbl_rows(50)
    main()
