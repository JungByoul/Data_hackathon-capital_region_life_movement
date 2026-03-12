#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# peek_outputs_simple.py : 생성된 파케이들 내용 간단 확인(shape / schema / head)
import os
import polars as pl

OUT_DIR = "/home1/bismarck/transit_seoul/dataset/erd"  # parquet 저장 폴더
HEAD_N = 100  # head() 출력 행 수

pl.Config.set_tbl_cols(-1)         # 모든 컬럼 표시
pl.Config.set_tbl_width_chars(0)   # 터미널 가로폭 제한 해제
pl.Config.set_tbl_rows(HEAD_N)     # 폴라스 표 출력 행 수 힌트

def peek(df: pl.DataFrame, n: int = HEAD_N):
    print(f"  shape = ({df.height:,}, {df.width:,})")
    print("  schema:", {k: str(v) for k, v in df.schema.items()})
    print("  head:")
    print(df.head(n))

def preview_generic(path: str):
    print(f"\n=== {os.path.basename(path)} ===")
    try:
        df = pl.read_parquet(path)
        peek(df, HEAD_N)
    except Exception as e:
        print("  ⚠️", e)

def main():
    files = [
        "dim_move_purpose.parquet",
        "fact_od_flow.parquet",
        "qa_demo_mismatch.parquet",
        "qa_invalid_time.parquet",
        # 필요하면 여기에 다른 파일명을 추가
        # "dim_time_slot.parquet",
        # "dim_date.parquet",
        # "dim_admin.parquet",
    ]
    for name in files:
        path = os.path.join(OUT_DIR, name)
        if not os.path.exists(path):
            print(f"\n=== {name} ===\n  (파일 없음)")
            continue
        preview_generic(path)

if __name__ == "__main__":
    main()
