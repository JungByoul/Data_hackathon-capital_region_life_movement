#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import polars as pl

HEAD_N = 100  # head() 출력 행 수

pl.Config.set_tbl_cols(-1)         # 모든 컬럼 표시
pl.Config.set_tbl_width_chars(0)   # 터미널 가로폭 제한 해제
pl.Config.set_tbl_rows(HEAD_N)     # 폴라스 표 출력 행 수 힌트


OUT_DIR = "/home1/bismarck/transit_seoul/dataset/erd"
SRC = os.path.join(OUT_DIR, "qa_demo_mismatch.parquet")
DST = os.path.join(OUT_DIR, "qa_demo_mismatch_core.parquet")  # 새 파일 이름

DROP_COLS = {"sum_demo_cnt", "diff_cnt", "rel_err"}

def main():
    df = pl.read_parquet(SRC)
    keep_cols = [c for c in df.columns if c not in DROP_COLS]
    out = df.select(keep_cols)

    out.write_parquet(DST)

    # 간단 확인 출력
    print(f"written: {DST}")
    print(f"  kept {len(keep_cols)} cols -> {keep_cols}")
    print(f"  shape = {out.shape}")
    
    
    # 3️⃣ 앞부분 확인 (상위 10행)
    print("\n[Head 100 rows]")
    print(out.head(100))

    # 4️⃣ 뒷부분 확인 (마지막 10행)
    print("\n[Tail 100 rows]")
    print(out.tail(100))

    # 5️⃣ 요약 통계 (수치형 컬럼만)
    print("\n[Summary statistics]")
    print(out.describe())

    # 6️⃣ mismatch가 큰 순으로 몇 개 뽑기 (diff_cnt 기준)
    print("\n[Top 50 mismatches by diff_cnt]")
    print(out.sort("diff_cnt", descending=True).head(50))

if __name__ == "__main__":
    main()
