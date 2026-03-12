# make_fact_od_mobility_demo.py
# - 원본 대용량 파케이에서 필요한 컬럼만 뽑아
#   (ETL_YMD, O_ADMI_CD, D_ADMI_CD, MOVE_PURPOSE를 그레인으로)
#   TOTAL_CNT 및 모든 MAL_*/FEM_* *_CNT 컬럼을 합계 집계하여 새 파케이 생성

import polars as pl

# === 경로 설정 ===
IN  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_opt.parquet"  # 원본 통합본
OUT = "/home1/bismarck/transit_seoul/dataset/fact_od_mobility_demo.parquet"

# === 그레인(그룹키) ===
KEYS = ["ETL_YMD", "O_ADMI_CD", "D_ADMI_CD", "MOVE_PURPOSE"]

# === 성별/연령 코호트 패턴 (예: MAL_0_CNT, MAL_5_CNT … / FEM_0_CNT, FEM_5_CNT …) ===
COHORT_PATTERNS = ["^MAL_.*_CNT$", "^FEM_.*_CNT$"]

def main():
    lf = pl.scan_parquet(IN)

    # 필수 컬럼 점검
    required = set(KEYS + ["TOTAL_CNT"])
    missing = [c for c in required if c not in lf.columns]
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing}")

    # 필요한 컬럼만 선택 (프룬/프레디케이트 푸시다운 유도)
    sel_exprs = [pl.col(k) for k in KEYS] + [pl.col("TOTAL_CNT")] + [pl.col(p) for p in COHORT_PATTERNS]

    # 그룹 집계: TOTAL_CNT 및 모든 *_CNT 코호트 합계
    # null 안전: 코호트 값 null은 0으로 간주
    out_lf = (
        lf.select(sel_exprs)
          .group_by(KEYS, maintain_order=False)
          .agg(
              pl.col("TOTAL_CNT").sum().alias("TOTAL_CNT"),
              pl.col("^MAL_.*_CNT$").fill_null(0).sum(),
              pl.col("^FEM_.*_CNT$").fill_null(0).sum(),
          )
    )

    # 스트리밍 저장 (메모리 절약)
    out_lf.sink_parquet(OUT)
    print(f"[OK] written: {OUT}")

    # 결과 간단 검증(행수/헤드)
    rc = pl.scan_parquet(OUT).select(pl.len()).collect().item()
    print(f"[INFO] row_count = {rc}")
    print(pl.read_parquet(OUT, n_rows=100))

if __name__ == "__main__":
    main()
