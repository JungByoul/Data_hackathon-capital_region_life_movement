# 0812 화
# 2024년 6월~2025년 5월 원본 parquet 통합본 완성 코드

# 결과값 파일: /home1/bismarck/transit_seoul/out_error/star0812/output/458236_통합parquet완성.out
    # 소요시간: 290.0s
import os, time
import polars as pl

# INPUT
PATTERN = "/home1/bismarck/transit_seoul/dataset/data_month/year=2024/month=*/SEOUL_PURPOSE_250M_IN_*.parquet"
# OUTPUT
FINAL   = "/home1/bismarck/transit_seoul/merged_all.parquet"
# 작업 진행중일 때.
TMP     = FINAL + ".tmp"

def log(m): print(m, flush=True)

def main():
    t0 = time.perf_counter()
    cpus = os.environ.get("SLURM_CPUS_PER_TASK")
    if cpus:
        os.environ["POLARS_MAX_THREADS"] = cpus
        log(f"[INFO] threads={cpus}")

    if os.path.exists(TMP):
        os.remove(TMP)

    lf = pl.scan_parquet(PATTERN)

    log("[INFO] streaming write to tmp ...")
    lf.sink_parquet(TMP, compression="lz4", statistics=False)  # 속도 우선
    os.replace(TMP, FINAL)

    log(f"[INFO] done → {FINAL}  elapsed={time.perf_counter()-t0:.1f}s")

if __name__ == "__main__":
    main()

# merged_all.parquet 요약정보

# shape: (5, 52)
# ┌───────────┬──────────┬──────────┬───────────┬───┬─────────────┬─────────────┬───────────┬──────────┐
# │ O_CELL_ID ┆ O_CELL_X ┆ O_CELL_Y ┆ O_CELL_TP ┆ … ┆ FEML_80_CNT ┆ FEML_85_CNT ┆ TOTAL_CNT ┆ ETL_YMD  │
# │ ---       ┆ ---      ┆ ---      ┆ ---       ┆   ┆ ---         ┆ ---         ┆ ---       ┆ ---      │
# │ str       ┆ i64      ┆ i64      ┆ i64       ┆   ┆ f64         ┆ f64         ┆ f64       ┆ i64      │
# ╞═══════════╪══════════╪══════════╪═══════════╪═══╪═════════════╪═════════════╪═══════════╪══════════╡
# │ 43750     ┆ 995125   ┆ 1888125  ┆ 0         ┆ … ┆ 0.0         ┆ 0.0         ┆ 2.03      ┆ 20240601 │
# │ 43750     ┆ 994375   ┆ 1889125  ┆ 0         ┆ … ┆ 0.0         ┆ 0.0         ┆ 2.6       ┆ 20240601 │
# │ 43770     ┆ 996375   ┆ 1887625  ┆ 0         ┆ … ┆ 0.0         ┆ 0.0         ┆ 1.04      ┆ 20240601 │
# │ 43770     ┆ 996625   ┆ 1889125  ┆ 0         ┆ … ┆ 0.0         ┆ 0.0         ┆ 3.5       ┆ 20240601 │
# │ 43770     ┆ 1014875  ┆ 1902875  ┆ 0         ┆ … ┆ 0.0         ┆ 0.0         ┆ 2.06      ┆ 20240601 │
# └───────────┴──────────┴──────────┴───────────┴───┴─────────────┴─────────────┴───────────┴──────────┘
# rows: 2735761353