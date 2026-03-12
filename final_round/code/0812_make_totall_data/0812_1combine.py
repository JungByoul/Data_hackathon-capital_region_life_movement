"""
0812_1combine.py
--------------------
기능:
  - 2024년 6월 ~ 2025년 5월 기간의 월별 원본 Parquet 데이터(SEOUL_PURPOSE_250M_IN_*.parquet)를
    모두 읽어 하나의 통합본(merged_all.parquet)으로 생성한다.
  - 데이터는 폴더 구조(year/month 파티션)에서 glob 패턴으로 수집.
  - Polars LazyFrame + streaming sink를 사용하여 대용량 데이터도 메모리 효율적으로 처리.
  - 임시 파일(.tmp)에 먼저 저장한 후 원자적 교체로 최종본을 생성.

주요 처리 단계:
  1. 입력 경로 패턴 설정(PATTERN) 및 출력 경로 지정(FINAL)
  2. Polars scan_parquet으로 LazyFrame 생성
  3. streaming 방식으로 압축(lz4) 저장
  4. 처리 시간과 스레드 수(환경 변수) 로그 출력
결과: 약 60GB의 데이터 생성(약 20억행)
"""

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