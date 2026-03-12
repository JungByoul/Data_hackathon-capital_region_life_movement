"""
0812_4totally_combine_purpose.py
------------------------
기능:
  - 2단계 통합 파일(merged_all_admi.parquet)에 MOVE_PURPOSE 숫자 코드를
    사람이 읽는 텍스트 컬럼(MOVE_PURPOSE_NM)으로 매핑해 추가하고, 원자적 교체로 저장한다.

매핑 규칙:
  1: 주간상주지
  2: 야간상주지
  3: 근무지
  4: 쇼핑
  5: 병원
  6: 학교
  7: 기타
  ※ 그 외/결측 값은 null 처리

특징:
  - Polars LazyFrame + streaming sink로 대용량도 메모리 효율적으로 처리
  - 임시 파일(.tmp) → os.replace()로 원자적 저장
    ->이렇게 해야 중간중간 저장데이터 용량 확인가능.
  - 처리 시간(초/분) 로그 출력

주요 단계:
  1) 입력 파일 스캔
  2) MOVE_PURPOSE를 Int64로 캐스팅 후 when/then/otherwise로 이름 매핑
  3) tmp에 저장 후 최종 파일로 교체

"""

import os, time
import polars as pl

INOUT = "/home1/bismarck/transit_seoul/dataset/merged_all_admi.parquet"  # 2단계 파일
TMP    = INOUT + ".tmp"

def log(m): print(m, flush=True)

def main():
    t0 = time.perf_counter()
    log("[INFO] MOVE_PURPOSE → MOVE_PURPOSE_NM 매핑 시작")

    lf = (
        pl.scan_parquet(INOUT)
          .with_columns([
              # 숫자 목적코드 → 한글 목적명
              pl.when(pl.col("MOVE_PURPOSE").cast(pl.Int64) == 1).then(pl.lit("주간상주지"))
               .when(pl.col("MOVE_PURPOSE").cast(pl.Int64) == 2).then(pl.lit("야간상주지"))
               .when(pl.col("MOVE_PURPOSE").cast(pl.Int64) == 3).then(pl.lit("근무지"))
               .when(pl.col("MOVE_PURPOSE").cast(pl.Int64) == 4).then(pl.lit("쇼핑"))
               .when(pl.col("MOVE_PURPOSE").cast(pl.Int64) == 5).then(pl.lit("병원"))
               .when(pl.col("MOVE_PURPOSE").cast(pl.Int64) == 6).then(pl.lit("학교"))
               .when(pl.col("MOVE_PURPOSE").cast(pl.Int64) == 7).then(pl.lit("기타"))
               .otherwise(pl.lit(None))
               .alias("MOVE_PURPOSE_NM")
          ])
    )

    if os.path.exists(TMP):
        os.remove(TMP)
    lf.sink_parquet(TMP, compression="lz4", statistics=False)
    os.replace(TMP, INOUT)

    dt = time.perf_counter() - t0
    log(f"[INFO] 저장 완료 → {INOUT}")
    log(f"[INFO] 소요시간: {dt:.1f}s ({dt/60:.2f}m)")

if __name__ == "__main__":
    main()
