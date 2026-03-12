"""
이거 인풋데이터 내부 내용이 바뀌어서 다시돌려야함!!@!! 파일명은 그대로임

0812_5totally_comb_opt.py
--------------------------------
기능:
  - ADMI 정보가 포함되고 목적 컬럼이 수정된 통합 데이터(merged_all_admi.parquet)에서
    좌표 관련 컬럼과 60세 이상 남성·여성 인구수 컬럼을 제거하여
    최적화된 데이터셋(merged_all_admi_opt.parquet)으로 저장한다.
  - 좌표 관련: O_CELL_*, D_CELL_* 전부 제거
  - 연령 관련: MALE/FEML 60, 65, 70, 75, 80, 85세 카운트 컬럼 제거
  - TOTAL_CNT 등 전체 집계값은 그대로 유지

특징:
  - Polars LazyFrame + streaming sink를 사용하여 대용량 데이터도 메모리 효율적으로 처리
  - 임시 파일(.tmp)에 저장 후 원자적 교체로 안전하게 결과물 생성
  - 처리 시간(초·분 단위)을 로그로 출력

주요 처리 단계:
  1. 제거할 좌표 및 연령 컬럼 목록 정의
  2. LazyFrame으로 원본 스캔 후 지정 컬럼 제외
  3. streaming sink로 압축(lz4) 저장
  4. 최종 저장 후 소요 시간 로그 출력
"""

import os
import time
import polars as pl

IN  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi.parquet"  # ADMI 붙인 최종본
OUT = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_opt.parquet"
TMP = OUT + ".tmp"

coord_patterns = ["^O_CELL_.*$", "^D_CELL_.*$"]  # 좌표 관련 컬럼 전부 제거
senior_cols = [  
    "MALE_00_CNT","MALE_40_CNT","MALE_45_CNT","MALE_50_CNT","MALE_55_CNT","MALE_60_CNT","MALE_65_CNT","MALE_70_CNT","MALE_75_CNT","MALE_80_CNT","MALE_85_CNT",
    "FEML_00_CNT","FEML_40_CNT","FEML_45_CNT","FEML_50_CNT","FEML_55_CNT","FEML_60_CNT","FEML_65_CNT","FEML_70_CNT","FEML_75_CNT","FEML_80_CNT","FEML_85_CNT",
] #안쓰는 나이대 컬럼 전부 제거

def log(msg):
    print(msg, flush=True)

def main():
    t0 = time.perf_counter()
    log("[INFO] 데이터 로드 및 컬럼 제거 시작")

    lf = (
        pl.scan_parquet(IN)
          .select(pl.all().exclude(*coord_patterns).exclude(senior_cols))
    )

    if os.path.exists(TMP):
        os.remove(TMP)
    lf.sink_parquet(TMP, compression="lz4", statistics=False)
    os.replace(TMP, OUT)

    elapsed = time.perf_counter() - t0
    log(f"[INFO] 저장 완료 → {OUT}")
    log(f"[INFO] 총 소요시간: {elapsed:.1f}초 ({elapsed/60:.2f}분)")

if __name__ == "__main__":
    main()
