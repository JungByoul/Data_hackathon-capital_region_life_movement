"""
3번 지표인 '퇴근 시간 귀가인구비율' 데이터 생성 (디버깅 로그 포함)

출력 스펙
- 행 단위: 날짜(ETL_YMD) × 도착시간대(FNS_TIME_CD) × 도착 행정동(D_ADMI_NM/D_FULL_NM)
- 값: 귀가 목적 합계(TOTAL_CNT의 합)
- 시간대: 17:00 ~ 23:00 (17:00~19:40은 20분 간격 코드, 20~23은 정수코드)
- 비율 계산 없음 (태블로에서 수행)
"""

import os
import time
import polars as pl

# 콘솔 출력 옵션
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(0)

IN  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_select.parquet"
OUT = "/home1/bismarck/transit_seoul/dataset/home_arrivals_17to23_by_dest.parquet"
TMP = OUT + ".tmp"

def log(msg: str): 
    print(msg, flush=True)

def time_mask_expr(code_col: str = "FNS_TIME_CD") -> pl.Expr:
    c = pl.col(code_col)
    mask_17_19 = c.is_in([1700, 1720, 1740, 1800, 1820, 1840, 1900, 1920, 1940])
    mask_20_23 = c.is_in([20, 21, 22, 23])
    return mask_17_19 | mask_20_23

def main():
    t0 = time.perf_counter()
    log("[INFO] 로드 시작")

    NEEDS = [
        "ETL_YMD", "FNS_TIME_CD",
        "MOVE_PURPOSE",
        "TOTAL_CNT",
        "D_SIDO_NM", "D_SGG_NM", "D_ADMI_NM", "D_FULL_NM",
    ]

    # 0) 기본 로드 + 타입 방어
    base_lf = (
        pl.scan_parquet(IN)
          .select(NEEDS)
          .with_columns([
              pl.col("FNS_TIME_CD").cast(pl.Int64, strict=False),
              pl.col("TOTAL_CNT").cast(pl.Int64, strict=False),
          ])
    )

    # === 체크포인트 #0: 기본 현황 ===
    log("[DBG#0] 스키마")
    print(base_lf.schema)

    log("[DBG#0] 샘플(상위 10)")
    print(base_lf.head(10).collect())

    log("[DBG#0] 전체 행 수")
    print(base_lf.select(pl.len()).collect().item())

    log("[DBG#0] FNS_TIME_CD 유니크(최대 50개)")
    print(base_lf.select(pl.col("FNS_TIME_CD").unique().sort().head(50)).collect())

    log("[DBG#0] MOVE_PURPOSE 유니크 및 개수 Top 10")
    print(
        base_lf
          .group_by("MOVE_PURPOSE")
          .agg(pl.len().alias("cnt"))
          .sort("cnt", descending=True)
          .head(10)
          .collect()
    )

    # 1) 목적 필터
    home_lf = base_lf.filter(pl.col("MOVE_PURPOSE") == 3)

    # === 체크포인트 #1: 목적 필터 후 ===
    log("[DBG#1] 목적=귀가 행 수")
    print(home_lf.select(pl.len()).collect().item())

    log("[DBG#1] 목적=귀가 샘플(상위 10)")
    print(home_lf.head(10).collect())

    log("[DBG#1] 목적=귀가의 FNS_TIME_CD 유니크(최대 50개)")
    print(home_lf.select(pl.col("FNS_TIME_CD").unique().sort().head(50)).collect())

    # 2) 시간대 필터
    time_filtered_lf = home_lf.filter(time_mask_expr("FNS_TIME_CD"))

    # === 체크포인트 #2: 시간대 필터 후 ===
    log("[DBG#2] 시간대(17:00~19:40 + 20~23) 필터 후 행 수")
    print(time_filtered_lf.select(pl.len()).collect().item())

    log("[DBG#2] 시간대 필터 후 샘플(상위 10)")
    print(time_filtered_lf.head(10).collect())

    log("[DBG#2] 시간대 필터 후 FNS_TIME_CD 유니크")
    print(time_filtered_lf.select(pl.col("FNS_TIME_CD").unique().sort()).collect())

    # 3) 집계
    agg_lf = (
        time_filtered_lf
        .group_by(["ETL_YMD", "FNS_TIME_CD", "D_SIDO_NM", "D_SGG_NM", "D_ADMI_NM", "D_FULL_NM"])
        .agg(pl.sum("TOTAL_CNT").alias("HOME_ARRIVAL_CNT"))
        .sort(["ETL_YMD", "FNS_TIME_CD", "D_SIDO_NM", "D_SGG_NM", "D_ADMI_NM"])
    )

    # === 체크포인트 #3: 집계 후 미리보기 ===
    log("[DBG#3] 집계 결과 샘플(상위 10)")
    print(agg_lf.head(10).collect())

    # 저장
    if os.path.exists(TMP):
        os.remove(TMP)
    agg_lf.sink_parquet(TMP, compression="lz4", statistics=False)
    os.replace(TMP, OUT)

    elapsed = time.perf_counter() - t0
    log(f"[INFO] 저장 완료 → {OUT}")
    log(f"[INFO] 총 소요시간: {elapsed:.1f}초 ({elapsed/60:.2f}분)")

    # 최종 파일 미리보기
    log("[INFO] 최종 결과 상위 10행")
    print(pl.read_parquet(OUT, n_rows=10))

if __name__ == "__main__":
    main()

# 젠장 movepurpose_nm 이 이상하게 되어있음.