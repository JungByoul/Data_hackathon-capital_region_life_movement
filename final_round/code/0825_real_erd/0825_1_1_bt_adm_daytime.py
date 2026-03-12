# bt_admi_daytime.py
# 요구: polars >= 0.20

import time
import polars as pl

# ===== 경로 =====
FACT_FP     = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
ADMI_CSV    = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"  # ADMI_CD, (RESIDENT 또는 ADMI_RES) 포함
OUT_PARQUET = "/home1/bismarck/transit_seoul/dataset/total_erd/BT_ADM_daytime.parquet"
OUT_CSV     = "/home1/bismarck/transit_seoul/dataset/total_erd/BT_ADM_daytime.csv"

YM_FROM = "202501"
YM_TO   = "202505"

def main():
    t0 = time.perf_counter()

    # 1) 원본 스캔
    fact = (
        pl.scan_parquet(FACT_FP)
          .with_columns(
              pl.col("ETL_YMD").cast(pl.Utf8),
              pl.col("O_ADMI_CD").cast(pl.Utf8),
              pl.col("D_ADMI_CD").cast(pl.Utf8),
          )
          # 목적: 통근(1), 등교(2)
          .filter(pl.col("MOVE_PURPOSE").is_in([1, 2]))
          # 년월(YYYYMM) 파생 및 범위 필터
          .with_columns(pl.col("ETL_YMD").str.slice(0, 6).alias("년월"))
          .filter((pl.col("년월") >= YM_FROM) & (pl.col("년월") <= YM_TO))
          # 출발지 구분(동일/다른 시군구)
          .with_columns(
              pl.col("O_ADMI_CD").str.zfill(8).str.slice(0, 5).alias("_O_SGG5"),
              pl.col("D_ADMI_CD").str.zfill(8).str.slice(0, 5).alias("_D_SGG5"),
          )
          .with_columns(
              pl.when(pl.col("_O_SGG5") == pl.col("_D_SGG5"))
                .then(pl.lit("동일 시군구"))
                .otherwise(pl.lit("다른 시군구"))
                .alias("출발지")
          )
    )
    # 2) 도착 ADMI의 상주인구(ADMI_RES) 조인 — 단순화 버전
    admi = (
        pl.scan_csv(ADMI_CSV, ignore_errors=True)
        .select(
            pl.col("ADMI_CD").cast(pl.Utf8),
            pl.col("ADMI_RES").cast(pl.Int64)
        )
    )


    fact = fact.join(admi, left_on="D_ADMI_CD", right_on="ADMI_CD", how="left")

    # 3) 집계 (정수 합계)
    out_lf = (
        fact
        .group_by(["년월", pl.col("D_ADMI_CD").alias("ADMI_CD"), "ADMI_RES", "출발지"])
        .agg(
            pl.col("TOTAL_CNT").sum().round(0).cast(pl.Int64).alias("TOTAL_CNT")
        )
        .select(["년월", "ADMI_CD", "ADMI_RES", "출발지", "TOTAL_CNT"])
        .sort(["년월", "ADMI_CD", "출발지"])
    )

    # 4) 실행/저장/검증 출력
    df = out_lf.collect(streaming=True)
    df.write_parquet(OUT_PARQUET)
    df.write_csv(OUT_CSV)

    t1 = time.perf_counter()
    print(f"[DONE] elapsed: {t1 - t0:.2f}s")
    print(f"[SHAPE] {df.shape}")
    print("[HEAD 50]")
    print(df.head(50))

if __name__ == "__main__":
    main()
