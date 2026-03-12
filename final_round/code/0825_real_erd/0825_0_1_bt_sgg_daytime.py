# bt_sgg_monthly_inflow.py
# 요구: polars >= 0.20

import time
import polars as pl

# ====== 경로 설정 ======
FACT_FP = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
ADMI_CSV = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"

OUT_PARQUET = "/home1/bismarck/transit_seoul/dataset/total_erd/BT_SGG_monthly_inflow.parquet"
OUT_CSV     = "/home1/bismarck/transit_seoul/dataset/total_erd/BT_SGG_monthly_inflow.csv"  # 불필요하면 None

YM_FROM = "202501"
YM_TO   = "202505"

def main():
    t0 = time.perf_counter()

    # 1) 원본 스캔 (컬럼명 유지)
    fact = (
        pl.scan_parquet(FACT_FP)
          .with_columns(
              pl.col("ETL_YMD").cast(pl.Utf8),
              pl.col("O_ADMI_CD").cast(pl.Utf8),
              pl.col("D_ADMI_CD").cast(pl.Utf8),
          )
          # 목적 필터: 1(출근), 2(등교)
          .filter(pl.col("MOVE_PURPOSE").is_in([1, 2]))
          # 년월 파생 및 기간 필터(YYYYMM)
          .with_columns(pl.col("ETL_YMD").str.slice(0, 6).alias("년월"))
          .filter((pl.col("년월") >= YM_FROM) & (pl.col("년월") <= YM_TO))
          # 출발지 구분(앞 5자리 비교; 7~8자리 혼재 대비 zfill 처리)
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

    # 2) 도착 ADMI → SGG 매핑 (SGG_NM, SGG_RES)
    admi = (
        pl.scan_csv(
            ADMI_CSV,
            ignore_errors=True
        )
        .select(
            pl.col("ADMI_CD").cast(pl.Utf8),
            pl.col("SGG_NM").cast(pl.Utf8),
            pl.col("SGG_RES").cast(pl.Int64),
        )
    )

    fact = fact.join(
        admi,
        left_on="D_ADMI_CD",
        right_on="ADMI_CD",
        how="left"
    )

    # 3) 집계 (원본명 최대 유지: 합계 컬럼도 TOTAL_CNT로)
    out_lf = (
        fact
        .group_by(["년월", "SGG_NM", "SGG_RES", "출발지"])
        .agg(pl.col("TOTAL_CNT").sum().alias("TOTAL_CNT"))
        .sort(["년월", "SGG_NM", "출발지"])
    )

    # 4) 실행/저장/검증
    df = out_lf.collect(streaming=True)
    df.write_parquet(OUT_PARQUET)
    if OUT_CSV:
        df.write_csv(OUT_CSV)

    t1 = time.perf_counter()
    print(f"[DONE] elapsed: {t1 - t0:.2f}s")
    print(f"[SHAPE] {df.shape}")
    print("[HEAD 50]")
    print(df.head(50))

if __name__ == "__main__":
    main()
