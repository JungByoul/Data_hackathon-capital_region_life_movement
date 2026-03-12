import os
import time
import polars as pl

# 입력/출력 경로
OD_PATH   = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_opt.parquet"
ADMI_PATH = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"
OUT_PATH  = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"
TMP_PATH  = OUT_PATH + ".tmp"

def log(msg):
    print(msg, flush=True)

def main():
    t0 = time.perf_counter()
    log("[INFO] 데이터 로드 시작")

    # ADMI 파일 (필요한 컬럼만)
    admi = pl.scan_csv(ADMI_PATH).select(["ADMI_CD", "SGG_NM", "ADMI_NM", "FULL_NM"])

    # OD 데이터
    od = pl.scan_parquet(OD_PATH)

    # 출발지 조인 후 prefix 붙이기
    od_with_o = (
        od.join(
            admi,
            left_on="O_ADMI_CD",
            right_on="ADMI_CD",
            how="left"
        )
        .rename({
            "SGG_NM": "O_SGG_NM",
            "ADMI_NM": "O_ADMI_NM",
            "FULL_NM": "O_FULL_NM"
        })
    )

    # 도착지 조인 후 prefix 붙이기
    od_with_od = (
        od_with_o.join(
            admi,
            left_on="D_ADMI_CD",
            right_on="ADMI_CD",
            how="left"
        )
        .rename({
            "SGG_NM": "D_SGG_NM",
            "ADMI_NM": "D_ADMI_NM",
            "FULL_NM": "D_FULL_NM"
        })
    )

    # 저장 (tmp 경유)
    if os.path.exists(TMP_PATH):
        os.remove(TMP_PATH)

    od_with_od.sink_parquet(TMP_PATH, compression="lz4", statistics=False)
    os.replace(TMP_PATH, OUT_PATH)

    elapsed = time.perf_counter() - t0
    log(f"[INFO] 저장 완료 → {OUT_PATH}")
    log(f"[INFO] 총 소요시간: {elapsed:.1f}초 ({elapsed/60:.2f}분)")

    # ===== 결과 검증 =====
    df = pl.read_parquet(OUT_PATH)
    pl.Config.set_tbl_rows(-1)   # 행 제한 해제
    pl.Config.set_tbl_cols(-1)   # 열 제한 해제
    pl.Config.set_tbl_width_chars(0)  # 콘솔 폭 제한 해제


    log(f"[INFO] 결과 shape: {df.shape}")
    log("[INFO] 상위 50행 미리보기:")
    print(df.head(50))

if __name__ == "__main__":
    main()
