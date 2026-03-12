import polars as pl

pl.Config.set_tbl_rows(-1)   # 행 제한 해제
pl.Config.set_tbl_cols(-1)   # 열 제한 해제
pl.Config.set_tbl_width_chars(0)  # 콘솔 폭 제한 해제

OUT_PATH  = "/home1/bismarck/transit_seoul/dataset/total_erd/merged_25_5m_with_admi.parquet"

# ===== 결과 검증 =====
df = pl.read_parquet(OUT_PATH)

# log(f"[INFO] 결과 shape: {df.shape}")

print(df.head(50))

