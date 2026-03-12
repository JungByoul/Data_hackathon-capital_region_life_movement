import polars as pl

OUT = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_select.parquet"

df = pl.read_parquet(OUT)

pl.Config.set_tbl_cols(-1)  # -1이면 모든 컬럼 출력
# pl.Config.set_tbl_rows(5)   # 표시할 행 수
pl.Config.set_tbl_width_chars(0)  # 터미널 가로폭 제한 해제


print(df.shape)   # (행 수, 열 수)
print(df.height)  # 행 수만
print(df.width)   # 열 수만

print(pl.read_parquet(OUT, n_rows=100))
