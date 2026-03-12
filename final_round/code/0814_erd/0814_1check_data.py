import polars as pl

IN = "/home1/bismarck/transit_seoul/dataset/merged_all_admi.parquet"
# 옵션 변경: 열 전체 표시
pl.Config.set_tbl_cols(-1)  # -1이면 모든 컬럼 출력
# pl.Config.set_tbl_rows(5)   # 표시할 행 수
pl.Config.set_tbl_width_chars(0)  # 터미널 가로폭 제한 해제

print(pl.read_parquet(IN, n_rows=10))

