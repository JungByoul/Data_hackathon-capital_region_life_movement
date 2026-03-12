import polars as pl

p = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/LATEST_ADMI_CODES.parquet"

# 열 개수: 헤더만 읽기
n_cols = len(pl.read_parquet(p, n_rows=0).columns)

# 행 개수: lazy로 길이 계산
n_rows = pl.scan_parquet(p).select(pl.len()).collect().item()

print(f"shape = ({n_rows}, {n_cols})")
