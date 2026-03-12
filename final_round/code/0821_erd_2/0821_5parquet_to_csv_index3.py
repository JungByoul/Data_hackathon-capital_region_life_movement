import polars as pl

IN  = "/home1/bismarck/transit_seoul/dataset/home_arrivals_17to23_by_dest.parquet"
OUT = "/home1/bismarck/transit_seoul/dataset/home_arrivals_17to23_by_dest.csv"

# parquet 읽고 csv로 저장
df = pl.read_parquet(IN)
df.write_csv(OUT)

print(f"[INFO] CSV 저장 완료 → {OUT}")
print(df.head(10))   # 변환 후 상위 10행 확인
