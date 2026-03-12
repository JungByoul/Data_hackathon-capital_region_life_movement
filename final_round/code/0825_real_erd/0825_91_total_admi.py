import pandas as pd

csv_path = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.csv"
df = pd.read_csv(csv_path)

parquet_path = "/home1/bismarck/transit_seoul/dataset/ADMI_RE/Total_ADMI.parquet"
df.to_parquet(parquet_path, engine="pyarrow", index=False)
