import polars as pl
"""
경기도의 O_SGG_NM 확인. 서울 인천은 '연수구'이렇게 되어있는데,
경기도는 '수원시 팔달구'인지, '수원시_팔달구'인지 확인해야함.

=> 결론 '수원시 팔달구'이렇게 되어 있음.
"""


IN = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_opt.parquet"

# lazy + streaming으로 메모리 부담 최소화
lf = pl.scan_parquet(IN)

vals = (
    lf.select(pl.col("O_SGG_NM").unique().sort())
      .collect()
      .to_series()
      .to_list()
)
t = tuple(vals)
print(t)          # 생략 없이 파이썬 튜플로 출력
print(len(t))     # 유니크 개수 확인
