# 6. Parquet 스캔 그리고 사용할 데이터 뽑기
import polars as pl

lazy_df = pl.scan_parquet('SEOUL_PURPOSE_250M_IN_202406.parquet', glob=True)

result = (
    lazy_df
    #   # ETL_YMD는 문자열이므로, '20240101'~'20240531' 사이 조건
    # .filter(
    #     (pl.col('ETL_YMD') >= '20240101') &
    #     (pl.col('ETL_YMD') <= '20240531')
    # )
    .group_by(['MOVE_PURPOSE'])
    .agg([
        pl.col('TOTAL_CNT').cast(pl.Float64).mean().alias('avg_cnt'),
    ])
    .collect()
)

print(result)
