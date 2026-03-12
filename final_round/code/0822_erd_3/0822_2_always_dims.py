


# -*- coding: utf-8 -*-
# 0822_2_always_dims.py : 항상 생성되는 차원 테이블 만들기
import pathlib, polars as pl

# ===== 경로 설정 (여기만 고치면 됩니다) =====
IN_PATH  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_select.parquet"   # 또는 .csv
OUT_DIR  = "/home1/bismarck/transit_seoul/dataset/erd"                 # 출력 폴더

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# always_dims.py : 항상 생성되는 차원 테이블 만들기
import pathlib
import polars as pl

# ===== 입출력 준비 =====
p = IN_PATH.lower()
if p.endswith(".parquet"):
    lf = pl.scan_parquet(IN_PATH)
elif p.endswith(".csv"):
    lf = pl.scan_csv(IN_PATH, infer_schema_length=5000)
else:
    raise ValueError("입력은 .parquet 또는 .csv 여야 합니다.")

out = pathlib.Path(OUT_DIR)
out.mkdir(parents=True, exist_ok=True)

# ===== dim_time_slot (00~23 + 07:00~09:40/17:00~19:40 20분 간격 전부) =====
# 1) 기본 1시간 단위 슬롯 (단, 07~09, 17~19는 제외)
hours = [(f"{h:02d}", h * 60) for h in range(24) if h not in [7, 8, 9, 17, 18, 19]]

# 2) 출퇴근 시간대 20분 단위 슬롯
m20 = list(range(7 * 60, 9 * 60 + 41, 20)) + list(range(17 * 60, 19 * 60 + 41, 20))
special = [(f"{m // 60:02d}{m % 60:02d}", m) for m in m20]

# 3) 합치기
dim_time_slot = (
    pl.DataFrame(hours + special, schema=["time_cd", "minute_of_day"], orient="row")
      .with_columns([
          pl.when(pl.col("time_cd").str.len_chars() == 2)  # 1시간 단위 슬롯
            .then(pl.format("{}:00–{}:59", pl.col("time_cd"), pl.col("time_cd")))
            .otherwise(  # 20분 단위 슬롯
                pl.col("time_cd").str.slice(0, 2) + ":" + pl.col("time_cd").str.slice(2, 2)
            )
            .alias("label"),
          pl.when(pl.col("minute_of_day").is_between(420, 580))  # 07:00~09:40
            .then(pl.lit("morning"))
           .when(pl.col("minute_of_day").is_between(1020, 1180))  # 17:00~19:40
            .then(pl.lit("evening"))
           .otherwise(pl.lit("etc"))
           .alias("bucket"),
          (
              pl.col("minute_of_day").is_between(420, 580) |
              pl.col("minute_of_day").is_between(1020, 1180)
          ).alias("commute_flag"),
      ])
      .select(["time_cd", "label", "minute_of_day", "bucket", "commute_flag"])
      .sort("minute_of_day")
)

dim_time_slot.write_parquet(out / "dim_time_slot.parquet")


# # ===== dim_time_slot (00~23 + 07:00~09:40/17:00~19:40 20분 간격 전부) =====
# hours = [(f"{h:02d}", h * 60) for h in range(24)]
# m20   = list(range(7 * 60,  9 * 60 + 40, 20)) + list(range(17 * 60, 19 * 60 + 40, 20))
# special = [(f"{m // 60:02d}{m % 60:02d}", m) for m in m20]

# dim_time_slot = (
#     pl.DataFrame(hours + special, schema=["time_cd", "minute_of_day"], orient="row")
#       .unique("time_cd")
#       .with_columns([
#           pl.when(pl.col("time_cd").str.len_chars() == 2)
#             .then(pl.format("{}:00–{}:59", pl.col("time_cd"), pl.col("time_cd")))
#             .otherwise(pl.col("time_cd").str.slice(0, 2) + ":" + pl.col("time_cd").str.slice(2, 2))
#             .alias("label"),
#           pl.when(pl.col("minute_of_day").is_between(420, 580)).then(pl.lit("morning"))
#            .when(pl.col("minute_of_day").is_between(1020, 1180)).then(pl.lit("evening"))
#            .otherwise(pl.lit("etc")).alias("bucket"),
#           (pl.col("minute_of_day").is_between(420, 580) |
#            pl.col("minute_of_day").is_between(1020, 1180)).alias("commute_flag"),
#       ])
#       .select(["time_cd", "label", "minute_of_day", "bucket", "commute_flag"])
#       .sort("minute_of_day")
# )
# dim_time_slot.write_parquet(out / "dim_time_slot.parquet")

# ===== dim_date (날짜 차원 테이블 생성) =====
import datetime

# 1) 날짜 범위 직접 생성 (예: 2024-06-01 ~ 2025-12-31)
start = datetime.date(2024, 6, 1)
end   = datetime.date(2025, 5, 31)
date_range = pl.date_range(start, end, interval="1d", eager=True).alias("date")

# 2) date_id 및 파생 컬럼 생성
dim_date = (
    pl.DataFrame({"date": date_range})
      .with_columns([
          pl.col("date").dt.strftime("%Y%m%d").alias("date_id"),
          pl.col("date").dt.year().alias("year"),
          pl.col("date").dt.month().alias("month"),
      ])
      .with_columns([
          (pl.col("year").cast(pl.Utf8) + pl.col("month").cast(pl.Utf8).str.zfill(2)).alias("ym"),
      ])
      .with_columns([
          pl.col("date").dt.weekday().alias("weekday"),
          pl.col("date").dt.weekday().is_in([5, 6]).alias("is_weekend"),
          pl.lit(False).alias("is_holiday"),
          pl.col("ym").alias("subway_ym"),
      ])
      .select(["date_id", "year", "month", "ym", "weekday", "is_weekend", "is_holiday", "subway_ym"])
      .sort("date_id")
)

# 3) 저장
dim_date.write_parquet(out / "dim_date.parquet")


# ===== dim_admin (출발/도착 합집합) =====
parts = []
for prefix in ["O", "D"]:
    cols = [f"{prefix}_ADMI_CD", f"{prefix}_SIDO_NM", f"{prefix}_SGG_NM", f"{prefix}_ADMI_NM", f"{prefix}_FULL_NM"]
    exist = [c for c in cols if c in lf.columns]
    if f"{prefix}_ADMI_CD" not in exist:
        continue
    part = (
        lf.select(exist).unique()
          .with_columns([
              pl.col(f"{prefix}_ADMI_CD").cast(pl.Utf8).alias("admdong_cd"),
              (pl.col(f"{prefix}_SIDO_NM").cast(pl.Utf8) if f"{prefix}_SIDO_NM" in exist else pl.lit(None).cast(pl.Utf8)).alias("sido_nm"),
              (pl.col(f"{prefix}_SGG_NM" ).cast(pl.Utf8) if f"{prefix}_SGG_NM"  in exist else pl.lit(None).cast(pl.Utf8)).alias("sgg_nm"),
              (pl.col(f"{prefix}_ADMI_NM").cast(pl.Utf8) if f"{prefix}_ADMI_NM" in exist else pl.lit(None).cast(pl.Utf8)).alias("adm_nm"),
              (pl.col(f"{prefix}_FULL_NM").cast(pl.Utf8) if f"{prefix}_FULL_NM" in exist else pl.lit(None).cast(pl.Utf8)).alias("full_nm"),
          ])
          .select(["admdong_cd", "sido_nm", "sgg_nm", "adm_nm", "full_nm"])
    )
    parts.append(part)

dim_admin = (
    pl.concat(parts, how="vertical").unique("admdong_cd").collect()
    if parts else pl.DataFrame({"admdong_cd": [], "sido_nm": [], "sgg_nm": [], "adm_nm": [], "full_nm": []})
)
dim_admin.write_parquet(out / "dim_admin.parquet")

print("[DONE] always dims →", out.resolve())