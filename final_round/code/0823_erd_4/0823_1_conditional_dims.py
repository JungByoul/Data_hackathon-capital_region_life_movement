#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# conditional_tables.py : 조건부 생성 테이블 (목적/팩트/QA)
import re
import pathlib
import polars as pl

# ===== 경로 설정 =====
IN_PATH  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_select.parquet"
OUT_DIR  = "/home1/bismarck/transit_seoul/dataset/erd"
TS_PATH  = "/home1/bismarck/transit_seoul/dataset/erd/dim_time_slot.parquet"  # 36 슬롯(시 24 + 피크 20분*12)

# 빈 DF 저장 여부
ALWAYS_WRITE_EMPTY = False

# 목적코드 매핑 (고정)
PURPOSE_MAP = pl.DataFrame({
    "purpose_cd": [1, 2, 3, 4, 5, 6, 7],
    "purpose_nm": ["출근", "등교", "귀가", "쇼핑", "관광", "병원", "기타"]
}).with_columns(pl.col("purpose_cd").cast(pl.Int8))

# 상단 import 아래에 추가
DEBUG = True

#디버깅 함수추가
def dbg(label: str, lf: pl.LazyFrame | None = None, cols: list[str] | None = None, n: int = 5):
    print(f"\n[DEBUG] {label}")
    if lf is None:
        return
    try:
        print("  schema:", lf.collect_schema().names())
    except Exception as e:
        print("  (schema) err:", e)
    if cols:
        try:
            peek_df = lf.select([pl.col(c) for c in cols]).limit(n).collect()
            print(peek_df)
        except Exception as e:
            print("  (peek) err:", e)


# 공통 IO
def read_lazy(path: str) -> pl.LazyFrame:
    p = path.lower()
    if p.endswith(".parquet"):
        return pl.scan_parquet(path)
    if p.endswith(".csv"):
        return pl.scan_csv(path, infer_schema_length=5000)
    raise ValueError("입력은 .parquet 또는 .csv 여야 합니다.")

def wp(df: pl.DataFrame, path: pathlib.Path):
    if df is None:
        return
    if (not ALWAYS_WRITE_EMPTY) and df.is_empty():
        return
    df.write_parquet(path)

# ===== HH/HHMM 정규화: dim_time_slot.time_cd 와 1:1 일치시키기 =====
# - "9"   -> "09"
# - "23"  -> "23"
# - "940" -> "0940"
# - "0940" 그대로
def to_dim_timecd(expr: pl.Expr) -> pl.Expr:
    s = expr.cast(pl.Utf8).str.replace_all(r"^\s+|\s+$", "")  # 앞뒤 공백 제거
    digits = s.str.replace_all(r"\D", "")                     # 숫자만 남김
    return (
        pl.when(digits.str.contains(r"^\d$")).then(pl.concat_str([pl.lit("0"), digits]))   # 1자리 → 앞에 0
        .when(digits.str.contains(r"^\d{2}$")).then(digits)                                # 2자리(HH)
        .when(digits.str.contains(r"^\d{3}$")).then(pl.concat_str([pl.lit("0"), digits]))  # 3자리 → 앞에 0
        .when(digits.str.contains(r"^\d{4}$")).then(digits)                                # 4자리(HHMM)
        .otherwise(pl.lit(None))
    )

def main():
    lf = read_lazy(IN_PATH) #기존에는 싹다 불러옴
    
    # # ✅ 변경: 처음부터 필요한 컬럼만
    # need_cols = [
    #     "ETL_YMD","O_ADMI_CD","D_ADMI_CD",
    #     "ST_TIME_CD","FNS_TIME_CD",
    #     "MOVE_PURPOSE", "TOTAL_CNT", "MOVE_PURPOSE", "MOVE_TIME",
    #     # 데모 컬럼들 자동탐지 후 추가
    # ]
    # lf = pl.scan_parquet(IN_PATH).select([c for c in need_cols if c in pl.scan_parquet(IN_PATH).collect_schema().names()])
        
    out = pathlib.Path(OUT_DIR); out.mkdir(parents=True, exist_ok=True)

    # 한 번만 스키마 확정해서 경고 방지
    all_cols = set(lf.collect_schema().names())

    # ===== 시간 차원(36슬롯) =====
    dim_ts = pl.read_parquet(TS_PATH)

    # 슬롯 길이 파생: HHMM(정규식 ^\d{4}$) → 20분, HH(그 외) → 60분  ← 버전 호환(정규식)
    ts_with_len = (
        dim_ts.lazy()
        .with_columns([
            pl.when(pl.col("time_cd").cast(pl.Utf8).str.contains(r"^\d{4}$")).then(20).otherwise(60)
              .cast(pl.Int16).alias("slot_len")
        ])
    )

    # 시작/종료용 조인 테이블
    ts_st = ts_with_len.select([
        pl.col("time_cd").alias("st_code"),
        pl.col("minute_of_day").alias("st_min"),
        pl.col("slot_len").alias("st_slot_len"),
    ])
    ts_fn = ts_with_len.select([
        pl.col("time_cd").alias("fn_code"),
        pl.col("minute_of_day").alias("fn_min"),
        pl.col("slot_len").alias("fn_slot_len"),
    ])

    # ===== dim_move_purpose =====
    if "MOVE_PURPOSE" in all_cols:
        codes = (
            lf.select(pl.col("MOVE_PURPOSE").cast(pl.Int8).alias("purpose_cd"))
              .drop_nulls()
              .unique()
              .collect()
        )
        dim_move_purpose = (
            codes.join(PURPOSE_MAP, on="purpose_cd", how="left")
                 .sort("purpose_cd")
        )
        wp(dim_move_purpose, out / "dim_move_purpose.parquet")
        wp(dim_move_purpose.filter(pl.col("purpose_nm").is_null()),
           out / "qa_unknown_purpose.parquet")

    # ===== 입력 전처리 (원 컬럼명 유지: ETL_YMD, ST_TIME_CD, FNS_TIME_CD) =====
    has_move = "MOVE_PURPOSE" in all_cols
    base = (
        lf.with_columns([
            pl.col("ETL_YMD").cast(pl.Utf8),   # 이름 변경 없음
            pl.col("O_ADMI_CD").cast(pl.Utf8),
            pl.col("D_ADMI_CD").cast(pl.Utf8),
            pl.col("ST_TIME_CD").cast(pl.Utf8),
            pl.col("FNS_TIME_CD").cast(pl.Utf8),
            (pl.col("MOVE_PURPOSE").cast(pl.Int8) if has_move else pl.lit(None).cast(pl.Int8)).alias("purpose_cd"),
        ])
        .with_columns([
            to_dim_timecd(pl.col("ST_TIME_CD")).alias("st_norm"),   # dim과 일치하는 코드
            to_dim_timecd(pl.col("FNS_TIME_CD")).alias("fn_norm"),
        ])
    )

    # ===== 시간 매핑(조인만) =====
    j0 = (
        base
        .join(ts_st, left_on="st_norm", right_on="st_code", how="left")
        .join(ts_fn, left_on="fn_norm", right_on="fn_code", how="left")
    )
    
    dbg(
    "after join j0",
    j0,
    cols=[
        "ST_TIME_CD","FNS_TIME_CD","st_norm","fn_norm",
        "st_min","fn_min","st_slot_len","fn_slot_len"
    ],
    n=50
    )

    # ===== 시간 QA (사유 분류) — 2단계로 분리 =====
    # (1) 먼저 raw_diff 생성
    j_q1 = j0.with_columns([
        (pl.col("fn_min") - pl.col("st_min")).alias("raw_diff")
    ])
    dbg("QA: after raw_diff", j_q1, ["st_norm","fn_norm","st_min","fn_min","raw_diff"], 50)

    # (2) raw_diff를 이용해 slot_minutes 생성
    j_q2 = j_q1.with_columns([
        pl.when(pl.col("st_norm") == pl.col("fn_norm"))
        .then(pl.col("st_slot_len"))          # 같은 코드면 슬롯길이(20/60)
        .otherwise(pl.col("raw_diff"))         # 다른 코드면 시각차(분)
        .alias("slot_minutes")
    ])
    dbg("QA: after slot_minutes", j_q2, ["st_norm","fn_norm","st_slot_len","raw_diff","slot_minutes"], 50)

    qa_invalid_time = (
        j_q2
        .with_columns([
            pl.when(pl.col("st_norm").is_null()).then(pl.lit("invalid_start_code"))
            .when(pl.col("fn_norm").is_null()).then(pl.lit("invalid_finish_code"))
            .when(pl.col("st_min").is_null()).then(pl.lit("unmapped_start_time"))
            .when(pl.col("fn_min").is_null()).then(pl.lit("unmapped_finish_time"))
            # 자정 넘김/역전: 서로 다른 코드인데 (fn - st) <= 0
            .when((pl.col("st_norm") != pl.col("fn_norm")) & (pl.col("raw_diff") <= 0))
            .then(pl.lit("non_positive_interval"))
            .otherwise(pl.lit("valid"))
            .alias("reason")
        ])
        .filter(pl.col("reason") != "valid")
        .select([
            "ETL_YMD","O_ADMI_CD","D_ADMI_CD",
            "ST_TIME_CD","FNS_TIME_CD","st_norm","fn_norm",
            "purpose_cd","st_min","fn_min","raw_diff","slot_minutes","reason"
        ])
        .collect()
    )
    print(f"[DEBUG] qa_invalid_time rows: {qa_invalid_time.height}")
    wp(qa_invalid_time, out / "qa_invalid_time.parquet")


    # # ===== 시간 QA (사유 분류) — 각자 필요한 파생을 '여기서' 계산 =====
    # qa_invalid_time = (
    #     j0
    #     .with_columns([
    #         (pl.col("fn_min") - pl.col("st_min")).alias("raw_diff"),  # 분단위 차이
    #         # 동일 코드면 슬롯길이(20/60), 다르면 raw_diff
    #         pl.when(pl.col("st_norm") == pl.col("fn_norm"))
    #           .then(pl.col("st_slot_len"))
    #           .otherwise(pl.col("raw_diff"))
    #           .alias("slot_minutes"),
    #     ])
    #     .with_columns([
    #         pl.when(pl.col("st_norm").is_null()).then(pl.lit("invalid_start_code"))      # 숫자화/정규화 실패
    #          .when(pl.col("fn_norm").is_null()).then(pl.lit("invalid_finish_code"))
    #          .when(pl.col("st_min").is_null()).then(pl.lit("unmapped_start_time"))       # dim에 없음
    #          .when(pl.col("fn_min").is_null()).then(pl.lit("unmapped_finish_time"))
    #          # 자정 넘김/역전: 서로 다른 코드인데 (fn - st) <= 0
    #          .when((pl.col("st_norm") != pl.col("fn_norm")) & (pl.col("raw_diff") <= 0))
    #              .then(pl.lit("non_positive_interval"))
    #          .otherwise(pl.lit("valid"))
    #          .alias("reason")
    #     ])
    #     .filter(pl.col("reason") != "valid")
    #     .select([
    #         "ETL_YMD","O_ADMI_CD","D_ADMI_CD",
    #         "ST_TIME_CD","FNS_TIME_CD","st_norm","fn_norm",
    #         "purpose_cd","st_min","fn_min","raw_diff","slot_minutes","reason"
    #     ])
    #     .collect()
    # )
    # wp(qa_invalid_time, out / "qa_invalid_time.parquet")

    # ===== 데모/수치 컬럼 탐지 & 캐스팅 =====
    all_cols_list = list(all_cols)
    male_cols = sorted([c for c in all_cols_list if re.fullmatch(r"MALE_(\d{2})_CNT", c or "")],
                       key=lambda x: int(re.findall(r"(\d{2})", x)[0]))
    feml_cols = sorted([c for c in all_cols_list if re.fullmatch(r"FEML_(\d{2})_CNT", c or "")],
                       key=lambda x: int(re.findall(r"(\d{2})", x)[0]))
    num_cols  = [c for c in ["TOTAL_CNT", "MOVE_DIST", "MOVE_TIME"] + male_cols + feml_cols if c in all_cols]

    # ===== 정상 팩트 — 2단계로 분리 (raw_diff → slot_minutes) =====
    # (1) raw_diff
    f1 = j0.with_columns([
        (pl.col("fn_min") - pl.col("st_min")).alias("raw_diff")
    ])
    dbg("FACT: after raw_diff", f1, ["st_norm","fn_norm","st_min","fn_min","raw_diff"], 5)

    # (2) slot_minutes
    f2 = f1.with_columns([
        pl.when(pl.col("st_norm") == pl.col("fn_norm"))
        .then(pl.col("st_slot_len"))
        .otherwise(pl.col("raw_diff"))
        .alias("slot_minutes")
    ])
    dbg("FACT: after slot_minutes", f2, ["st_norm","fn_norm","st_slot_len","raw_diff","slot_minutes"], 5)


    # # ===== 정상 팩트 — 여기서도 파생 다시 계산 (의존성 제거) =====
    id_cols_for_key = ["ETL_YMD","O_ADMI_CD","D_ADMI_CD","st_norm","fn_norm","purpose_cd"]

    fact_pre = (
    f2
    .filter(pl.col("slot_minutes") > 0)  # 23→23은 60/20으로 양수, 23→00은 음수로 걸러짐
    .with_columns([pl.col(c).cast(pl.Float64).fill_null(0.0) for c in num_cols])
    .with_columns([
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("NA") for c in id_cols_for_key],
            separator="|"
        ).alias("flow_key")
    ])
    .with_columns(pl.col("flow_key").hash().cast(pl.UInt64).alias("flow_id"))
    )

    # # ===== 데모합 vs TOTAL_CNT QA =====
    # TOL_ABS = 1e-3
    # TOL_REL = 0.01
    # demo_sum = pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in (male_cols + feml_cols)])

    # fact_qa = (
    #     fact_pre.with_columns([
    #         demo_sum.alias("sum_demo_cnt"),
    #         (pl.col("TOTAL_CNT") - demo_sum).alias("diff_cnt"),
    #         pl.when(pl.col("TOTAL_CNT") > 0)
    #           .then(pl.col("diff_cnt") / pl.col("TOTAL_CNT"))
    #           .otherwise(0.0)
    #           .alias("rel_err"),
    #     ])
    # )

    # qa_demo_mismatch = (
    #     fact_qa
    #     .filter( (pl.col("diff_cnt").abs() > TOL_ABS) & (pl.col("rel_err").abs() > TOL_REL) )
    #     .select(["ETL_YMD","O_ADMI_CD","D_ADMI_CD","st_norm","fn_norm","purpose_cd",
    #              "TOTAL_CNT","sum_demo_cnt","diff_cnt","rel_err","slot_minutes"])
    #     .collect()
    # )
    # wp(qa_demo_mismatch, out / "qa_demo_mismatch.parquet")

    # # 허용오차 이내는 TOTAL_CNT를 데모합으로 맞춤 (정책은 필요 시 변경)
    # fact_fixed = fact_qa.with_columns([
    #     pl.when( (pl.col("diff_cnt").abs() <= TOL_ABS) | (pl.col("rel_err").abs() <= TOL_REL) )
    #       .then(pl.col("sum_demo_cnt")).otherwise(pl.col("TOTAL_CNT"))
    #       .alias("TOTAL_CNT_adj")
    # ])
    
    # ===== 데모합 vs TOTAL_CNT QA =====
    TOL_ABS = 1e-3
    TOL_REL = 0.01

    demo_sum = pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in (male_cols + feml_cols)])

    # 1) sum_demo_cnt
    fact_qa = fact_pre.with_columns([
        demo_sum.alias("sum_demo_cnt"),
    ])

    # 2) diff_cnt  (TOTAL_CNT는 fact_pre에서 이미 Float64 + fill_null(0.0) 완료)
    fact_qa = fact_qa.with_columns([
        (pl.col("TOTAL_CNT") - pl.col("sum_demo_cnt")).alias("diff_cnt"),
    ])

    # 3) rel_err  (여기서 diff_cnt 참조 가능)
    fact_qa = fact_qa.with_columns([
        pl.when(pl.col("TOTAL_CNT") > 0)
        .then(pl.col("diff_cnt") / pl.col("TOTAL_CNT"))
        .otherwise(0.0)
        .alias("rel_err"),
    ])

    qa_demo_mismatch = (
        fact_qa
        .filter(
            (pl.col("diff_cnt").abs() > TOL_ABS) &
            (pl.col("rel_err").abs() > TOL_REL)
        )
        .select([
            "ETL_YMD","O_ADMI_CD","D_ADMI_CD","st_norm","fn_norm","purpose_cd",
            "TOTAL_CNT","sum_demo_cnt","diff_cnt","rel_err","slot_minutes"
        ])
        .collect()
    )
    wp(qa_demo_mismatch, out / "qa_demo_mismatch.parquet")

    # 허용오차 이내는 TOTAL_CNT를 데모합으로 맞춤 (정책 필요시 변경)
    fact_fixed = fact_qa.with_columns([
        pl.when(
            (pl.col("diff_cnt").abs() <= TOL_ABS) |
            (pl.col("rel_err").abs() <= TOL_REL)
        )
        .then(pl.col("sum_demo_cnt"))
        .otherwise(pl.col("TOTAL_CNT"))
        .alias("TOTAL_CNT_adj")
    ])


    # ===== 최종 저장 (원본/정규화 코드 모두 보관) =====
    fact_od_flow = (
        fact_fixed
        .select(
            ["flow_id","ETL_YMD","O_ADMI_CD","D_ADMI_CD",
             "ST_TIME_CD","FNS_TIME_CD",
             pl.col("st_norm").alias("start_time_cd"),
             pl.col("fn_norm").alias("finish_time_cd"),
             "purpose_cd",
             "st_min","fn_min","slot_minutes",
             "TOTAL_CNT_adj"] +
            [c for c in ["MOVE_DIST","MOVE_TIME"] if c in num_cols] +
            male_cols + feml_cols
        )
        .rename({"TOTAL_CNT_adj": "TOTAL_CNT"})
        .collect()
    )
    wp(fact_od_flow, out / "fact_od_flow.parquet")

if __name__ == "__main__":
    pl.Config.set_tbl_rows(80)
    main()
