
"""
최적화 다시 진행. 목적은 원본 통합 데이터 용량 줄이기
1) 좌표 관련 컬럼 제거
2) 우리가 정한 6개 시/군/구 필터링
   - 서울: 마포구, 양천구
   - 경기: 수원시 팔달구, 동두천시
   - 인천: 부평구, 서구
"""

import os
import time
import polars as pl

pl.Config.set_tbl_cols(-1)  # -1이면 모든 컬럼 출력
# pl.Config.set_tbl_rows(5)   # 표시할 행 수
pl.Config.set_tbl_width_chars(0)  # 터미널 가로폭 제한 해제


IN  = "/home1/bismarck/transit_seoul/dataset/merged_all_admi.parquet"   # ADMI 붙인 최종본
OUT = "/home1/bismarck/transit_seoul/dataset/merged_all_admi_select.parquet"
TMP = OUT + ".tmp"

# 좌표 관련 컬럼 전부 제거
coord_patterns = ["^O_CELL_.*$", "^D_CELL_.*$"]

# # 안 쓰는 고연령대 컬럼은 일단 보류
# senior_cols = [
#     "MALE_00_CNT","MALE_40_CNT","MALE_45_CNT","MALE_50_CNT","MALE_55_CNT","MALE_60_CNT","MALE_65_CNT","MALE_70_CNT","MALE_75_CNT","MALE_80_CNT","MALE_85_CNT",
#     "FEML_00_CNT","FEML_40_CNT","FEML_45_CNT","FEML_50_CNT","FEML_55_CNT","FEML_60_CNT","FEML_65_CNT","FEML_70_CNT","FEML_75_CNT","FEML_80_CNT","FEML_85_CNT",
# ]

# 대상 시군구(문자열 정확 일치 기준)
TARGET_SGG = ["마포구", "양천구", "수원시 팔달구", "동두천시", "부평구", "서구"]

def log(msg: str):
    print(msg, flush=True)

def build_filter_expr(cols: list[str]):
    """
    스키마에 O_SIDO_NM이 있으면 시도+구 조합으로 더 정밀하게 필터,
    없으면 O_SGG_NM만으로 필터.
    """
    if "O_SIDO_NM" in cols and "O_SGG_NM" in cols:
        seoul = (pl.col("O_SIDO_NM") == "서울특별시")   & pl.col("O_SGG_NM").is_in(["마포구", "양천구"])
        gyeonggi = (pl.col("O_SIDO_NM") == "경기도")    & pl.col("O_SGG_NM").is_in(["수원시 팔달구", "동두천시"])
        incheon = (pl.col("O_SIDO_NM") == "인천광역시") & pl.col("O_SGG_NM").is_in(["부평구", "서구"])
        return seoul | gyeonggi | incheon
    else:
        # Fallback: 시도 정보가 없으면 시군구명만으로 필터
        return pl.col("O_SGG_NM").is_in(TARGET_SGG)

def main():
    t0 = time.perf_counter()
    log("[INFO] 스키마 확인 중…")
    # 가벼운 헤더 로드로 컬럼 목록만 확보
    header_df = pl.read_parquet(IN, n_rows=0)
    cols = header_df.columns

    log("[INFO] 데이터 로드 및 컬럼 제거·필터링 시작")
    lf = (
        pl.scan_parquet(IN)
          .select(pl.all().exclude(*coord_patterns))
          .filter(build_filter_expr(cols))
    )

    if os.path.exists(TMP):
        os.remove(TMP)

    # 스트리밍 싱크: 대용량에도 안정적
    lf.sink_parquet(TMP, compression="lz4", statistics=False)
    os.replace(TMP, OUT)

    elapsed = time.perf_counter() - t0
    log(f"[INFO] 저장 완료 → {OUT}")
    log(f"[INFO] 총 소요시간: {elapsed:.1f}초 ({elapsed/60:.2f}분)")
    
        # 🔎 저장된 파일에서 상위 10행 확인
    log("[INFO] 결과 미리보기 (상위 10행)")
    preview = pl.read_parquet(OUT, n_rows=10)
    print(preview)

if __name__ == "__main__":
    main()
