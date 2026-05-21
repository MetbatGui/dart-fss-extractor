import os
import sys
import logging
import shutil
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# src 디렉토리를 모듈 검색 경로에 추가
sys.path.append(str(Path(__file__).parent / "src"))

from infra.adapters.parquet_repository_adapter import ParquetRepositoryAdapter
from infra.adapters.excel_export_adapter import ExcelExportAdapter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clear_output_directory(output_dir: Path) -> None:
    """output 디렉토리 안의 모든 파일과 서브디렉토리를 삭제합니다."""
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        return

    logger.info(f"'{output_dir}' 디렉토리 초기화 중 (모든 파일 삭제)...")
    for item in output_dir.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        except Exception as e:
            logger.error(f"파일 삭제 실패 ({item}): {e}")

def load_integrated_data(repo_dir: Path) -> pd.DataFrame:
    """연결(CFS)이 있으면 연결을 기본으로 하고, 없으면 개별(OFS)을 사용하며, 기존 Raw 캐시 저장소 데이터를 스마트하게 병합합니다."""
    cfs_dir = repo_dir / "financial_data_cfs"
    ofs_dir = repo_dir / "financial_data_ofs"
    raw_dir = repo_dir.parent / "financial_data_raw"
    
    # 1. V2 정밀 가공 데이터 로드 (연결 우선, 개별 보완)
    cfs_codes = set(f.stem for f in cfs_dir.glob("*.parquet")) if cfs_dir.exists() else set()
    ofs_codes = set(f.stem for f in ofs_dir.glob("*.parquet")) if ofs_dir.exists() else set()
    v2_codes = cfs_codes | ofs_codes
    
    logger.info(f"V2 DB 수집 상황: 총 {len(v2_codes)}개 기업 (연결 존재: {len(cfs_codes)}개, 개별 존재: {len(ofs_codes)}개)")
    
    all_data = []
    
    for code in tqdm(v2_codes, desc="V2 정밀 데이터 로드 중"):
        df = pd.DataFrame()
        source = ""
        
        # 연결 재무제표가 존재할 경우 연결 사용
        if code in cfs_codes:
            file_path = cfs_dir / f"{code}.parquet"
            df = pd.read_parquet(file_path)
            source = "연결(CFS)"
        # 연결이 없고 개별만 존재할 경우 개별 사용
        elif code in ofs_codes:
            file_path = ofs_dir / f"{code}.parquet"
            df = pd.read_parquet(file_path)
            source = "개별(OFS)"
            
        if not df.empty:
            df["종목코드"] = code
            df["데이터출처"] = source
            all_data.append(df)
            
    # 2. 기존 수집 완료된 Raw 캐시 데이터 스마트 병합 (V2에 없는 기업만 추가 지원)
    if raw_dir.exists():
        raw_files = list(raw_dir.glob("*.parquet"))
        raw_codes = set(f.stem for f in raw_files)
        additional_codes = raw_codes - v2_codes
        
        logger.info(f"Raw 캐시 상황: 총 {len(raw_codes)}개 기업 중 {len(additional_codes)}개 기업을 무손실 통합 보완합니다.")
        
        for code in tqdm(additional_codes, desc="Raw 캐시 데이터 병합 중"):
            file_path = raw_dir / f"{code}.parquet"
            try:
                df = pd.read_parquet(file_path)
                if not df.empty:
                    # 한글 깨짐 방지를 위해 컬럼명 강제 세팅 및 표준화
                    if df.shape[1] == 7:
                        df.columns = ["기업명", "연도", "구분", "분기", "매출액", "영업이익", "당기순이익"]
                    elif df.shape[1] == 8:
                        df.columns = ["기업명", "연도", "구분", "구분_상세", "분기", "매출액", "영업이익", "당기순이익"]
                        
                    df["종목코드"] = code
                    df["데이터출처"] = "기존수집(RAW)"
                    all_data.append(df)
            except Exception as e:
                logger.error(f"Raw 캐시 로드 중 오류 발생 ({code}): {e}")
    else:
        logger.warning(f"Raw 캐시 디렉토리가 존재하지 않습니다: {raw_dir}")
            
    if not all_data:
        return pd.DataFrame()
        
    return pd.concat(all_data, ignore_index=True)

def export_v2_integrated():
    """연결 우선 / 개별 보완 방식으로 최종 통합 엑셀 파일을 생성합니다."""
    
    output_dir = Path("output")
    # 1. output 폴더 청소
    clear_output_directory(output_dir)
    
    output_path = output_dir / "financial_data_integrated.xlsx"
    repo_base_dir = Path("data/repository/financial_data_v2")
    
    # 2. 데이터 수집 및 통합
    logger.info("연결(CFS) 우선 / 개별(OFS) 대체 전략으로 데이터를 읽는 중...")
    df_base = load_integrated_data(repo_base_dir)
    
    if df_base.empty:
        logger.error("통합 로드된 데이터가 없습니다. 저장소 경로 및 데이터를 확인하세요.")
        return
        
    logger.info(f"성공적으로 {len(df_base)}행의 데이터를 통합 로드했습니다. 비정상 캐시 데이터 정제 가드를 적용합니다.")

    # 2.1 비정상 캐시 오염 기업 필터 가드 (매출액, 영업이익, 당기순이익이 모두 0.0이거나 결측치인 수집 오류 기업 배제)
    if not df_base.empty:
        df_temp = df_base.copy()
        df_temp["매출액_abs"] = pd.to_numeric(df_temp["매출액"], errors="coerce").abs().fillna(0)
        df_temp["영업이익_abs"] = pd.to_numeric(df_temp["영업이익"], errors="coerce").abs().fillna(0)
        df_temp["당기순이익_abs"] = pd.to_numeric(df_temp["당기순이익"], errors="coerce").abs().fillna(0)
        df_temp["실적합"] = df_temp["매출액_abs"] + df_temp["영업이익_abs"] + df_temp["당기순이익_abs"]
        
        # 기업별 실적합의 총합이 0인 비정상 기업 식별
        zero_performance_corps = df_temp.groupby("종목코드")["실적합"].sum()
        invalid_codes = zero_performance_corps[zero_performance_corps == 0].index.tolist()
        
        if invalid_codes:
            for code in invalid_codes:
                corp_name = df_base[df_base["종목코드"] == code]["기업명"].iloc[0]
                logger.warning(f"[FILTER GUARD] 비정상 캐시 오염 기업 제거 완료: {corp_name} ({code}) - 모든 실적 데이터가 0.0 또는 결측치입니다.")
            
            # 오염 기업 데이터 제외
            df_base = df_base[~df_base["종목코드"].isin(invalid_codes)]
            logger.info(f"필터 가드 적용 후 남은 데이터: {len(df_base)}행")

    # 3. 데이터 가공 (Pivot 및 Scaling)
    final_dfs = {}
    
    # (1) 분기 데이터 처리
    df_quarter = df_base[df_base["구분"] == "분기"].copy()
    if not df_quarter.empty:
        # 기간 컬럼 생성 (예: 2023.1Q)
        df_quarter["기간"] = df_quarter["연도"].astype(int).astype(str) + "." + df_quarter["분기"].astype(str)
        
        # 중복 제거 (종목코드, 기간 기준)
        df_quarter = df_quarter.drop_duplicates(subset=["종목코드", "기간"])
        
        # Pivot (기업명과 종목코드가 나란히 엑셀에 출력되도록 멀티인덱스 지정)
        final_dfs["매출액_분기"] = df_quarter.pivot(index=["기업명", "종목코드"], columns="기간", values="매출액")
        final_dfs["영업이익_분기"] = df_quarter.pivot(index=["기업명", "종목코드"], columns="기간", values="영업이익")
        final_dfs["당기순이익_분기"] = df_quarter.pivot(index=["기업명", "종목코드"], columns="기간", values="당기순이익")

    # (2) 연간 데이터 처리
    df_annual = df_base[df_base["구분"] == "연간"].copy()
    if not df_annual.empty:
        # 중복 제거 (종목코드, 연도 기준)
        df_annual = df_annual.drop_duplicates(subset=["종목코드", "연도"])
        
        # Pivot (기업명과 종목코드가 나란히 엑셀에 출력되도록 멀티인덱스 지정)
        final_dfs["매출액_연간"] = df_annual.pivot(index=["기업명", "종목코드"], columns="연도", values="매출액")
        final_dfs["영업이익_연간"] = df_annual.pivot(index=["기업명", "종목코드"], columns="연도", values="영업이익")
        final_dfs["당기순이익_연간"] = df_annual.pivot(index=["기업명", "종목코드"], columns="연도", values="당기순이익")

    # (3) 단위 변환 (원 -> 백만 원)
    DIVISOR = 1_000_000 # 백만 원 단위
    for sheet_name, df in final_dfs.items():
        if df is not None and not df.empty:
            # 숫자로 변환 후 백만 단위로 나누고 반올림
            final_dfs[sheet_name] = (df.apply(pd.to_numeric, errors='coerce') / DIVISOR).round(0)

    # 4. 엑셀 내보내기
    logger.info(f"통합 엑셀 파일 생성 중: {output_path} (단위: 백만 원)")
    exporter = ExcelExportAdapter()
    try:
        exporter.export_excel(final_dfs, str(output_path))
        logger.info(f"[SUCCESS] 엑셀 파일 생성 완료: {output_path}")
    except PermissionError:
        alternative_path = output_dir / "financial_data_integrated_v3.xlsx"
        logger.warning(f"[WARNING] 기존 엑셀 파일이 열려 있어 덮어쓰기가 불가능합니다. 대체 파일로 생성합니다: {alternative_path}")
        try:
            exporter.export_excel(final_dfs, str(alternative_path))
            logger.info(f"[SUCCESS] 대체 엑셀 파일 생성 완료: {alternative_path}")
            output_path = alternative_path
        except Exception as alt_e:
            logger.error(f"대체 엑셀 저장 중 오류 발생: {alt_e}")
    except Exception as e:
        logger.error(f"엑셀 저장 중 오류 발생: {e}")
        
    # 생성 후 검증
    try:
        remaining_files = list(output_dir.iterdir())
        logger.info(f"output 디렉토리의 최종 파일 목록: {[f.name for f in remaining_files]}")
    except Exception as e:
        logger.error(f"최종 목록 조회 실패: {e}")

if __name__ == "__main__":
    export_v2_integrated()

