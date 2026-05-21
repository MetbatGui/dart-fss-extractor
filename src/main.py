"""메인 실행 스크립트."""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# src 디렉토리를 모듈 검색 경로에 추가
sys.path.append(str(Path(__file__).parent))

from core.services.financial_collection_service import FinancialCollectionService
from core.services.data_processing_service import DataProcessingService
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from infra.adapters.corp_code_adapter import CorpCodeAdapter
from infra.adapters.parquet_repository_adapter import ParquetRepositoryAdapter
from infra.adapters.excel_export_adapter import ExcelExportAdapter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

import argparse

def main():
    # .env 파일 로드
    load_dotenv()
    
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        return

    # 인자 파싱
    parser = argparse.ArgumentParser(description='DART 재무 데이터 수집기')
    parser.add_argument('--start-year', type=int, default=2015, help='수집 시작 연도 (기본값: 2015)')
    parser.add_argument('--end-year', type=int, default=2025, help='수집 종료 연도 (기본값: 2025)')
    parser.add_argument('--companies', type=str, default="data/target_companies.csv", help='대상 기업 목록 파일 경로')
    parser.add_argument('--output', type=str, default="output/financial_data_result.xlsx", help='결과 엑셀 파일 저장 경로')
    parser.add_argument('--force', action='store_true', help='기존 수집 데이터를 무시하고 다시 수집')
    parser.add_argument('--no-skip-failed', action='store_true', help='실패한 연도도 다시 시도')

    args = parser.parse_args()

    logger.info("서비스 초기화 중...")
    
    # 어댑터 초기화
    corp_code_adapter = CorpCodeAdapter()
    financial_adapter = DartFinancialAdapter(api_key=api_key, use_cache=True)
    repository_adapter = ParquetRepositoryAdapter()
    export_adapter = ExcelExportAdapter()
    processing_service = DataProcessingService()

    # 서비스 초기화
    service = FinancialCollectionService(
        corp_code_port=corp_code_adapter,
        financial_port=financial_adapter,
        repository_port=repository_adapter,
        export_port=export_adapter,
        processing_service=processing_service
    )

    # 수집 대상 기업 목록 로드
    target_file = Path(args.companies)
    if target_file.exists():
        try:
            if target_file.suffix == '.csv':
                df = pd.read_csv(target_file)
            elif target_file.suffix == '.xlsx':
                df = pd.read_excel(target_file)
            else:
                logger.error(f"지원하지 않는 파일 형식입니다: {target_file.suffix}")
                return

            # 지능적인 기업명 컬럼 매핑
            company_names = []
            
            # 1. 컬럼명 직접 매칭 시도
            target_col = None
            for col in df.columns:
                col_str = str(col).strip()
                if col_str in ["기업명", "종목명", "회사명", "corp_name"]:
                    target_col = col
                    break
            
            if target_col is not None:
                company_names = df[target_col].dropna().astype(str).str.strip().unique().tolist()
                logger.info(f"'{args.companies}'의 '{target_col}' 컬럼을 사용하여 {len(company_names)}개 기업을 로드했습니다.")
            else:
                # 2. 컬럼명이 매칭되지 않을 경우 열 형태에 따른 추정
                if df.shape[1] >= 2:
                    # 첫 번째 열의 첫 번째 유효 값이 6자리 숫자(종목코드 등)인지 확인
                    first_val = str(df.iloc[:, 0].dropna().iloc[0]).strip() if not df.iloc[:, 0].dropna().empty else ""
                    if first_val.isdigit() and len(first_val) == 6:
                        # 첫 번째 열이 코드이고 두 번째 열이 기업명일 확률이 매우 높음 (전종목리스트.xlsx 대응)
                        company_names = df.iloc[:, 1].dropna().astype(str).str.strip().unique().tolist()
                        logger.info(f"'{args.companies}'의 첫 번째 열(코드 형태)을 건너뛰고, 두 번째 열을 사용하여 {len(company_names)}개 기업을 로드했습니다.")
                    
                if not company_names:
                    # 차선책: 첫 번째 열 사용 (기존 호환성 유지)
                    company_names = df.iloc[:, 0].dropna().astype(str).str.strip().unique().tolist()
                    logger.info(f"'{args.companies}'의 첫 번째 열을 사용하여 {len(company_names)}개 기업을 로드했습니다.")
        except Exception as e:
            logger.error(f"기업 목록 파일 읽기 실패: {e}")
            return
    else:
        logger.error(f"기업 목록 파일을 찾을 수 없습니다: {args.companies}")
        return

    logger.info(f"데이터 수집 시작: {len(company_names)}개 기업, {args.start_year}~{args.end_year}년")
    
    try:
        service.collect_and_save(
            company_names=company_names,
            start_year=args.start_year,
            end_year=args.end_year,
            output_path=args.output,
            skip_failed=not args.no_skip_failed,
            force_recollect=args.force
        )
        logger.info("모든 작업이 완료되었습니다.")
        
    except Exception as e:
        logger.exception(f"작업 중 치명적인 오류 발생: {e}")

if __name__ == "__main__":
    main()
