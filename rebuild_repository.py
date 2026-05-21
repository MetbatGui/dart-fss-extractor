import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

from core.services.financial_collection_service import FinancialCollectionService
from core.services.data_processing_service import DataProcessingService
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from infra.adapters.parquet_repository_adapter import ParquetRepositoryAdapter
from infra.adapters.excel_export_adapter import ExcelExportAdapter
from infra.adapters.corp_code_adapter import CorpCodeAdapter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('collection_rebuild.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def get_target_companies():
    """수집 대상 기업 리스트를 엑셀 파일에서 추출합니다."""
    # 1. 엑셀 파일 경로 확인
    excel_path = Path("output/financial_data_integrated.xlsx")
    if not excel_path.exists():
        # 원본 파일이 없으면 기존 financial_data_integrated.xlsx 확인
        excel_path = Path("financial_data_integrated.xlsx")
        
    if not excel_path.exists():
        logger.error("대상 기업을 찾을 수 있는 엑셀 파일이 없습니다.")
        return []

    try:
        # '재_분기별' 시트에서 기업명 리스트 추출
        xl = pd.ExcelFile(excel_path)
        if '재_분기별' in xl.sheet_names:
            df = xl.parse('재_분기별', index_col=0)
            return df.index.unique().tolist()
        else:
            # 시트가 없으면 첫 번째 시트에서 시도
            df = xl.parse(xl.sheet_names[0], index_col=0)
            return df.index.unique().tolist()
    except Exception as e:
        logger.error(f"엑셀 파일 읽기 중 오류 발생: {e}")
        return []

def main():
    # 0. 명령행 인자 처리
    parser = argparse.ArgumentParser(description='DART 재무 데이터 전수 재수집 스크립트')
    parser.add_argument('--limit', type=int, default=5000, help='API 호출 한도 (기본값: 5000)')
    args = parser.parse_args()

    # 1. 대상 기업 리스트 확보
    company_names = get_target_companies()
    if not company_names:
        logger.error("수집 대상 기업이 없습니다. 프로그램을 종료합니다.")
        return

    logger.info(f"총 {len(company_names)}개 기업에 대한 전수 재수집을 준비합니다. (한도: {args.limit})")

    # 2. 어댑터 및 서비스 초기화
    # 신규 저장소 경로 사용
    repo_adapter = ParquetRepositoryAdapter(base_dir="data/repository/financial_data_v2")
    financial_port = DartFinancialAdapter(use_cache=True) # 기존 캐시는 활용 (API 절약)
    export_port = ExcelExportAdapter()
    
    # 기업 코드 어댑터 (DART API를 통한 XML 매핑 사용)
    corp_code_adapter = CorpCodeAdapter()
    
    processing_service = DataProcessingService()
    collection_service = FinancialCollectionService(
        corp_code_port=corp_code_adapter,
        financial_port=financial_port,
        repository_port=repo_adapter,
        export_port=export_port,
        processing_service=processing_service
    )

    # 3. 기업 코드 매핑
    logger.info("기업 코드 매핑 중...")
    codes = corp_code_adapter.get_codes(company_names)
    target_list = []
    for name, code in zip(company_names, codes):
        if code:
            target_list.append({"name": name, "code": code})
    
    logger.info(f"유효한 기업 코드 확인 완료: {len(target_list)}개 기업")

    # 4. 전수 재수집 실행
    # 2014년부터 2025년까지 전수 수집
    output_path = "output/financial_data_rebuild.xlsx"
    collection_service.rebuild_full_repository(
        company_list=target_list,
        start_year=2014,
        end_year=2025,
        output_path=output_path,
        max_api_calls=args.limit,
        progress_path="data/collection_progress_v2.json"
    )

if __name__ == "__main__":
    main()
