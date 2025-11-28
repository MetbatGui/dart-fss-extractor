"""증분 업데이트 실행 스크립트."""

import argparse
import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# src 디렉토리를 모듈 검색 경로에 추가
sys.path.append(str(Path(__file__).parent))

from core.services.incremental_update_service import IncrementalUpdateService
from infra.adapters.local_file_reader_adapter import LocalFileReaderAdapter
from infra.adapters.corp_code_adapter import CorpCodeAdapter
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from infra.adapters.local_storage_adapter import LocalStorageAdapter
from core.services.data_processing_service import DataProcessingService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("IncrementalUpdate")

def main():
    # .env 로드
    load_dotenv()
    
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        return

    parser = argparse.ArgumentParser(description='누락된 분기 데이터 증분 업데이트')
    parser.add_argument('--file', required=True, help='업데이트할 엑셀 파일 경로 (예: output/financial_data_2015_2025.xlsx)')
    parser.add_argument('--year', type=int, required=True, help='대상 연도 (예: 2025)')
    parser.add_argument('--quarter', type=int, required=True, choices=[1,2,3,4], help='대상 분기 (1, 2, 3, 4)')
    parser.add_argument('--no-backup', action='store_true', help='백업 생략')
    parser.add_argument('--max-api-calls', type=int, default=9950, help='최대 API 호출 횟수 (기본값: 9950)')
    
    args = parser.parse_args()
    
    logger.info("서비스 초기화 중...")
    
    # 어댑터 초기화
    file_reader = LocalFileReaderAdapter()
    corp_code_port = CorpCodeAdapter()
    financial_port = DartFinancialAdapter(api_key=api_key, use_cache=True)
    storage_port = LocalStorageAdapter()
    processing_service = DataProcessingService()
    
    # 서비스 초기화
    service = IncrementalUpdateService(
        file_reader=file_reader,
        corp_code_port=corp_code_port,
        financial_port=financial_port,
        storage_port=storage_port,
        processing_service=processing_service,
        max_api_calls=args.max_api_calls
    )
    
    # 실행
    try:
        service.update_missing_quarters(
            file_path=args.file,
            target_year=args.year,
            target_quarter=args.quarter,
            auto_backup=not args.no_backup
        )
    except Exception as e:
        logger.exception(f"작업 중 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
