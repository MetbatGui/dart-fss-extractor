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
from infra.adapters.local_storage_adapter import LocalStorageAdapter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    # .env 파일 로드
    load_dotenv()
    
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        return

    logger.info("서비스 초기화 중...")
    
    # 어댑터 초기화
    corp_code_adapter = CorpCodeAdapter()
    financial_adapter = DartFinancialAdapter(api_key=api_key, use_cache=True)
    storage_adapter = LocalStorageAdapter()
    # DataProcessingService는 기본 설정 파일(config/account_keywords.toml) 사용
    processing_service = DataProcessingService()

    # 서비스 초기화
    service = FinancialCollectionService(
        corp_code_port=corp_code_adapter,
        financial_port=financial_adapter,
        storage_port=storage_adapter,
        processing_service=processing_service
    )

    # 수집 대상 기업 목록 로드
    target_file = Path("data/target_companies.csv")
    if target_file.exists():
        try:
            df = pd.read_csv(target_file)
            if "기업명" in df.columns:
                company_names = df["기업명"].tolist()
            else:
                logger.warning("target_companies.csv에 '기업명' 컬럼이 없습니다. 기본 리스트를 사용합니다.")
                company_names = ["삼성전자", "SK하이닉스", "LG에너지솔루션", "현대차", "NAVER"]
        except Exception as e:
            logger.error(f"기업 목록 파일 읽기 실패: {e}")
            company_names = ["삼성전자", "SK하이닉스"]
    else:
        logger.info("target_companies.csv 파일이 없습니다. 기본 리스트를 사용합니다.")
        # 기본 리스트 생성 및 저장
        company_names = ["삼성전자", "SK하이닉스", "LG에너지솔루션", "현대차", "NAVER"]
        target_file.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"기업명": company_names}).to_csv(target_file, index=False)
        logger.info(f"기본 기업 목록을 {target_file}에 저장했습니다.")

    # 수집 설정
    start_year = 2021
    end_year = 2023
    output_path = "output/financial_data_result.xlsx"

    logger.info(f"데이터 수집 시작: {len(company_names)}개 기업, {start_year}~{end_year}년")
    
    try:
        service.collect_and_save(
            company_names=company_names,
            start_year=start_year,
            end_year=end_year,
            output_path=output_path
        )
        logger.info("모든 작업이 완료되었습니다.")
        
    except Exception as e:
        logger.exception(f"작업 중 치명적인 오류 발생: {e}")

if __name__ == "__main__":
    main()
