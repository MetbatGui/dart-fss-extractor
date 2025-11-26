import os
import pytest
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from core.services.financial_collection_service import FinancialCollectionService
from core.services.data_processing_service import DataProcessingService
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from infra.adapters.corp_code_adapter import CorpCodeAdapter
from infra.adapters.local_storage_adapter import LocalStorageAdapter

import sys
import logging
from dotenv import load_dotenv

# .env 파일 로드 (API 키 설정을 위해)
load_dotenv()

@pytest.fixture(autouse=True)
def setup_logging():
    """E2E 테스트를 위한 로깅 설정"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # 기존 핸들러 제거 (중복 방지)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    # Stdout 핸들러 추가
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


@pytest.fixture
def setup_services():
    # 실제 어댑터 사용
    # API 키는 환경 변수 DART_API_KEY에서 자동으로 로드됨
    financial_adapter = DartFinancialAdapter(use_cache=True)
    corp_code_adapter = CorpCodeAdapter()
    storage_adapter = LocalStorageAdapter()
    # DataProcessingService는 기본 설정 파일(config/account_keywords.toml) 사용
    processing_service = DataProcessingService()

    service = FinancialCollectionService(
        corp_code_port=corp_code_adapter,
        financial_port=financial_adapter,
        storage_port=storage_adapter,
        processing_service=processing_service
    )
    return service

def test_financial_collection_e2e(setup_services, tmp_path):
    """
    E2E 테스트:
    1. 10개 기업 목록을 로드
    2. 2023년 데이터 수집 실행
    3. 결과 엑셀 파일 생성 및 내용 검증
    """
    service = setup_services
    
    # 1. 기업 목록 로드 (테스트용 더미 데이터 생성)
    data = {"기업명": ["삼성전자", "SK하이닉스"]}
    df_companies = pd.DataFrame(data)
    
    # 테스트를 위해 임시 파일로 저장했다가 읽는 척 하거나, 그냥 바로 리스트로 변환
    company_names = df_companies['기업명'].tolist()
    
    # 2. 수집 실행 (2023년)
    # 결과 파일은 임시 디렉토리 또는 output 디렉토리에 저장
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "e2e_test_result_2023.xlsx"
    
    # 기존 파일이 있다면 삭제
    if output_file.exists():
        os.remove(output_file)
        
    print(f"\n[E2E] 데이터 수집 시작: {len(company_names)}개 기업, 2023년")
    service.collect_and_save(
        company_names=company_names,
        start_year=2023,
        end_year=2023,
        output_path=str(output_file)
    )
    
    # 3. 검증
    assert output_file.exists(), "결과 엑셀 파일이 생성되지 않았습니다."
    
    # 엑셀 파일 읽기
    xls = pd.ExcelFile(output_file)
    
    # 필수 시트 확인
    expected_sheets = [
        "매출액_분기", "영업이익_분기", "당기순이익_분기",
        "매출액_연간", "영업이익_연간", "당기순이익_연간"
    ]
    for sheet in expected_sheets:
        assert sheet in xls.sheet_names, f"시트 '{sheet}'가 누락되었습니다."
        
        df = pd.read_excel(xls, sheet_name=sheet, index_col="기업명")
        assert not df.empty, f"시트 '{sheet}'의 데이터가 비어있습니다."
        
        # Wide Format 검증: 인덱스가 기업명이어야 함
        assert "삼성전자" in df.index, f"시트 '{sheet}'에 '삼성전자' 행이 없습니다."
        assert "SK하이닉스" in df.index, f"시트 '{sheet}'에 'SK하이닉스' 행이 없습니다."
        
        # 컬럼 검증 (기간)
        if "분기" in sheet:
            # 예: 2023.1Q, 2023.2Q ...
            expected_col = "2023.1Q"
            assert expected_col in df.columns, f"시트 '{sheet}'에 컬럼 '{expected_col}'이 누락되었습니다."
        else:
            # 예: 2023
            expected_col = 2023
            assert expected_col in df.columns, f"시트 '{sheet}'에 컬럼 '{expected_col}'이 누락되었습니다."

    print("\n[E2E] 테스트 성공: 모든 시트와 데이터가 정상적으로 생성되었습니다.")
