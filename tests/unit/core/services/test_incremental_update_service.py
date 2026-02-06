"""IncrementalUpdateService 테스트."""

import pytest
import pandas as pd
from unittest.mock import Mock, call
from core.ports.file_reader_port import FileReaderPort
from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.export_port import ExportPort
from core.services.data_processing_service import DataProcessingService
from core.services.incremental_update_service import IncrementalUpdateService


@pytest.fixture
def mock_file_reader():
    return Mock(spec=FileReaderPort)

@pytest.fixture
def mock_corp_code_port():
    return Mock(spec=CorpCodePort)

@pytest.fixture
def mock_financial_port():
    return Mock(spec=FinancialStatementPort)

@pytest.fixture
def mock_export_port():
    return Mock(spec=ExportPort)

@pytest.fixture
def mock_processing_service():
    return Mock(spec=DataProcessingService)

@pytest.fixture
def service(mock_file_reader, mock_corp_code_port, mock_financial_port, mock_export_port, mock_processing_service):
    return IncrementalUpdateService(
        file_reader=mock_file_reader,
        corp_code_port=mock_corp_code_port,
        financial_port=mock_financial_port,
        export_port=mock_export_port,
        processing_service=mock_processing_service
    )


def test_find_missing_companies_column_missing(service):
    """컬럼 자체가 없으면 모든 인덱스를 반환해야 함."""
    df = pd.DataFrame(index=["A", "B"], columns=["2023.1Q"])
    sheets = {"매출액_분기별": df}
    
    missing = service.find_missing_companies(sheets, "2023.2Q")
    assert missing == ["A", "B"]


def test_find_missing_companies_partial(service):
    """일부 값이 NaN인 경우 해당 인덱스만 반환."""
    df = pd.DataFrame({
        "2023.1Q": [100, None, 200],
        "2023.2Q": [None, 300, 400]
    }, index=["A", "B", "C"])
    sheets = {"매출액_분기별": df}
    
    # 2023.1Q 결측: B
    missing_1q = service.find_missing_companies(sheets, "2023.1Q")
    assert missing_1q == ["B"]
    
    # 2023.2Q 결측: A
    missing_2q = service.find_missing_companies(sheets, "2023.2Q")
    assert missing_2q == ["A"]


def test_merge_quarterly_data_append(service):
    """새로운 데이터 병합 (기존 데이터 유지)."""
    # 기존: A사 1Q 있음
    existing_df = pd.DataFrame({"2023.1Q": [100]}, index=["A"])
    existing_sheets = {"매출액_분기별": existing_df}
    
    # 신규: A사 2Q 데이터
    new_df = pd.DataFrame({"2023.2Q": [200]}, index=["A"])
    new_sheets = {"매출액_분기별": new_df}
    
    merged = service.merge_quarterly_data(existing_sheets, new_sheets)
    result_df = merged["매출액_분기별"]
    
    assert "2023.1Q" in result_df.columns
    assert "2023.2Q" in result_df.columns
    assert result_df.loc["A", "2023.1Q"] == 100
    assert result_df.loc["A", "2023.2Q"] == 200


def test_merge_quarterly_data_overwrite(service):
    """덮어쓰기 모드."""
    # 기존: A사 1Q = 100
    existing_df = pd.DataFrame({"2023.1Q": [100]}, index=["A"])
    existing_sheets = {"매출액_분기별": existing_df}
    
    # 신규: A사 1Q = 999 (수정됨)
    new_df = pd.DataFrame({"2023.1Q": [999]}, index=["A"])
    new_sheets = {"매출액_분기별": new_df}
    
    # overwrite=True
    merged = service.merge_quarterly_data(existing_sheets, new_sheets, overwrite=True)
    assert merged["매출액_분기별"].loc["A", "2023.1Q"] == 999
    
    # overwrite=False (기본값)
    merged_no_overwrite = service.merge_quarterly_data(existing_sheets, new_sheets, overwrite=False)
    # combine_first는 기존 값이 있으면 유지함
    assert merged_no_overwrite["매출액_분기별"].loc["A", "2023.1Q"] == 100


def test_merge_quarterly_data_sorting(service):
    """컬럼 정렬 확인 (2023.1Q -> 2023.2Q)."""
    existing_df = pd.DataFrame({"2023.2Q": [200]}, index=["A"]) 
    new_df = pd.DataFrame({"2023.1Q": [100]}, index=["A"]) # 1Q가 나중에 들어옴
    
    existing_sheets = {"매출액_분기별": existing_df}
    new_sheets = {"매출액_분기별": new_df}
    
    merged = service.merge_quarterly_data(existing_sheets, new_sheets)
    cols = merged["매출액_분기별"].columns.tolist()
    
    assert cols == ["2023.1Q", "2023.2Q"]
