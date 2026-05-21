"""FinancialCollectionService 테스트."""

import pytest
from unittest.mock import Mock, call, MagicMock
from decimal import Decimal
import pandas as pd

from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.repository_port import RepositoryPort
from core.ports.export_port import ExportPort
from core.services.data_processing_service import DataProcessingService
from core.services.financial_collection_service import FinancialCollectionService
from core.domain.models.financial_statement import FinancialStatement, ReportType
from core.domain.models.performance_metrics import QuarterlyMetrics, FinancialMetrics
from core.domain.models.company import Company


@pytest.fixture
def mock_corp_code_port():
    return Mock(spec=CorpCodePort)

@pytest.fixture
def mock_financial_port():
    return Mock(spec=FinancialStatementPort)

@pytest.fixture
def mock_repository_port():
    return Mock(spec=RepositoryPort)

@pytest.fixture
def mock_export_port():
    return Mock(spec=ExportPort)

@pytest.fixture
def mock_processing_service():
    return Mock(spec=DataProcessingService)

@pytest.fixture
def service(mock_corp_code_port, mock_financial_port, mock_repository_port, mock_export_port, mock_processing_service):
    return FinancialCollectionService(
        corp_code_port=mock_corp_code_port,
        financial_port=mock_financial_port,
        repository_port=mock_repository_port,
        export_port=mock_export_port,
        processing_service=mock_processing_service
    )


def test_collect_and_save_success(
    service, 
    mock_corp_code_port, 
    mock_financial_port, 
    mock_processing_service, 
    mock_repository_port,
    mock_export_port
):
    """정상적인 수집 및 저장 흐름 테스트 (Metadata 포함)."""
    
    # 1. Mocking setup
    company_names = ["TestCorp"]
    mock_corp_code_port.get_codes.return_value = ["12345678"]
    
    # 이어하기 체크: 파티션 없음
    mock_repository_port.exists.return_value = False
    
    # Company 메타데이터 로드 (없음)
    mock_repository_port.load_company_metadata.return_value = None

    # load_all Mock (저장된 데이터 시뮬레이션)
    saved_df = pd.DataFrame([{
        "기업명": "TestCorp", "연도": 2023, "구분": "분기", "분기": "1Q", 
        "매출액": 1000, "영업이익": 100, "당기순이익": 50,
        "기간": "2023.1Q"
    }])
    mock_repository_port.load_all.return_value = saved_df
    
    # Financial Statements Mock
    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    
    # Metrics Calculation Mock
    mock_metrics = QuarterlyMetrics(corp_name="TestCorp")
    mock_metrics.metrics_by_quarter = {
        "1Q": FinancialMetrics(revenue=Decimal("1000")),
    }
    mock_processing_service.calculate_quarterly_performance.return_value = mock_metrics
    
    # 2. Execution
    service.collect_and_save(
        company_names=company_names,
        start_year=2023,
        end_year=2023,
        output_path="test_output.xlsx"
    )
    
    # 3. Validation
    # 메타데이터 로드 확인
    mock_repository_port.load_company_metadata.assert_called_once_with("12345678")
    
    # 메타데이터 저장 확인
    mock_repository_port.save_company_metadata.assert_called_once()
    saved_company = mock_repository_port.save_company_metadata.call_args[0][0]
    assert isinstance(saved_company, Company)
    assert 2023 in saved_company.success_years
    
    # 기타 저장 확인
    mock_repository_port.save_partition.assert_called_once()
    mock_export_port.export_excel.assert_called_once()


def test_collect_failure_tracking(
    service,
    mock_corp_code_port,
    mock_financial_port,
    mock_repository_port,
    mock_processing_service
):
    """실패 연도(Failed Years)가 추적되는지 확인."""
    company_names = ["FailCorp"]
    mock_corp_code_port.get_codes.return_value = ["999999"]
    mock_repository_port.exists.return_value = False
    mock_repository_port.load_company_metadata.return_value = None
    mock_repository_port.load_all.return_value = pd.DataFrame()

    # API 에러 발생
    mock_financial_port.get_financial_statement.side_effect = Exception("API Fail")

    service.collect_and_save(company_names, 2023, 2023, "test.xlsx")

    # 메타데이터 저장 확인
    mock_repository_port.save_company_metadata.assert_called_once()
    saved_company = mock_repository_port.save_company_metadata.call_args[0][0]
    
    # 2023년이 실패 목록에 있어야 함
    assert 2023 in saved_company.failed_years
    assert 2023 not in saved_company.success_years


def test_retry_on_failure_history(
    service,
    mock_corp_code_port,
    mock_financial_port,
    mock_repository_port,
    mock_processing_service
):
    """이전에 실패한 이력이 있으면 파티션이 존재해도 재시도하는지 확인."""
    company_names = ["RetryCorp"]
    mock_corp_code_port.get_codes.return_value = ["888888"]
    
    # 파티션은 존재하지만
    mock_repository_port.exists.return_value = True
    
    # 실패 이력이 있는 상태
    existing_company = Company(code="888888", name="RetryCorp")
    existing_company.failed_years = [2023]
    mock_repository_port.load_company_metadata.return_value = existing_company
    
    mock_repository_port.load_all.return_value = pd.DataFrame() # 마지막 병합용

    # 이번엔 성공하도록 설정
    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    mock_processing_service.calculate_quarterly_performance.return_value = QuarterlyMetrics("RetryCorp")

    service.collect_and_save(company_names, 2023, 2023, "test.xlsx")

    # 재시도 수행 확인 (데이터 조회 호출됨)
    mock_financial_port.get_financial_statement.assert_called()
    
    # 메타데이터 저장 확인
    mock_repository_port.save_company_metadata.assert_called_once()
    saved_company = mock_repository_port.save_company_metadata.call_args[0][0]
    
    # 성공으로 업데이트 되었는지 확인
    assert 2023 in saved_company.success_years
    assert 2023 not in saved_company.failed_years
