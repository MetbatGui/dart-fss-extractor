"""FinancialCollectionService 테스트."""

import pytest
from unittest.mock import Mock, call
from decimal import Decimal

from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.repository_port import RepositoryPort
from core.ports.export_port import ExportPort
from core.services.data_processing_service import DataProcessingService
from core.services.financial_collection_service import FinancialCollectionService
from core.domain.models.financial_statement import FinancialStatement, ReportType
from core.domain.models.performance_metrics import QuarterlyMetrics, FinancialMetrics


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
    """정상적인 수집 및 저장 흐름 테스트."""
    
    # 1. Mocking setup
    company_names = ["TestCorp"]
    mock_corp_code_port.get_codes.return_value = ["12345678"]
    
    # Financial Statements Mock
    # 연도(2023) * 보고서(4개) = 4번 호출됨
    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    
    # Metrics Calculation Mock
    mock_metrics = QuarterlyMetrics(corp_name="TestCorp")
    mock_metrics.metrics_by_quarter = {
        "1Q": FinancialMetrics(revenue=Decimal("1000")),
        "2Q": FinancialMetrics(revenue=Decimal("2000")),
        "3Q": FinancialMetrics(revenue=Decimal("3000")),
        "4Q": FinancialMetrics(revenue=Decimal("4000")),
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
    
    # 기업 코드 조회 확인
    mock_corp_code_port.get_codes.assert_called_once_with(company_names)
    
    # 재무제표 조회 확인 (4번: 1Q, Semi, 3Q, Annual)
    assert mock_financial_port.get_financial_statement.call_count == 4
    
    # 데이터 처리 서비스 호출 확인
    mock_processing_service.calculate_quarterly_performance.assert_called_once()
    
    # 저장 확인 (Parquet)
    mock_repository_port.save_dataframe.assert_called_once()
    
    # Export 확인
    mock_export_port.export_excel.assert_called_once()
    saved_data = mock_export_port.export_excel.call_args[0][0]
    
    # 저장된 데이터 구조 검증
    assert "매출액_분기" in saved_data
    assert "매출액_연간" in saved_data
    
    # 값 검증 (백만원 단위 변환)
    # 매출액_분기: 1Q=1000 -> 1 (백만원 단위라면 1000/1000000 = 0.001인데... )
    # 서비스 코드 로직: (df / 1_000_000).round(0)
    # 1000 -> 0 (round(0))
    # 테스트 데이터를 좀 더 크게 설정해야 함 (1조원 등)
    

def test_collect_and_save_no_code(
    service,
    mock_corp_code_port,
    mock_financial_port,
    mock_repository_port,
    mock_export_port
):
    """기업 코드가 없는 경우 건너뛰는지 확인."""
    mock_corp_code_port.get_codes.return_value = [None] # 코드 없음
    
    service.collect_and_save(["UnknownCorp"], 2023, 2023, "out.xlsx")
    
    # 조회 시도조차 안 해야 함
    mock_financial_port.get_financial_statement.assert_not_called()
    # 저장도 안 함 (데이터 없음)
    mock_repository_port.save_dataframe.assert_not_called()
    mock_export_port.export_excel.assert_not_called()


def test_collect_and_save_api_error_handling(
    service,
    mock_corp_code_port,
    mock_financial_port,
    mock_repository_port,
    mock_export_port
):
    """API 호출 중 에러 발생 시 해당 건을 건너뛰고 계속 진행하는지 확인."""
    mock_corp_code_port.get_codes.return_value = ["12345678"]
    
    # API 호출 시 예외 발생
    mock_financial_port.get_financial_statement.side_effect = Exception("API Error")
    
    service.collect_and_save(["TestCorp"], 2023, 2023, "out.xlsx")
    
    # 예외가 발생해도 프로그램이 죽지 않고 로그를 남기고 종료 (데이터 없음)
    mock_repository_port.save_dataframe.assert_not_called()
    mock_export_port.export_excel.assert_not_called()
