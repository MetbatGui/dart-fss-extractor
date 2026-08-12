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
    
    # 결산월 모킹 추가
    mock_financial_port.get_settlement_month.return_value = 12

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
    
    # 메타데이터 저장 확인 (생성 시 1번 + 완료 시 1번 = 총 2번)
    assert mock_repository_port.save_company_metadata.call_count == 2
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
    mock_financial_port.get_settlement_month.return_value = 12
    mock_repository_port.load_all.return_value = pd.DataFrame()

    # API 에러 발생
    mock_financial_port.get_financial_statement.side_effect = Exception("API Fail")

    service.collect_and_save(company_names, 2023, 2023, "test.xlsx")

    # 메타데이터 저장 확인 (생성 시 1번 + 실패 시 1번 = 총 2번)
    assert mock_repository_port.save_company_metadata.call_count == 2
    saved_company = mock_repository_port.save_company_metadata.call_args[0][0]
    
    # 2023년이 실패 목록에 있어야 함
    assert 2023 in saved_company.failed_years
    assert 2023 not in saved_company.success_years


def test_metadata_sync_ignores_years_with_no_valid_values(
    service,
    mock_corp_code_port,
    mock_financial_port,
    mock_processing_service,
    mock_repository_port,
    mock_export_port
):
    """저장소에 이미 존재하는 파티션 데이터를 메타데이터와 동기화(2-1단계)할 때,
    해당 연도의 모든 행이 값 없이(None) 비어있으면 success_years로 취급하면 안 된다.
    (과거 버그로 빈 행만 저장된 연도가 메타데이터 동기화 시점에 다시 success로
    재오염되어 영구히 재시도 대상에서 빠지는 것을 방지)
    """
    company_names = ["SyncCorp"]
    mock_corp_code_port.get_codes.return_value = ["33333333"]
    mock_repository_port.load_company_metadata.return_value = None
    mock_financial_port.get_settlement_month.return_value = 12
    mock_repository_port.exists.return_value = True

    # 2020년은 전부 빈 값(과거 버그로 오염), 2021년은 실제 값 보유
    existing_df = pd.DataFrame([
        {"기업명": "SyncCorp", "연도": 2020, "구분": "분기", "분기": "1Q", "구분_상세": "연결",
         "매출액": None, "영업이익": None, "당기순이익": None, "rcept_no": None},
        {"기업명": "SyncCorp", "연도": 2021, "구분": "분기", "분기": "1Q", "구분_상세": "연결",
         "매출액": 1000, "영업이익": 100, "당기순이익": 50, "rcept_no": None},
    ])
    mock_repository_port.load_partition.return_value = existing_df
    mock_repository_port.load_all.return_value = pd.DataFrame()

    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    mock_processing_service.calculate_quarterly_performance.return_value = QuarterlyMetrics(
        corp_name="SyncCorp"
    )
    mock_processing_service.calculate_annual_from_quarters.return_value = FinancialMetrics()

    # 요청 범위를 2020~2021로 좁혀서 실제 재수집 루프가 이 두 해만 스캔하도록 함
    service.collect_and_save(company_names, 2020, 2021, "test.xlsx", skip_failed=False)

    saved_company = mock_repository_port.save_company_metadata.call_args[0][0]
    assert 2020 not in saved_company.success_years
    assert 2021 in saved_company.success_years


def test_collect_and_save_marks_failure_when_all_metrics_are_empty(
    service,
    mock_corp_code_port,
    mock_financial_port,
    mock_processing_service,
    mock_repository_port,
    mock_export_port
):
    """계정과목 키워드가 전혀 매칭되지 않아 추출된 지표가 전부 None이면,
    success_years가 아닌 failed_years로 기록되어 다음 수집 때 재시도 대상이 되어야 한다.
    (실제 프로덕션에서 우리금융지주 등 60여개 기업이 빈 값으로 success 처리되어
    영구히 재시도되지 않고 방치된 버그의 재현 테스트)
    """
    company_names = ["EmptyCorp"]
    mock_corp_code_port.get_codes.return_value = ["11111111"]
    mock_repository_port.exists.return_value = False
    mock_repository_port.load_company_metadata.return_value = None
    mock_financial_port.get_settlement_month.return_value = 12
    mock_repository_port.load_all.return_value = pd.DataFrame()

    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)

    empty_metrics = QuarterlyMetrics(corp_name="EmptyCorp")
    empty_metrics.metrics_by_quarter = {
        "1Q": FinancialMetrics(), "2Q": FinancialMetrics(),
        "3Q": FinancialMetrics(), "4Q": FinancialMetrics(),
    }
    mock_processing_service.calculate_quarterly_performance.return_value = empty_metrics
    mock_processing_service.calculate_annual_from_quarters.return_value = FinancialMetrics()

    service.collect_and_save(company_names, 2023, 2023, "test.xlsx")

    saved_company = mock_repository_port.save_company_metadata.call_args[0][0]
    assert 2023 not in saved_company.success_years
    assert 2023 in saved_company.failed_years


def test_append_to_list_sets_detail_type_for_merge_compatibility(
    service,
    mock_corp_code_port,
    mock_financial_port,
    mock_processing_service,
    mock_repository_port,
    mock_export_port
):
    """새로 수집된 행에 '구분_상세'가 없으면, 기존 파티션(구분_상세 컬럼 보유)과
    concat 병합 시 신규 행의 값이 NaN이 되어 SQLite NOT NULL(detail_type) 제약 위반으로
    저장 자체가 실패하는 버그의 재현/회귀 테스트. 모든 신규 행에 '구분_상세'가 채워져야 한다.
    """
    company_names = ["MergeCorp"]
    mock_corp_code_port.get_codes.return_value = ["22222222"]
    mock_repository_port.load_company_metadata.return_value = None
    mock_financial_port.get_settlement_month.return_value = 12

    # 기존 파티션이 이미 존재하며 '구분_상세' 컬럼을 보유한 상황 (실제 load_partition 결과와 동일한 형태)
    mock_repository_port.exists.return_value = True
    existing_df = pd.DataFrame([{
        "기업명": "MergeCorp", "연도": 2022, "구분": "분기", "분기": "1Q", "구분_상세": "연결",
        "매출액": 500, "영업이익": 50, "당기순이익": 25, "rcept_no": None,
    }])
    mock_repository_port.load_partition.return_value = existing_df
    mock_repository_port.load_all.return_value = pd.DataFrame()

    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)

    metrics = QuarterlyMetrics(corp_name="MergeCorp")
    metrics.metrics_by_quarter = {"1Q": FinancialMetrics(revenue=Decimal("1000"))}
    mock_processing_service.calculate_quarterly_performance.return_value = metrics

    service.collect_and_save(company_names, 2023, 2023, "test.xlsx")

    saved_df = mock_repository_port.save_partition.call_args[0][2]
    assert "구분_상세" in saved_df.columns
    assert saved_df["구분_상세"].isna().sum() == 0


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
    
    # 결산월 모킹 추가
    mock_financial_port.get_settlement_month.return_value = 12
    
    mock_repository_port.load_all.return_value = pd.DataFrame() # 마지막 병합용

    # 이번엔 성공하도록 설정
    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    mock_processing_service.calculate_quarterly_performance.return_value = QuarterlyMetrics("RetryCorp")

    service.collect_and_save(company_names, 2023, 2023, "test.xlsx", skip_failed=False)

    # 재시도 수행 확인 (데이터 조회 호출됨)
    mock_financial_port.get_financial_statement.assert_called()
    
    # 메타데이터 저장 확인
    mock_repository_port.save_company_metadata.assert_called_once()
    saved_company = mock_repository_port.save_company_metadata.call_args[0][0]
    
    # 성공으로 업데이트 되었는지 확인
    assert 2023 in saved_company.success_years
    assert 2023 not in saved_company.failed_years
