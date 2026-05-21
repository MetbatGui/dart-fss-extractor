
import pytest
from unittest.mock import Mock, call, MagicMock
from decimal import Decimal
import pandas as pd
from core.domain.models.company import Company
from core.domain.models.performance_metrics import QuarterlyMetrics, FinancialMetrics
from core.domain.models.financial_statement import FinancialStatement
from core.services.financial_collection_service import FinancialCollectionService
from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.repository_port import RepositoryPort
from core.ports.export_port import ExportPort
from core.services.data_processing_service import DataProcessingService

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

def test_smart_skip_if_fully_collected(
    service,
    mock_corp_code_port,
    mock_repository_port,
    mock_financial_port
):
    """요청한 연도(2023~2024)가 이미 성공 이력에 있으면 수집을 건너뛰는지 확인."""
    company_names = ["SkipCorp"]
    mock_corp_code_port.get_codes.return_value = ["111111"]
    
    # 이미 성공한 이력 존재
    existing_company = Company(code="111111", name="SkipCorp")
    existing_company.success_years = [2023, 2024]
    mock_repository_port.load_company_metadata.return_value = existing_company
    
    service.collect_and_save(company_names, 2023, 2024, "test.xlsx")
    
    # API 호출이 없어야 함
    mock_financial_port.get_financial_statement.assert_not_called()
    # 저장도 불필요 (건너뜀)
    mock_repository_port.save_partition.assert_not_called()

def test_merge_with_existing_data(
    service,
    mock_corp_code_port,
    mock_repository_port,
    mock_financial_port,
    mock_processing_service
):
    """기존 데이터(2023)가 있을 때, 새 데이터(2024)를 병합하여 저장하는지 확인."""
    company_names = ["MergeCorp"]
    Code = "222222"
    mock_corp_code_port.get_codes.return_value = [Code]
    
    # 1. 기존 상태 설정: 2023년 성공
    existing_company = Company(code=Code, name="MergeCorp")
    existing_company.success_years = [2023]
    mock_repository_port.load_company_metadata.return_value = existing_company
    
    # 기존 파티션 데이터 (2023년)
    existing_df = pd.DataFrame([{
        "기업명": "MergeCorp", "연도": 2023, "구분": "분기", "분기": "1Q", "매출액": 100
    }])
    mock_repository_port.exists.return_value = True
    mock_repository_port.load_partition.return_value = existing_df
    
    # 2. 새 데이터 수집 (2024년 요청)
    # 2023은 건너뛰고 2024만 수집될 것임
    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    
    mock_metrics = QuarterlyMetrics(corp_name="MergeCorp")
    mock_metrics.metrics_by_quarter = {
        "1Q": FinancialMetrics(revenue=Decimal("200"))
    }
    mock_processing_service.calculate_quarterly_performance.return_value = mock_metrics

    # 실행
    service.collect_and_save(company_names, 2023, 2024, "test.xlsx")

    # 3. 검증
    # load_partition 호출 확인
    mock_repository_port.load_partition.assert_called_with("financial_data_raw", Code)
    
    # save_partition 호출 확인 (병합된 데이터)
    mock_repository_port.save_partition.assert_called_once()
    saved_df = mock_repository_port.save_partition.call_args[0][2]
    
    # 병합 결과 확인: 2023(기존) + 2024(신규 분기) + 2024(신규 연간) = 3행
    assert len(saved_df) == 3
    assert 2023 in saved_df["연도"].values
    assert 2024 in saved_df["연도"].values
    assert saved_df[saved_df["연도"] == 2023]["매출액"].iloc[0] == 100
    assert saved_df[saved_df["연도"] == 2024]["매출액"].iloc[0] == 200

def test_non_contiguous_gap_filling(
    service,
    mock_corp_code_port,
    mock_repository_port,
    mock_financial_port,
    mock_processing_service
):
    """2022, 2024가 있는 상태에서 2023년 누락 데이터를 수집하여 병합하는지 확인."""
    company_names = ["GapCorp"]
    Code = "333333"
    mock_corp_code_port.get_codes.return_value = [Code]
    
    # 1. 기존 데이터: 2022, 2024년 존재
    existing_company = Company(code=Code, name="GapCorp")
    existing_company.success_years = [2022, 2024]
    mock_repository_port.load_company_metadata.return_value = existing_company
    
    existing_df = pd.DataFrame([
        {"기업명": "GapCorp", "연도": 2022, "구분": "분기", "분기": "1Q", "매출액": 100},
        {"기업명": "GapCorp", "연도": 2024, "구분": "분기", "분기": "1Q", "매출액": 300},
    ])
    mock_repository_port.exists.return_value = True
    mock_repository_port.load_partition.return_value = existing_df
    
    # 2. 수집 요청 (2022~2024) -> 2023만 수집 시도
    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    mock_metrics = QuarterlyMetrics(corp_name="GapCorp")
    mock_metrics.metrics_by_quarter = {"1Q": FinancialMetrics(revenue=Decimal("200"))}
    mock_processing_service.calculate_quarterly_performance.return_value = mock_metrics
    
    # 실행
    service.collect_and_save(company_names, 2022, 2024, "test.xlsx")
    
    # 3. 검증
    mock_repository_port.save_partition.assert_called_once()
    saved_df = mock_repository_port.save_partition.call_args[0][2]
    
    # 2022, 2023, 2024 모두 존재해야 함 (연간 데이터까지 포함되므로 총 4~5행 예상되나 현재 로직상 2023분기+2023연간 추가)
    assert 2023 in saved_df["연도"].values
    assert len(saved_df[saved_df["연도"] == 2023]) >= 1
    # 정렬 확인
    years = saved_df["연도"].tolist()
    assert years == sorted(years)

def test_duplicate_handling_keep_last(
    service,
    mock_corp_code_port,
    mock_repository_port,
    mock_financial_port,
    mock_processing_service
):
    """이미 있는 연도(2023)를 다시 수집할 때 최신 데이터(last)로 덮어쓰는지 확인."""
    company_names = ["DupCorp"]
    Code = "444444"
    mock_corp_code_port.get_codes.return_value = [Code]
    
    # 1. 기존 데이터: 2023년 존재 (중복 업데이트 테스트를 위해 2023을 실패 목록에 넣고 skip_failed=False로 호출)
    existing_company = Company(code=Code, name="DupCorp")
    existing_company.success_years = [2023]
    existing_company.failed_years = [2023] 
    mock_repository_port.load_company_metadata.return_value = existing_company
    
    existing_df = pd.DataFrame([{
        "기업명": "DupCorp", "연도": 2023, "구분": "분기", "분기": "1Q", "매출액": 100, "영업이익": 0, "당기순이익": 0
    }])
    mock_repository_port.exists.return_value = True
    mock_repository_port.load_partition.return_value = existing_df
    
    # 2. 새 데이터 (매출액 999로 변경)
    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    mock_metrics = QuarterlyMetrics(corp_name="DupCorp")
    mock_metrics.metrics_by_quarter = {"1Q": FinancialMetrics(revenue=Decimal("999"))}
    mock_processing_service.calculate_quarterly_performance.return_value = mock_metrics
    
    # 3. 검증
    # load_all이 비어있어도 save_partition은 그 전에 호출되어야 함
    mock_repository_port.load_all.return_value = pd.DataFrame([{
        "기업명": "DupCorp", "연도": 2023, "구분": "분기", "분기": "1Q", "매출액": 100
    }]) 
    
    # 4. 실행 (강제 재수집, 실패 기록 무시)
    service.collect_and_save(company_names, 2023, 2023, "test.xlsx", skip_failed=False, force_recollect=True)
    
    # 4. 검증
    call_args_list = mock_repository_port.save_partition.call_args_list
    assert len(call_args_list) > 0, "save_partition was not called!"
    
    saved_df = call_args_list[0][0][2]

    # 2023 1Q 데이터가 하나여야 하고, 값은 999여야 함
    q1_2023 = saved_df[(saved_df["연도"] == 2023) & (saved_df["분기"] == "1Q")]
    assert len(q1_2023) == 1
    assert q1_2023["매출액"].iloc[0] == 999


def test_sync_and_skip_failed(service, mock_repository_port, mock_corp_code_port, mock_financial_port, mock_processing_service):
    """저장소 동기화(Sync) 및 실패 연도 스킵 기능을 테스트합니다."""
    # 1. 설정
    name = "SyncCorp"
    code = "777777"
    mock_corp_code_port.get_codes.return_value = [code]
    
    # 메타데이터: 2021년만 성공, 2022년은 실패 이력
    existing_company = Company(code=code, name=name)
    existing_company.success_years = [2021]
    existing_company.failed_years = [2022]
    mock_repository_port.load_company_metadata.return_value = existing_company
    
    # 실제 저장소 파일에는 2023년 데이터가 이미 있음 (메타데이터와 불일치 상황)
    mock_repository_port.exists.return_value = True
    repo_df = pd.DataFrame([
        {"기업명": name, "연도": 2021, "구분": "분기", "분기": "1Q", "매출액": 100},
        {"기업명": name, "연도": 2023, "구분": "분기", "분기": "1Q", "매출액": 300},
    ])
    mock_repository_port.load_partition.return_value = repo_df
    
    # 수집 시도: 2021 ~ 2024
    # 2021: 메타데이터상 성공 -> 스킵
    # 2023: 저장소 데이터 발견 -> 동기화 후 스킵
    # 2022: 실패 이력 -> skip_failed=True면 스킵
    # 2024: 신규 -> 수집 시도
    
    # Mock responses for collection (2024 or 2022)
    mock_financial_port.get_financial_statement.return_value = Mock(spec=FinancialStatement)
    mock_metrics = QuarterlyMetrics(corp_name=name)
    mock_metrics.metrics_by_quarter = {"1Q": FinancialMetrics(revenue=Decimal("400"), operating_profit=0, net_income=0)}
    mock_processing_service.calculate_quarterly_performance.return_value = mock_metrics

    # Mock load_all to return all data for excel generation check
    repo_df["영업이익"] = 0
    repo_df["당기순이익"] = 0
    all_data = pd.concat([repo_df, pd.DataFrame([
        {"기업명": name, "연도": 2024, "구분": "분기", "분기": "1Q", "매출액": 400, "영업이익": 0, "당기순이익": 0, "기간": "2024.1Q"}
    ])])
    mock_repository_port.load_all.return_value = all_data

    # 2. 실행 (skip_failed=True)
    service.collect_and_save([name], 2015, 2024, "test.xlsx", skip_failed=True)
    
    # 3. 검증
    # 2023년이 성공 목록에 들어갔어야 함
    assert 2023 in existing_company.success_years
    
    # save_partition은 2024년 데이터가 추가된 상태로 저장되어야 함
    call_args_list = mock_repository_port.save_partition.call_args_list
    assert len(call_args_list) > 0
    saved_df = call_args_list[0][0][2]
    
    # 2022년은 수집되지 않아야 함 (스킵됨)
    assert 2022 not in saved_df["연도"].values
    # 2024년은 수집되어야 함
    assert 2024 in saved_df["연도"].values
    
    # 4. 재실행 (skip_failed=False 로 강제 재시도 테스트)
    mock_repository_port.save_partition.reset_mock()
    service.collect_and_save([name], 2021, 2024, "test.xlsx", skip_failed=False)
    
    # 이제 2022년도 수집 대상에 포함되어야 함 (adapter가 2022년에 대해 호출될 것)
    # (여기서는 간단히 save_partition 호출 여부만 확인하거나 adapter 호출 카운트 확인 가능)
    saved_df_retry = mock_repository_port.save_partition.call_args[0][2]
    # 2022년이 포함되어 저장되었는지 확인 (adapter가 2022 처리했다고 가정)
    assert 2022 in saved_df_retry["연도"].values
