"""DataProcessingService 테스트."""

import pytest
from decimal import Decimal
from datetime import date, datetime

from core.domain.models.financial_statement import FinancialStatement, AccountItem, ReportType, FinancialStatementType
from core.domain.models.performance_metrics import FinancialMetrics
from core.services.data_processing_service import DataProcessingService


@pytest.fixture
def service():
    return DataProcessingService()


def create_statement(revenue: str, op_profit: str, net_income: str, is_cumulative: bool = True) -> FinancialStatement:
    """테스트용 재무제표 생성 헬퍼."""
    accounts = []
    if revenue:
        accounts.append(AccountItem("매출액", revenue))
    if op_profit:
        accounts.append(AccountItem("영업이익", op_profit))
    if net_income:
        accounts.append(AccountItem("당기순이익", net_income))
        
    return FinancialStatement(
        corp_code="test",
        corp_name="Test Corp",
        bsns_year=2023,
        reprt_type=ReportType.ANNUAL,
        fs_type=FinancialStatementType.CONSOLIDATED,
        accounts=accounts,
        extracted_at=datetime.now(),
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        is_cumulative=is_cumulative
    )


def test_extract_metrics(service):
    """지표 추출 테스트."""
    stmt = create_statement("1,000", "100", "80")
    metrics = service.extract_metrics(stmt)
    
    assert metrics.revenue == Decimal("1000")
    assert metrics.operating_profit == Decimal("100")
    assert metrics.net_income == Decimal("80")


def test_calculate_quarterly_performance_normal(service):
    """일반적인 분기 실적 계산 (모두 누적 데이터)."""
    # Q1: 100
    q1 = create_statement("100", "10", "5", is_cumulative=True)
    # Semi: 250 (Q2=150)
    semi = create_statement("250", "25", "15", is_cumulative=True)
    # Q3: 450 (Q3=200)
    q3 = create_statement("450", "45", "25", is_cumulative=True)
    # Annual: 800 (Q4=350)
    annual = create_statement("800", "80", "45", is_cumulative=True)

    metrics = service.calculate_quarterly_performance(q1, semi, q3, annual)

    assert metrics.metrics_by_quarter["1Q"].revenue == Decimal("100")
    assert metrics.metrics_by_quarter["2Q"].revenue == Decimal("150")  # 250 - 100
    assert metrics.metrics_by_quarter["3Q"].revenue == Decimal("200")  # 450 - 250
    assert metrics.metrics_by_quarter["4Q"].revenue == Decimal("350")  # 800 - 450


def test_calculate_quarterly_performance_q3_separate(service):
    """3분기가 별도 데이터인 경우."""
    # Q1: 100
    q1 = create_statement("100", "10", "5", is_cumulative=True)
    # Semi: 250 (Q2=150)
    semi = create_statement("250", "25", "15", is_cumulative=True)
    # Q3: 200 (별도, Q3=200)
    q3 = create_statement("200", "20", "10", is_cumulative=False)
    # Annual: 800 (Q4=350)
    annual = create_statement("800", "80", "45", is_cumulative=True)

    metrics = service.calculate_quarterly_performance(q1, semi, q3, annual)

    assert metrics.metrics_by_quarter["1Q"].revenue == Decimal("100")
    assert metrics.metrics_by_quarter["2Q"].revenue == Decimal("150")
    assert metrics.metrics_by_quarter["3Q"].revenue == Decimal("200")  # 별도 그대로 사용
    
    # Q4 계산: 연간(800) - 3분기누적(반기250 + 3분기200 = 450) = 350
    assert metrics.metrics_by_quarter["4Q"].revenue == Decimal("350")


def test_calculate_quarterly_performance_missing_data(service):
    """데이터 누락 시 처리."""
    q1 = create_statement("100", "10", "5")
    # 반기 누락
    
    metrics = service.calculate_quarterly_performance(q1, None, None, None)
    
    assert metrics.metrics_by_quarter["1Q"].revenue == Decimal("100")
    assert metrics.metrics_by_quarter["2Q"].revenue is None
    assert metrics.metrics_by_quarter["3Q"].revenue is None
    assert metrics.metrics_by_quarter["4Q"].revenue is None
