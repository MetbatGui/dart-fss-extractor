"""FinancialStatement 도메인 엔티티의 풍부한 도메인 행동 단위 테스트."""

import pytest
from datetime import datetime
from core.domain.models.amount import Amount
from core.domain.models.financial_statement import (
    AccountItem,
    FinancialStatement,
    ReportType,
    FinancialStatementType,
)


def test_financial_statement_account_search():
    """우선순위 키워드 기반의 계정과목 검색 기능 검증."""
    accounts = [
        AccountItem("수익(매출액)", "10,000,000", "30,000,000", statement_type="IS"),
        AccountItem("영업이익", "1,500,000", "4,500,000", statement_type="IS"),
        # BS(자본)에 위치해 배제되어야 하는 가짜 당기순이익 항목 모사
        AccountItem("당기순이익", "5,000,000", statement_type="BS"), 
        AccountItem("당기순이익", "800,000", "2,400,000", statement_type="IS"),
    ]
    
    stmt = FinancialStatement(
        corp_code="00123456",
        corp_name="테스트전자",
        bsns_year=2026,
        reprt_type=ReportType.Q3,
        fs_type=FinancialStatementType.CONSOLIDATED,
        accounts=accounts
    )
    
    # 1. 일반 검색 검증
    revenue = stmt.find_account_amount(["매출액", "수익(매출액)"])
    assert revenue == Amount(10000000)
    
    # 2. 누적 금액 검색 검증
    revenue_cum = stmt.find_account_amount(["매출액", "수익(매출액)"], use_cumulative=True)
    assert revenue_cum == Amount(30000000)
    
    # 3. BS 배제 가드 및 손익 당기순이익 정상 검색 검증
    net_income = stmt.find_account_amount(["당기순이익"])
    assert net_income == Amount(800000)  # BS의 5,000,000이 아니라 IS의 800,000 매칭 검증


def test_financial_statement_split_revenue_handling():
    """통합 매출액이 없고 내수/수출로 쪼개진 특수 공시 양식 자동 합산 기능 검증."""
    accounts = [
        AccountItem("매출액(수출)", "7,000,000", statement_type="IS"),
        AccountItem("매출액(내수)", "3,000,000", statement_type="IS"),
        AccountItem("영업이익", "1,000,000", statement_type="IS"),
    ]
    
    stmt = FinancialStatement(
        corp_code="00123456",
        corp_name="수출기업",
        bsns_year=2026,
        reprt_type=ReportType.Q1,
        fs_type=FinancialStatementType.CONSOLIDATED,
        accounts=accounts
    )
    
    # 내수와 수출의 자동 합산 동작(10,000,000) 검증
    revenue = stmt.find_account_amount(["매출액", "영업수익"])
    assert revenue == Amount(10000000)


def test_financial_statement_scale_normalization():
    """자릿수가 다른(예: 원 단위 vs 천원 단위) 보고서 간의 자동 스케일 보정 기능 검증."""
    # 정상 스케일 (원 단위 표기)
    stmt_normal = FinancialStatement(
        corp_code="00123456",
        corp_name="스케일기업",
        bsns_year=2026,
        reprt_type=ReportType.Q1,
        fs_type=FinancialStatementType.CONSOLIDATED,
        accounts=[
            AccountItem("매출액", "10,000,000"),
            AccountItem("영업이익", "1,000,000"),
        ]
    )
    
    # 축소 스케일 (천원 단위 표기, 원본의 1/1000 스케일)
    stmt_scaled = FinancialStatement(
        corp_code="00123456",
        corp_name="스케일기업",
        bsns_year=2026,
        reprt_type=ReportType.SEMI_ANNUAL,
        fs_type=FinancialStatementType.CONSOLIDATED,
        accounts=[
            AccountItem("매출액", "20,000"),  # 원래 20,000,000이어야 함
            AccountItem("영업이익", "2,000"),   # 원래 2,000,000이어야 함
        ]
    )
    
    # 정규화 수행
    FinancialStatement.normalize_scales([stmt_normal, stmt_scaled])
    
    # 정상화된 결과 검증 (1000배 곱해져서 20,000,000으로 보정되었는지 확인)
    assert stmt_scaled.find_account_amount(["매출액"]) == Amount(20000000)
    assert stmt_scaled.find_account_amount(["영업이익"]) == Amount(20000000 / 10)


def test_quarterly_metrics_calculation():
    """QuarterlyMetrics.calculate_from_statements를 통한 분기 실적 복원 역산 검증."""
    from core.domain.models.performance_metrics import QuarterlyMetrics
    
    q1 = FinancialStatement("00123456", "테스트전자", 2026, ReportType.Q1, FinancialStatementType.CONSOLIDATED, [
        AccountItem("매출액", "10,000"),
        AccountItem("영업이익", "1,000"),
        AccountItem("당기순이익", "800")
    ])
    
    # 2분기 누적으로 25,000 공시 (단독 15,000 기대)
    semi = FinancialStatement("00123456", "테스트전자", 2026, ReportType.SEMI_ANNUAL, FinancialStatementType.CONSOLIDATED, [
        AccountItem("매출액", "15,000", "25,000"),
        AccountItem("영업이익", "1,500", "2,500"),
        AccountItem("당기순이익", "1,200", "2,000")
    ])
    
    # 3분기 누적으로 42,000 공시 (단독 17,000 기대)
    q3 = FinancialStatement("00123456", "테스트전자", 2026, ReportType.Q3, FinancialStatementType.CONSOLIDATED, [
        AccountItem("매출액", "17,000", "42,000"),
        AccountItem("영업이익", "1,700", "4,200"),
        AccountItem("당기순이익", "1,300", "3,300")
    ])
    
    # 연간 누적으로 60,000 공시 (4분기 단독 18,000 기대)
    annual = FinancialStatement("00123456", "테스트전자", 2026, ReportType.ANNUAL, FinancialStatementType.CONSOLIDATED, [
        AccountItem("매출액", "60,000", "60,000"),
        AccountItem("영업이익", "6,000", "6,000"),
        AccountItem("당기순이익", "4,800", "4,800")
    ])
    
    metrics = QuarterlyMetrics.calculate_from_statements(
        corp_name="테스트전자",
        q1_stmt=q1,
        semi_stmt=semi,
        q3_stmt=q3,
        annual_stmt=annual,
        revenue_kws=["매출액"],
        op_profit_kws=["영업이익"],
        net_income_kws=["당기순이익"]
    )
    
    assert metrics.metrics_by_quarter["1Q"].revenue == Amount(10000)
    assert metrics.metrics_by_quarter["2Q"].revenue == Amount(15000)
    assert metrics.metrics_by_quarter["3Q"].revenue == Amount(17000)
    assert metrics.metrics_by_quarter["4Q"].revenue == Amount(18000)  # 60,000 - 42,000 = 18,000


def test_quarterly_metrics_annual_sum():
    """수립된 분기 실적의 롤업(합산) 기능 검증."""
    from core.domain.models.performance_metrics import QuarterlyMetrics, FinancialMetrics
    
    metrics_dict = {
        "1Q": FinancialMetrics(1000, 100, 80),
        "2Q": FinancialMetrics(2000, 200, 160),
        "3Q": FinancialMetrics(3000, 300, 240),
        "4Q": FinancialMetrics(4000, 400, 320)
    }
    
    metrics = QuarterlyMetrics(corp_name="테스트전자", metrics_by_quarter=metrics_dict)
    annual_sum = metrics.calculate_annual_from_quarters()
    
    assert annual_sum.revenue == Amount(10000)
    assert annual_sum.operating_profit == Amount(1000)
    assert annual_sum.net_income == Amount(800)

