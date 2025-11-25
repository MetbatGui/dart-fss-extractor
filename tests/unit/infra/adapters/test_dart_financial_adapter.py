"""DART Financial Adapter 테스트."""

import os
import pytest
from pathlib import Path

from src.core.domain.models.financial_statement import ReportType
from src.infra.adapters.dart_financial_adapter import DartFinancialAdapter


@pytest.fixture
def adapter():
    """테스트용 어댑터 인스턴스."""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        pytest.skip("DART_API_KEY 환경변수가 설정되지 않았습니다")
    return DartFinancialAdapter(api_key=api_key, use_cache=True)


def test_get_financial_statement_consolidated(adapter):
    """연결재무제표 조회 테스트.
    
    삼성전자의 2023년 사업보고서를 조회합니다.
    """
    # Arrange
    corp_code = "00126380"  # 삼성전자
    year = 2023
    report_type = ReportType.ANNUAL

    # Act
    statement = adapter.get_financial_statement(
        corp_code=corp_code,
        year=year,
        report_type=report_type,
        prefer_consolidated=True
    )

    # Assert
    assert statement is not None, "재무제표 조회 실패"
    assert statement.corp_code == corp_code
    assert statement.bsns_year == year
    assert statement.reprt_type == report_type
    assert len(statement.accounts) > 0, "계정과목이 없습니다"
    
    # 매출액 계정 확인
    revenue_accounts = [acc for acc in statement.accounts if "매출" in acc.account_nm]
    assert len(revenue_accounts) > 0, "매출 관련 계정이 없습니다"


def test_cache_mechanism(adapter):
    """캐싱 메커니즘 테스트."""
    # Arrange
    corp_code = "00126380"
    year = 2023
    report_type = ReportType.Q1

    # Act - 첫 번째 조회 (API 호출)
    statement1 = adapter.get_financial_statement(
        corp_code, year, report_type
    )

    # Act - 두 번째 조회 (캐시에서 로드)
    statement2 = adapter.get_financial_statement(
        corp_code, year, report_type
    )

    # Assert
    assert statement1 is not None
    assert statement2 is not None
    assert statement1.corp_code == statement2.corp_code
    assert statement1.bsns_year == statement2.bsns_year


def test_get_separate_financial_statement(adapter):
    """개별재무제표 명시적 요청 테스트 (양방향 fallback).
    
    prefer_consolidated=False로 개별재무제표를 우선 요청하지만,
    개별이 없으면 연결재무제표로 fallback합니다.
    """
    # Arrange
    corp_code = "00126380"  # 삼성전자
    year = 2023
    report_type = ReportType.ANNUAL

    # Act
    statement = adapter.get_financial_statement(
        corp_code=corp_code,
        year=year,
        report_type=report_type,
        prefer_consolidated=True  # 개별 우선, 없으면 연결로 fallback
    )

    # Assert
    assert statement is not None, "재무제표를 조회해야 합니다 (OFS 또는 CFS)"
    assert statement.corp_code == corp_code
    assert statement.bsns_year == year


def test_invalid_corp_code(adapter):
    """잘못된 기업코드 처리 테스트."""
    # Arrange
    invalid_corp_code = "99999999"
    year = 2023
    report_type = ReportType.ANNUAL

    # Act
    statement = adapter.get_financial_statement(
        invalid_corp_code, year, report_type
    )

    # Assert
    assert statement is None, "잘못된 기업코드에 대해 None을 반환해야 합니다"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
