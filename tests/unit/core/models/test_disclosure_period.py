"""DisclosurePeriod 도메인 모델 단위 테스트."""

from core.domain.models.disclosure_period import DisclosurePeriod
from core.domain.models.financial_statement import ReportType


def test_from_report_title_q1():
    """일반 1분기 공시 판별 검증."""
    period = DisclosurePeriod.from_report_title("분기보고서 (2026.03)")
    assert period.year == 2026
    assert period.quarter == "1Q"
    assert period.report_type == ReportType.Q1
    assert period.is_amendment is False


def test_from_report_title_semi_annual():
    """일반 반기 공시 판별 검증."""
    period = DisclosurePeriod.from_report_title("반기보고서 (2026.06)")
    assert period.quarter == "2Q"


def test_from_report_title_q3():
    """일반 3분기 공시 판별 검증."""
    period = DisclosurePeriod.from_report_title("분기보고서 (2026.09)")
    assert period.quarter == "3Q"


def test_from_report_title_annual():
    """연간 사업보고서 판별 검증."""
    period = DisclosurePeriod.from_report_title("사업보고서 (2025.12)")
    assert period.year == 2025
    assert period.quarter == "4Q"


def test_from_report_title_amendment_in_title():
    """제목 내 기재정정 텍스트를 통한 정정 여부 검증."""
    period = DisclosurePeriod.from_report_title("[기재정정]분기보고서 (2026.03)")
    assert period.is_amendment is True


def test_from_report_title_amendment_via_rm():
    """rm(비고) 필드를 통한 우회 정정 검증."""
    period = DisclosurePeriod.from_report_title("분기보고서 (2026.03)", rm="정")
    assert period.is_amendment is True


def test_from_report_title_no_match_returns_none():
    """제목에서 (YYYY.MM) 패턴을 찾지 못하면 None을 반환합니다."""
    assert DisclosurePeriod.from_report_title("분기보고서") is None


def test_from_report_title_non_december_settlement():
    """비12월 결산 기업의 회계연도 보정 검증 (3월 결산, 4월 시작 회기)."""
    # 3월 결산 기업의 회기 시작월은 4월. 3월 공시는 직전 회기(전년도)에 속함.
    period = DisclosurePeriod.from_report_title("사업보고서 (2026.03)", settlement_month=3)
    assert period.year == 2025
    assert period.quarter == "4Q"
