"""Company 도메인 엔티티 단위 테스트."""

from core.domain.models.company import Company


def test_to_calendar_period_december_settlement_unchanged():
    """12월 결산 기업은 회계기간과 캘린더기간이 동일합니다."""
    company = Company(code="001", name="A사", settlement_month=12)
    year, quarter = company.to_calendar_period(2026, "3Q")
    assert (year, quarter) == (2026, "3Q")


def test_to_calendar_period_march_settlement_q1():
    """3월 결산(회기 4월 시작) 기업의 1분기는 전년도 2분기(4~6월)에 해당합니다."""
    company = Company(code="002", name="B사", settlement_month=3)
    year, quarter = company.to_calendar_period(2026, "1Q")
    assert (year, quarter) == (2025, "2Q")


def test_to_calendar_period_march_settlement_annual():
    """3월 결산 기업의 사업보고서(4Q)는 1~3월 캘린더 1분기에 해당합니다."""
    company = Company(code="002", name="B사", settlement_month=3)
    year, quarter = company.to_calendar_period(2026, "4Q")
    assert (year, quarter) == (2026, "1Q")
