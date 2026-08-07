"""Company 도메인 엔티티 단위 테스트.

주의: "N분기공시"(결산월 종료 후 순번상 N번째 분기, DisclosurePeriod가 계산하는 회계기수)와
"캘린더 N분기"(2026년 1~3월처럼 실제 달력상의 분기)는 서로 다른 개념이다.
to_calendar_period()는 전자를 후자로 변환하는 함수이므로, 테스트의 기댓값은 반드시
실제 공시의 종료일(캘린더 시간)을 기준으로 삼아야 한다.
"""

from core.domain.models.company import Company
from core.domain.models.disclosure_period import DisclosurePeriod


def test_to_calendar_period_december_settlement_unchanged():
    """12월 결산 기업은 회계기간과 캘린더기간이 동일합니다."""
    company = Company(code="001", name="A사", settlement_month=12)
    year, quarter = company.to_calendar_period(2026, "3Q")
    assert (year, quarter) == (2026, "3Q")


def test_to_calendar_period_march_settlement_q1():
    """3월 결산(회기 4월 시작) 기업의 1분기공시(4~6월)는 회계기수와 같은 해의 캘린더 2분기다.

    실제 사례: 이지케어텍(00490090, 결산월 3월)의 2026-08-07 실제 공시
    "분기보고서 (2026.06)" -> 회계기수상 1분기공시(4~6월, fiscal_year=2026) ->
    실제 시간은 2026년 4~6월이므로 캘린더로는 2026년 2분기여야 한다(전년도가 아님).
    """
    company = Company(code="002", name="B사", settlement_month=3)
    year, quarter = company.to_calendar_period(2026, "1Q")
    assert (year, quarter) == (2026, "2Q")


def test_to_calendar_period_march_settlement_annual():
    """3월 결산 기업의 사업보고서(4Q, 회계기수 마지막 분기=1~3월)는 회계기수 다음 해의 캘린더 1분기다."""
    company = Company(code="002", name="B사", settlement_month=3)
    year, quarter = company.to_calendar_period(2026, "4Q")
    assert (year, quarter) == (2027, "1Q")


def test_to_calendar_period_real_ezcaretech_disclosure_2026_08_07():
    """실제 공시(2026-08-07, 이지케어텍) 전 과정(제목 판별 -> 캘린더 변환) 회귀 검증."""
    period = DisclosurePeriod.from_report_title("분기보고서 (2026.06)", settlement_month=3)
    company = Company(code="00490090", name="이지케어텍", settlement_month=3)
    year, quarter = company.to_calendar_period(period.year, period.quarter)
    assert (year, quarter) == (2026, "2Q")


def test_to_calendar_period_real_grt_four_quarters():
    """실제 GRT(01170962, 6월 결산) 4개 분기 공시 - PIPELINE_MEMORY.md에 이미 검증된 실제 XBRL 종료일 기준.

    GRT_1분기보고서(제목상 2025.03) 실제 종료일 2025-03-31 -> 캘린더 2025년 1Q
    GRT_사업보고서 (제목상 2025.06)  실제 종료일 2025-06-30 -> 캘린더 2025년 2Q
    GRT_3분기보고서(제목상 2025.09)  실제 종료일 2025-09-30 -> 캘린더 2025년 3Q
    GRT_반기보고서 (제목상 2025.12)  실제 종료일 2025-12-31 -> 캘린더 2025년 4Q
    """
    company = Company(code="01170962", name="GRT", settlement_month=6)

    cases = [
        ("분기보고서 (2025.03)", (2025, "1Q")),
        ("사업보고서 (2025.06)", (2025, "2Q")),
        ("분기보고서 (2025.09)", (2025, "3Q")),
        ("반기보고서 (2025.12)", (2025, "4Q")),
    ]
    for report_nm, expected in cases:
        period = DisclosurePeriod.from_report_title(report_nm, settlement_month=6)
        result = company.to_calendar_period(period.year, period.quarter)
        assert result == expected, f"{report_nm}: expected {expected}, got {result}"
