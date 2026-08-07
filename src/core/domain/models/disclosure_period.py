"""DART 공시 제목 기반 대상 기간 판별 도메인 모델."""

import re
from dataclasses import dataclass
from typing import Optional

from core.domain.models.financial_statement import ReportType

# 결산월-공시월 diff(개월) → (ReportType, 분기 라벨)
_QUARTER_MAPPING = {
    3: (ReportType.Q1, "1Q"),
    6: (ReportType.SEMI_ANNUAL, "2Q"),
    9: (ReportType.Q3, "3Q"),
    12: (ReportType.ANNUAL, "4Q"),
}


@dataclass
class DisclosurePeriod:
    """공시 제목에서 판별된 대상 회계연도/분기/정정 여부."""

    year: int
    quarter: str
    report_type: ReportType
    is_amendment: bool

    @classmethod
    def from_report_title(
        cls, report_nm: str, settlement_month: int = 12, rm: str = ""
    ) -> Optional["DisclosurePeriod"]:
        """보고서 제목 및 비고 필드에서 실제 DART 기준 대상 연도, 분기, 정정 여부를 판별합니다."""
        # 괄호 안의 YYYY.MM 패턴 검색
        match = re.search(r"\((\d{4})\.(\d{2})\)", report_nm)
        if not match:
            return None

        year_in_title = int(match.group(1))
        month_in_title = int(match.group(2))

        # 결산월과 공시월의 차이를 기반으로 DART 분기 판별 (일반 공식)
        diff = (month_in_title - settlement_month) % 12
        if diff == 0:
            diff = 12

        period = _QUARTER_MAPPING.get(diff)
        if not period:
            return None

        report_type, quarter_str = period

        # 결산월이 12월이면 연도 변환 불필요
        if settlement_month == 12:
            fiscal_year = year_in_title
        else:
            # 회기 시작월 계산 (예: 3월 결산 -> 4월 시작)
            start_month = (settlement_month % 12) + 1
            # 공시 기준월이 회기 시작월보다 크거나 같으면, 회기가 시작한 해와 공시 연도가 같음
            if month_in_title >= start_month:
                fiscal_year = year_in_title
            else:
                # 공시 기준월이 결산월 이하인 경우(해를 넘겨 공시된 경우), 회기가 시작한 해는 공시 연도 - 1
                fiscal_year = year_in_title - 1

        # 정정 여부 판별 (제목 내 기재정정/정정 텍스트 혹은 비고 필드 '정' 마크)
        is_amendment = (
            "[기재정정]" in report_nm or "정정" in report_nm or "정" in rm.strip()
        )

        return cls(
            year=fiscal_year,
            quarter=quarter_str,
            report_type=report_type,
            is_amendment=is_amendment,
        )
