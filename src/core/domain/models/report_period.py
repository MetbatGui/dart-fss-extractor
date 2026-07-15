import re
from dataclasses import dataclass
from datetime import date
from typing import Optional

# 기간 코드 → (분기 번호, 한글 레이블, YTD 개월수)
_PERIOD_CODE_MAP = {
    "FQ": (1, "1Q", 3),
    "HY": (2, "2Q", 6),
    "TQ": (3, "3Q", 9),
    "FY": (4, "4Q", 12),
}

@dataclass
class XbrlReportPeriod:
    """XBRL 한 공시의 당기(CFY) 기간 메타데이터."""
    quarter: int          # 1=1분기(1Q), 2=반기(2Q), 3=3분기(3Q), 4=사업보고서(4Q)
    quarter_label: str    # "1Q" | "2Q" | "3Q" | "4Q"
    period_months: int    # YTD 누적 개월수 (3 / 6 / 9 / 12)
    start_date: date      # YTD 누적 시작일
    end_date: date        # 보고서 마감일


def parse_period_code(ctx_id: str) -> Optional[str]:
    """
    Context ID에서 기간 코드(FQ / HY / TQ / FY)를 추출합니다.
    CFY 기준 duration + Accumulated 컨텍스트만 대상으로 합니다.

    예:
        "CFY2026dFQA"               → "FQ"
        "CFY2025dTQA_...Member"     → "TQ"
    """
    m = re.match(r"CFY\d{4}d(FQ|HY|TQ|FY)A", ctx_id)
    if m:
        return m.group(1)
    return None


class XbrlPeriodParser:
    """XBRL context 구조를 분석하여 보고서 기간 메타데이터를 추출합니다."""

    @staticmethod
    def extract_period(root_element, namespaces: dict) -> Optional[XbrlReportPeriod]:
        """CFY + A suffix 컨텍스트에서 기간 정보를 추출합니다."""
        for ctx in root_element.findall(".//xbrli:context", namespaces):
            ctx_id = ctx.get("id", "")
            period_code = parse_period_code(ctx_id)
            if not period_code:
                continue

            period = ctx.find("xbrli:period", namespaces)
            if period is None:
                continue
            s_elem = period.find("xbrli:startDate", namespaces)
            e_elem = period.find("xbrli:endDate", namespaces)
            if s_elem is None or e_elem is None:
                continue

            try:
                s_date = date.fromisoformat(s_elem.text.strip())
                e_date = date.fromisoformat(e_elem.text.strip())
            except ValueError:
                continue

            quarter, label, months = _PERIOD_CODE_MAP[period_code]

            # 실제 날짜 기간 계산
            actual_months = (e_date.year - s_date.year) * 12 + (e_date.month - s_date.month) + 1

            return XbrlReportPeriod(
                quarter=quarter,
                quarter_label=label,
                period_months=actual_months,
                start_date=s_date,
                end_date=e_date,
            )

        return None
