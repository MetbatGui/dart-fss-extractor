"""재무제표 도메인 모델 - 최소 정의."""

from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import List, Optional


class ReportType(Enum):
    """보고서 타입."""
    ANNUAL = "11011"
    SEMI_ANNUAL = "11012"
    Q1 = "11013"
    Q3 = "11014"


class FinancialStatementType(Enum):
    """재무제표 구분."""
    CONSOLIDATED = "CFS"  # 연결
    SEPARATE = "OFS"      # 개별


@dataclass
class AccountItem:
    """계정과목 항목."""
    account_nm: str          # 계정과목명
    thstrm_amount: str       # 당기금액 (문자열로 받음)
    
    
@dataclass
class FinancialStatement:
    """재무제표 엔티티."""
    corp_code: str
    corp_name: str
    bsns_year: int
    reprt_type: ReportType
    fs_type: FinancialStatementType
    accounts: List[AccountItem]
    extracted_at: datetime = field(default_factory=datetime.now)
    
    # 기간 정보 (정확한 계산을 위해 추가)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    is_cumulative: bool = False  # True면 누적 데이터 (예: 1.1 ~ 6.30)
