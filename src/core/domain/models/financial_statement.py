"""재무제표 도메인 모델 - 최소 정의."""

from dataclasses import dataclass
from datetime import datetime
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
    extracted_at: datetime
