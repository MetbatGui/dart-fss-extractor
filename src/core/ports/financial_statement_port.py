"""재무제표 조회 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Optional

from src.core.domain.models.financial_statement import (
    FinancialStatement,
    ReportType,
)


class FinancialStatementPort(ABC):
    """재무제표 조회 포트."""

    @abstractmethod
    def get_financial_statement(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        prefer_consolidated: bool = True
    ) -> Optional[FinancialStatement]:
        """단일 재무제표 조회.
        
        Args:
            corp_code: 기업코드
            year: 연도
            report_type: 보고서 타입
            prefer_consolidated: 연결 우선 여부
        
        Returns:
            재무제표 or None
        """
        raise NotImplementedError
