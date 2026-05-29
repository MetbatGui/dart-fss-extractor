"""재무제표 조회 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Optional

from core.domain.models.financial_statement import (
    FinancialStatement,
    ReportType,
)


class FinancialStatementPort(ABC):
    """재무제표 조회 포트."""

    @property
    @abstractmethod
    def call_count(self) -> int:
        """현재 세션의 API 호출 횟수."""
        raise NotImplementedError

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

    @abstractmethod
    def get_disclosures(
        self,
        bgn_de: str,
        end_de: str,
        pblntf_ty: str = "A"
    ) -> list[dict]:
        """지정된 날짜 범위의 공시 목록 조회.
        
        Args:
            bgn_de: 시작일자 (YYYYMMDD)
            end_de: 종료일자 (YYYYMMDD)
            pblntf_ty: 공시유형 (기본값: 'A' - 정기공시)
            
        Returns:
            공시 목록 list of dict
        """
        raise NotImplementedError
