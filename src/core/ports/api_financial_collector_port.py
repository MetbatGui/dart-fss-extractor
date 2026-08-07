"""OpenDART API REST 호출 기반 수집 포트 인터페이스."""

from abc import ABC, abstractmethod

from core.domain.models.financial_statement import (
    FinancialStatement,
    FinancialStatementType,
    ReportType,
)


class ApiFinancialCollectorPort(ABC):
    """OpenDART REST API (JSON) 기반 재무제표 수집 포트."""

    @property
    @abstractmethod
    def call_count(self) -> int:
        """API 호출 횟수를 반환합니다."""
        raise NotImplementedError

    @abstractmethod
    def get_financial_statement(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        prefer_consolidated: bool = True,
    ) -> FinancialStatement | None:
        """단일 재무제표 API 조회."""
        raise NotImplementedError

    @abstractmethod
    def get_all_statements(
        self, corp_code: str, year: int, report_type: ReportType
    ) -> dict[FinancialStatementType, FinancialStatement]:
        """연결 및 개별 재무제표를 각각 API로 조회."""
        raise NotImplementedError

    @abstractmethod
    def get_settlement_month(self, corp_code: str) -> int:
        """기업의 결산월을 조회합니다.

        Args:
            corp_code: DART 기업 코드

        Returns:
            결산월 (1~12, 기본값 12)
        """
        raise NotImplementedError
