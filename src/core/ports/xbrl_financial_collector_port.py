"""DART 공시 검색 및 로컬 XBRL 파싱 기반 수집 포트 인터페이스."""

from abc import ABC, abstractmethod

from core.domain.models.financial_statement import (
    FinancialStatement,
    FinancialStatementType,
    ReportType,
)


class XbrlFinancialCollectorPort(ABC):
    """공시 검색 및 로컬 XBRL ZIP 파싱 기반 수집 포트."""

    @abstractmethod
    def get_disclosures(
        self, bgn_de: str, end_de: str, pblntf_ty: str = "A"
    ) -> list[dict]:
        """지정된 날짜 범위의 정기 공시 목록 조회."""
        raise NotImplementedError

    @abstractmethod
    def collect_from_disclosure(
        self,
        rcept_no: str,
        corp_code: str,
        corp_name: str,
        year: int,
        report_type: ReportType,
        acc_month: int = 12,
    ) -> dict[FinancialStatementType, FinancialStatement]:
        """접수번호(rcept_no)에 대해 XBRL ZIP 다운로드 및 로컬 XML 파싱을 거쳐 재무제표 맵 반환."""
        raise NotImplementedError
