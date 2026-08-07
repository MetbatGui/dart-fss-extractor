"""로컬 XBRL 파일 파서 포트 인터페이스."""

from abc import ABC, abstractmethod

from core.domain.models.financial_statement import (
    FinancialStatement,
    FinancialStatementType,
    ReportType,
)


class XbrlParserPort(ABC):
    """로컬 XBRL ZIP 바이너리 데이터를 분석하여 재무제표 도메인 모델 사전을 만드는 포트."""

    @abstractmethod
    def parse_xbrl_zip(
        self,
        zip_data: bytes,
        corp_code: str,
        corp_name: str,
        year: int,
        report_type: ReportType,
        acc_month: int = 12,
    ) -> dict[FinancialStatementType, FinancialStatement]:
        """로컬 공시 원본(XBRL ZIP) 바이너리를 읽어 연결(CFS) 및 개별(OFS) FinancialStatement 객체 사전을 생성합니다.

        Args:
            zip_data: ZIP 파일 bytes
            corp_code: 기업 고유 고유번호 (8자리)
            corp_name: 기업명
            year: 대상 사업연도
            report_type: 보고서 타입 (1Q, 반기, 3Q, 사업보고서 등)
            acc_month: 기업의 결산월 (기본값: 12)

        Returns:
            {FinancialStatementType.CONSOLIDATED: CFS객체, FinancialStatementType.SEPARATE: OFS객체}
        """
        raise NotImplementedError
