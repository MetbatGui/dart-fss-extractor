"""DailyCollectionService 단위 테스트."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd

from core.domain.models.financial_statement import (
    FinancialStatement,
    ReportType,
    FinancialStatementType,
    AccountItem
)
from core.services.daily_collection_service import DailyCollectionService
from core.services.data_processing_service import DataProcessingService


@pytest.fixture
def mock_ports():
    """포트들의 Mock 인스턴스 묶음."""
    corp_code_port = MagicMock()
    financial_port = MagicMock()
    repository_port = MagicMock()
    
    # 기본 모킹 세팅 (A사 매칭용)
    corp_code_port.get_codes.return_value = ["001"]
    repository_port.load_company_metadata.return_value = None
    
    return corp_code_port, financial_port, repository_port


@pytest.fixture
def service(mock_ports):
    """DailyCollectionService 테스트 인스턴스."""
    cc_port, fin_port, repo_port = mock_ports
    proc_service = DataProcessingService()
    
    return DailyCollectionService(
        corp_code_port=cc_port,
        financial_port=fin_port,
        repository_port=repo_port,
        processing_service=proc_service
    )


def test_parse_report_period(service):
    """공시 제목 및 비고 필드를 통한 정밀 연도/분기/정정 여부 판별 검증."""
    # 1. 일반 1분기 공시
    r1 = service.parse_report_period("분기보고서 (2026.03)")
    assert r1["year"] == 2026
    assert r1["quarter"] == "1Q"
    assert r1["is_amendment"] is False

    # 2. 일반 반기 공시
    r2 = service.parse_report_period("반기보고서 (2026.06)")
    assert r2["quarter"] == "2Q"

    # 3. 일반 3분기 공시
    r3 = service.parse_report_period("분기보고서 (2026.09)")
    assert r3["quarter"] == "3Q"

    # 4. 연간 사업 보고서
    r4 = service.parse_report_period("사업보고서 (2025.12)")
    assert r4["year"] == 2025
    assert r4["quarter"] == "4Q"

    # 5. 기재정정 공시 검증
    r5 = service.parse_report_period("[기재정정]분기보고서 (2026.03)")
    assert r5["is_amendment"] is True

    # 6. rm(비고) 필드를 통한 우회 정정 검증
    r6 = service.parse_report_period("분기보고서 (2026.03)", rm="정")
    assert r6["is_amendment"] is True


def test_collect_daily_disclosures_filtering_and_routing(service, mock_ports):
    """당일 공시 목록 중 대상 기업만 정상 필터링하여 실적을 수집하는 시나리오 검증."""
    cc_port, fin_port, repo_port = mock_ports

    # 1. 오늘 들어온 공시 목록 모사 (대상 A사와 비대상 B사 섞임)
    disclosures = [
        {
            "corp_code": "001",
            "corp_name": "A사",
            "report_nm": "분기보고서 (2026.03)",
            "rcept_no": "202605290001",
            "rm": ""
        },
        {
            "corp_code": "002",
            "corp_name": "B사 (비대상)",
            "report_nm": "분기보고서 (2026.03)",
            "rcept_no": "202605290002",
            "rm": ""
        }
    ]
    fin_port.get_disclosures.return_value = disclosures

    # 2. DART 상세 재무제표 반환 모사 (1Q ~ 4Q)
    def mock_get_statement(code, year, rep_type):
        return FinancialStatement(
            corp_code=code,
            corp_name="A사",
            bsns_year=year,
            reprt_type=rep_type,
            fs_type=FinancialStatementType.CONSOLIDATED,
            accounts=[
                AccountItem("매출액", "1,000"),
                AccountItem("영업이익", "100"),
                AccountItem("당기순이익", "80")
            ]
        )
    fin_port.get_financial_statement.side_effect = mock_get_statement

    # 수집 수행
    result = service.collect_daily_disclosures(
        target_company_names=["A사"],
        start_date="20260529",
        end_date="20260529"
    )

    # 3. 단언 검증
    # A사(001)는 수집 성공 큐에 들어가고, B사(002)는 대상 외이므로 스킵되었음을 검증
    assert result["success"] == ["001"]
    assert result["failed"] == []
    
    # 4개 분기 보고서 조회가 정확히 호출되었는지 확인
    assert fin_port.get_financial_statement.call_count == 4
    
    # SQLite 저장소 적재(save_partition)가 정상 트리거되었는지 확인
    repo_port.save_partition.assert_called()
