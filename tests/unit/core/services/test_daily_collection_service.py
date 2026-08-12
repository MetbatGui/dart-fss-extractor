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
    cache_port = MagicMock()
    download_port = MagicMock()
    xbrl_parser_port = MagicMock()
    
    # 기본 모킹 세팅 (A사 매칭용)
    corp_code_port.get_codes.return_value = ["001"]
    repository_port.load_company_metadata.return_value = None
    cache_port.load_all.return_value = {}
    
    return corp_code_port, financial_port, repository_port, cache_port, download_port, xbrl_parser_port


@pytest.fixture
def service(mock_ports):
    """DailyCollectionService 테스트 인스턴스."""
    cc_port, fin_port, repo_port, cache_port, down_port, parser_port = mock_ports
    proc_service = DataProcessingService()
    
    return DailyCollectionService(
        corp_code_port=cc_port,
        financial_port=fin_port,
        repository_port=repo_port,
        cache_port=cache_port,
        download_port=down_port,
        xbrl_parser_port=parser_port,
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


def test_collect_daily_disclosures_does_not_fallback_to_api(service, mock_ports):
    """당일 공시 목록 중 대상 기업만 정상 필터링하여 실적을 수집하는 시나리오 검증."""
    cc_port, fin_port, repo_port, cache_port, down_port, parser_port = mock_ports

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
    def mock_get_all_statements(code, year, rep_type):
        stmt = FinancialStatement(
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
        return {FinancialStatementType.CONSOLIDATED: stmt}
    fin_port.get_all_statements.side_effect = mock_get_all_statements
    fin_port.get_settlement_month.return_value = 12

    # 수집 수행
    result = service.collect_daily_disclosures(
        target_company_names=["A사"],
        start_date="20260529",
        end_date="20260529"
    )

    # 3. 단언 검증
    # A사(001)는 수집 성공 큐에 들어가고, B사(002)는 대상 외이므로 스킵되었음을 검증
    assert result["success"] == []
    assert result["failed"] == ["001"]
    
    # 4개 분기 보고서 조회가 정확히 호출되었는지 확인
    fin_port.get_all_statements.assert_not_called()
    
    # SQLite 저장소 적재(save_partition)가 정상 트리거되었는지 확인
    repo_port.save_partition.assert_not_called()
    saved_company = repo_port.save_company_metadata.call_args.args[0]
    assert saved_company.failed_years == [2026]


def test_collect_daily_disclosures_with_local_xbrl_path(service, mock_ports):
    """로컬에 XBRL ZIP 캐시가 있거나 다운로드된 경우, API 대신 로컬 파서를 이용해 파싱 및 적재하는 시나리오 검증."""
    cc_port, fin_port, repo_port, cache_port, down_port, parser_port = mock_ports

    # 1. 공시 목록 세팅 (A사)
    disclosures = [{
        "corp_code": "001",
        "corp_name": "A사",
        "report_nm": "분기보고서 (2026.03)",
        "rcept_no": "202605290001",
        "rm": ""
    }]
    fin_port.get_disclosures.return_value = disclosures
    fin_port.get_settlement_month.return_value = 12

    # 2. 로컬 ZIP에서 파싱된 가상의 FinancialStatement 결과 모사
    # 연결재무제표만 반환
    stmt = FinancialStatement(
        corp_code="001",
        corp_name="A사",
        bsns_year=2026,
        reprt_type=ReportType.Q1,
        fs_type=FinancialStatementType.CONSOLIDATED,
        accounts=[
            AccountItem("매출액", "5000"),
            AccountItem("영업이익", "500"),
            AccountItem("당기순이익", "400")
        ]
    )
    parser_port.parse_xbrl_zip.return_value = {FinancialStatementType.CONSOLIDATED: stmt}
    down_port.download_xbrl_zip.return_value = b"mock_zip_bytes"

    # 3. 로컬 파일 시스템 모킹하여 캐시가 없는 상태 모사
    with patch("pathlib.Path.exists") as mock_exists, \
         patch("pathlib.Path.read_bytes") as mock_read, \
         patch("pathlib.Path.write_bytes") as mock_write, \
         patch("pathlib.Path.mkdir") as mock_mkdir:
         
        mock_exists.return_value = False  # 로컬 ZIP 파일 및 JSON 캐시 모두 미존재
        
        # 수집 실행
        result = service.collect_daily_disclosures(
            target_company_names=["A사"],
            start_date="20260529",
            end_date="20260529"
        )
        
        # 4. 검증
        assert result["success"] == ["001"]
        # download_port가 실제로 호출되어 ZIP 파일을 받아왔는지 검증
        down_port.download_xbrl_zip.assert_called_with("202605290001")
        # 받아온 ZIP 바이트로 로컬 파서가 호출되었는지 검증
        parser_port.parse_xbrl_zip.assert_called()
        # 데이터베이스 적재 검증
        repo_port.save_partition.assert_called()


def test_sync_ticker_name_caches_new_ticker(service):
    """처음 보는 corp_code는 조용히 캐시에 저장되고 dirty 플래그가 켜진다."""
    service._ticker_name_port = MagicMock()
    service._ticker_name_port.get_name_by_ticker.return_value = "LIG디펜스앤에어로스페이스"
    service._ticker_name_cache = {}

    service._sync_ticker_name("00503668", "079550")

    assert service._ticker_name_cache["00503668"]["name"] == "LIG디펜스앤에어로스페이스"
    assert service._ticker_name_cache["00503668"]["ticker"] == "079550"
    assert service._ticker_cache_dirty is True


def test_sync_ticker_name_detects_name_change(service, caplog):
    """캐시된 이름과 네이버가 돌려준 최신 이름이 다르면 충돌로 감지하고 갱신한다."""
    service._ticker_name_port = MagicMock()
    service._ticker_name_port.get_name_by_ticker.return_value = "LIG디펜스앤에어로스페이스"
    service._ticker_name_cache = {
        "00503668": {"ticker": "079550", "name": "LIG넥스원", "updated_at": "x"}
    }

    with caplog.at_level("INFO"):
        service._sync_ticker_name("00503668", "079550")

    assert service._ticker_name_cache["00503668"]["name"] == "LIG디펜스앤에어로스페이스"
    assert service._ticker_cache_dirty is True
    assert "종목명 변경 감지" in caplog.text


def test_sync_ticker_name_no_change_stays_clean(service):
    """이름이 그대로면 캐시를 건드리지 않고 dirty 플래그도 켜지지 않는다."""
    service._ticker_name_port = MagicMock()
    service._ticker_name_port.get_name_by_ticker.return_value = "LIG디펜스앤에어로스페이스"
    service._ticker_name_cache = {
        "00503668": {
            "ticker": "079550",
            "name": "LIG디펜스앤에어로스페이스",
            "updated_at": "x",
        }
    }
    service._ticker_cache_dirty = False

    service._sync_ticker_name("00503668", "079550")

    assert service._ticker_cache_dirty is False
