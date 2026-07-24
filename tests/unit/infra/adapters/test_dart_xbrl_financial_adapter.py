"""DartXbrlFinancialAdapter 단위 테스트."""

import pytest
from unittest.mock import Mock, MagicMock
from pathlib import Path

from infra.adapters.dart_xbrl_financial_adapter import DartXbrlFinancialAdapter
from core.ports.download_port import DownloadPort
from core.ports.xbrl_parser_port import XbrlParserPort
from core.domain.models.financial_statement import FinancialStatement, ReportType, FinancialStatementType


@pytest.fixture
def mock_download_port():
    return Mock(spec=DownloadPort)


@pytest.fixture
def mock_xbrl_parser_port():
    return Mock(spec=XbrlParserPort)


def test_collect_from_disclosure_with_cached_file(tmp_path, mock_xbrl_parser_port):
    """로컬 캐시 ZIP 파일이 존재할 때 바로 파서로 넘겨 파싱하는지 검증."""
    rcept_no = "20260331000001"
    zip_file = tmp_path / f"{rcept_no}.zip"
    zip_file.write_bytes(b"dummy_zip_bytes")

    expected_stmt = Mock(spec=FinancialStatement)
    mock_xbrl_parser_port.parse_xbrl_zip.return_value = {
        FinancialStatementType.CONSOLIDATED: expected_stmt
    }

    adapter = DartXbrlFinancialAdapter(
        api_key="TEST_API_KEY",
        xbrl_parser_port=mock_xbrl_parser_port,
        cache_dir=tmp_path
    )

    result = adapter.collect_from_disclosure(
        rcept_no=rcept_no,
        corp_code="00126380",
        corp_name="삼성전자",
        year=2026,
        report_type=ReportType.Q1
    )

    assert FinancialStatementType.CONSOLIDATED in result
    assert result[FinancialStatementType.CONSOLIDATED] == expected_stmt
    mock_xbrl_parser_port.parse_xbrl_zip.assert_called_once()


def test_collect_from_disclosure_download_and_parse(tmp_path, mock_download_port, mock_xbrl_parser_port):
    """로컬 캐시가 없을 때 download_port로 다운로드 후 파싱하는지 검증."""
    rcept_no = "20260331000002"
    mock_download_port.download_xbrl_zip.return_value = b"downloaded_zip_bytes"
    mock_xbrl_parser_port.parse_xbrl_zip.return_value = {}

    adapter = DartXbrlFinancialAdapter(
        api_key="TEST_API_KEY",
        download_port=mock_download_port,
        xbrl_parser_port=mock_xbrl_parser_port,
        cache_dir=tmp_path
    )

    result = adapter.collect_from_disclosure(
        rcept_no=rcept_no,
        corp_code="00126380",
        corp_name="삼성전자",
        year=2026,
        report_type=ReportType.Q1
    )

    mock_download_port.download_xbrl_zip.assert_called_once_with(rcept_no)
    mock_xbrl_parser_port.parse_xbrl_zip.assert_called_once()
    assert (tmp_path / f"{rcept_no}.zip").is_file()
