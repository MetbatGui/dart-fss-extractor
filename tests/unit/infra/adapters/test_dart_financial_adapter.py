"""DART Financial Adapter 테스트."""

import os
import json
import pytest
from datetime import date
from unittest.mock import patch, MagicMock
from pathlib import Path

from core.domain.models.financial_statement import ReportType, FinancialStatementType
from infra.adapters.dart_financial_adapter import DartFinancialAdapter


@pytest.fixture
def mock_api_response():
    """API 응답 Mock 데이터."""
    return {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20240312000736",
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "thstrm_dt": "2023.01.01 ~ 2023.12.31",
                "account_nm": "매출액",
                "thstrm_amount": "1000000000"
            }
        ]
    }


@pytest.fixture
def adapter():
    """테스트용 어댑터 인스턴스."""
    with patch.dict(os.environ, {"DART_API_KEY": "dummy_key"}):
        return DartFinancialAdapter(use_cache=False)


def test_get_financial_statement_consolidated(adapter, mock_api_response):
    """연결재무제표 조회 테스트."""
    # Arrange
    corp_code = "00126380"
    year = 2023
    report_type = ReportType.ANNUAL

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Act
        statement = adapter.get_financial_statement(
            corp_code=corp_code,
            year=year,
            report_type=report_type,
            prefer_consolidated=True
        )

    # Assert
    assert statement is not None
    assert statement.corp_code == corp_code
    assert statement.bsns_year == year
    assert statement.start_date == date(2023, 1, 1)
    assert statement.end_date == date(2023, 12, 31)
    assert statement.is_cumulative is True
    assert len(statement.accounts) == 1
    assert statement.accounts[0].account_nm == "매출액"


def test_date_parsing_cumulative(adapter):
    """누적 데이터 날짜 파싱 테스트."""
    # Arrange
    response_data = {
        "status": "000",
        "list": [{
            "corp_code": "00126380",
            "corp_name": "삼성전자",
            "thstrm_dt": "2023.01.01 ~ 2023.09.30",  # 3분기 누적
            "account_nm": "매출액",
            "thstrm_amount": "100"
        }]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = response_data
        
        # Act
        statement = adapter.get_financial_statement("00126380", 2023, ReportType.Q3)

    # Assert
    assert statement.start_date == date(2023, 1, 1)
    assert statement.end_date == date(2023, 9, 30)
    assert statement.is_cumulative is True


def test_date_parsing_separate(adapter):
    """별도(3개월) 데이터 날짜 파싱 테스트."""
    # Arrange
    response_data = {
        "status": "000",
        "list": [{
            "corp_code": "00126380",
            "corp_name": "삼성전자",
            "thstrm_dt": "2023.07.01 ~ 2023.09.30",  # 3분기 별도
            "account_nm": "매출액",
            "thstrm_amount": "100"
        }]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = response_data
        
        # Act
        statement = adapter.get_financial_statement("00126380", 2023, ReportType.Q3)

    # Assert
    assert statement.start_date == date(2023, 7, 1)
    assert statement.end_date == date(2023, 9, 30)
    assert statement.is_cumulative is False


def test_api_error_handling(adapter):
    """API 에러 처리 테스트."""
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = {"status": "013", "message": "데이터 없음"}
        
        statement = adapter.get_financial_statement("00126380", 2023, ReportType.ANNUAL)
        
    assert statement is None


def test_get_disclosures_success(adapter):
    """공시 검색 API 정상 조회 및 결과 매핑 테스트."""
    mock_list_response = {
        "status": "000",
        "message": "정상",
        "total_page": "1",
        "list": [
            {
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "stock_code": "005930",
                "report_nm": "분기보고서 (2026.03)",
                "rcept_no": "20260515000123",
                "rm": ""
            }
        ]
    }

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = mock_list_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        disclosures = adapter.get_disclosures("20260515", "20260515")

    assert len(disclosures) == 1
    assert disclosures[0]["corp_name"] == "삼성전자"
    assert disclosures[0]["report_nm"] == "분기보고서 (2026.03)"


def test_get_disclosures_pagination(adapter):
    """공시 목록 조회 시 2페이지 이상 다중 페이지네이션 작동 테스트."""
    # 1페이지 응답 모사
    page1_response = {
        "status": "000",
        "total_page": "2",
        "list": [{"corp_name": "삼성전자", "report_nm": "1페이지보고서"}]
    }
    # 2페이지 응답 모사
    page2_response = {
        "status": "000",
        "total_page": "2",
        "list": [{"corp_name": "현대자동차", "report_nm": "2페이지보고서"}]
    }

    with patch("requests.get") as mock_get:
        mock_get.side_effect = [
            MagicMock(json=lambda: page1_response, raise_for_status=lambda: None),
            MagicMock(json=lambda: page2_response, raise_for_status=lambda: None),
        ]

        disclosures = adapter.get_disclosures("20260515", "20260515")

    assert len(disclosures) == 2
    assert disclosures[0]["report_nm"] == "1페이지보고서"
    assert disclosures[1]["report_nm"] == "2페이지보고서"

