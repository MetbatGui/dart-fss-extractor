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
