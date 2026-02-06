"""DartResponseParser 테스트."""

from datetime import date
from core.domain.models.financial_statement import FinancialStatementType, ReportType
from infra.adapters.dart_response_parser import DartResponseParser

def test_parse_financial_statement_valid():
    """정상적인 API 응답 파싱 테스트."""
    # Arrange
    response_data = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20240312000736",
                "reprt_code": "11011",
                "bsns_year": "2023",
                "corp_code": "00126380",
                "stock_code": "005930",
                "fs_div": "CFS",
                "fs_nm": "연결재무제표",
                "sj_div": "BS",
                "sj_nm": "재무상태표",
                "account_nm": "유동자산",
                "thstrm_nm": "제 55 기",
                "thstrm_dt": "2023.01.01 ~ 2023.12.31",
                "thstrm_amount": "210,000,000,000",
                "frmtrm_nm": "제 54 기",
                "frmtrm_dt": "2022.01.01 ~ 2022.12.31",
                "frmtrm_amount": "190,000,000,000",
                "bfefrmtrm_nm": "제 53 기",
                "bfefrmtrm_dt": "2021.01.01 ~ 2021.12.31",
                "bfefrmtrm_amount": "170,000,000,000",
                "ord": "1",
                "currency": "KRW",
                "corp_name": "삼성전자"
            },
            {
                "rcept_no": "20240312000736",
                "reprt_code": "11011",
                "bsns_year": "2023",
                "corp_code": "00126380",
                "stock_code": "005930",
                "fs_div": "CFS",
                "fs_nm": "연결재무제표",
                "sj_div": "IS",
                "sj_nm": "손익계산서",
                "account_nm": "매출액",
                "thstrm_nm": "제 55 기",
                "thstrm_dt": "2023.01.01 ~ 2023.12.31",
                "thstrm_amount": "300,000,000,000",
                "corp_name": "삼성전자"
            }
        ]
    }

    # Act
    result = DartResponseParser.parse_financial_statement(
        response_data,
        corp_code="00126380",
        year=2023,
        report_type=ReportType.ANNUAL,
        fs_type=FinancialStatementType.CONSOLIDATED
    )

    # Assert
    assert result is not None
    assert result.corp_code == "00126380"
    assert result.corp_name == "삼성전자"
    assert result.bsns_year == 2023
    assert result.reprt_type == ReportType.ANNUAL
    assert result.fs_type == FinancialStatementType.CONSOLIDATED
    
    # 계정과목 확인
    assert len(result.accounts) == 2
    assert result.accounts[0].account_nm == "유동자산"
    assert result.accounts[0].thstrm_amount == "210,000,000,000"
    assert result.accounts[1].account_nm == "매출액"
    assert result.accounts[1].thstrm_amount == "300,000,000,000"

    # 날짜 확인
    assert result.start_date == date(2023, 1, 1)
    assert result.end_date == date(2023, 12, 31)
    assert result.is_cumulative is True


def test_parse_financial_statement_invalid_status():
    """API 상태 코드가 정상이 아닌 경우."""
    # Arrange
    response_data = {
        "status": "013",
        "message": "조회된 데이타가 없습니다."
    }

    # Act
    result = DartResponseParser.parse_financial_statement(
        response_data,
        corp_code="00126380",
        year=2023,
        report_type=ReportType.ANNUAL,
        fs_type=FinancialStatementType.CONSOLIDATED
    )

    # Assert
    assert result is None


def test_parse_financial_statement_empty_list():
    """데이터 리스트가 비어있는 경우."""
    # Arrange
    response_data = {
        "status": "000",
        "message": "정상",
        "list": []
    }

    # Act
    result = DartResponseParser.parse_financial_statement(
        response_data,
        corp_code="00126380",
        year=2023,
        report_type=ReportType.ANNUAL,
        fs_type=FinancialStatementType.CONSOLIDATED
    )

    # Assert
    assert result is None


def test_parse_date_info_cumulative():
    """누적 데이터 날짜 파싱 (1월 1일 시작)."""
    items = [{"thstrm_dt": "2023.01.01 ~ 2023.06.30"}]
    start_date, end_date, is_cumulative = DartResponseParser._parse_date_info(items)
    
    assert start_date == date(2023, 1, 1)
    assert end_date == date(2023, 6, 30)
    assert is_cumulative is True


def test_parse_date_info_separate():
    """별도 데이터 날짜 파싱 (1월 1일이 아닌 경우)."""
    items = [{"thstrm_dt": "2023.04.01 ~ 2023.06.30"}]
    start_date, end_date, is_cumulative = DartResponseParser._parse_date_info(items)
    
    assert start_date == date(2023, 4, 1)
    assert end_date == date(2023, 6, 30)
    assert is_cumulative is False


def test_parse_date_info_invalid_format():
    """날짜 형식이 잘못된 경우."""
    items = [{"thstrm_dt": "2023.01.01 - 2023.06.30"}] # ~ 대신 -
    start_date, end_date, is_cumulative = DartResponseParser._parse_date_info(items)
    
    assert start_date is None
    assert end_date is None
    assert is_cumulative is False
