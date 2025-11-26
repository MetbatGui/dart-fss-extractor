"""어댑터 통합 테스트.

여러 어댑터가 함께 작동하는지 검증합니다.
- CorpCodeAdapter + DartFinancialAdapter
- 실제 DART API 호출
"""

import os
import csv
from pathlib import Path
import pytest

from infra.adapters.corp_code_adapter import CorpCodeAdapter
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from core.domain.models.financial_statement import ReportType


@pytest.fixture
def stock_list_path():
    """테스트 데이터 경로."""
    return Path(__file__).parent.parent / "fixtures" / "test_data" / "stock_list.csv"


@pytest.fixture
def corp_code_adapter():
    """기업코드 어댑터."""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        pytest.skip("DART_API_KEY 환경변수가 설정되지 않았습니다")
    return CorpCodeAdapter()


@pytest.fixture
def financial_adapter():
    """재무제표 어댑터."""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        pytest.skip("DART_API_KEY 환경변수가 설정되지 않았습니다")
    return DartFinancialAdapter(api_key=api_key, use_cache=False)  # 캐시 비활성화


def read_stock_list(csv_path: Path) -> list[str]:
    """CSV에서 종목명 읽기."""
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # 헤더 스킵
        return [row[0].strip() for row in reader if row]


def test_read_stock_list(stock_list_path):
    """Step 1: CSV 종목 리스트 읽기 테스트."""
    # Act
    stocks = read_stock_list(stock_list_path)
    
    # Assert
    assert len(stocks) == 2, "2개 종목이 있어야 합니다"
    assert "삼성전자" in stocks
    assert "AP위성" in stocks


def test_convert_company_names_to_codes(stock_list_path, corp_code_adapter):
    """Step 2: 기업명 → 기업코드 변환 테스트."""
    # Arrange
    stocks = read_stock_list(stock_list_path)
    
    # Act
    codes = corp_code_adapter.get_codes(stocks)
    
    # Assert
    assert len(codes) == 2, "2개 코드가 반환되어야 합니다"
    assert codes[0] is not None, "삼성전자 코드가 있어야 합니다"
    assert codes[0] == "00126380", "삼성전자 코드는 00126380입니다"
    print(f"삼성전자: {codes[0]}, AP위성: {codes[1]}")


def test_fetch_single_financial_statement(corp_code_adapter, financial_adapter):
    """Step 3: 단일 재무제표 조회 테스트 (삼성전자 2023년 연간)."""
    # Arrange
    samsung_code = corp_code_adapter.get_code("삼성전자")
    
    # Act
    statement = financial_adapter.get_financial_statement(
        corp_code=samsung_code,
        year=2023,
        report_type=ReportType.ANNUAL
    )
    
    # Assert
    assert statement is not None, "재무제표를 조회해야 합니다"
    assert statement.bsns_year == 2023
    assert len(statement.accounts) > 0, "계정과목이 있어야 합니다"
    
    # 매출액 찾기
    revenue_accounts = [acc for acc in statement.accounts if "매출" in acc.account_nm]
    print(f"\n매출 관련 계정과목: {[acc.account_nm for acc in revenue_accounts[:3]]}")
    assert len(revenue_accounts) > 0, "매출 관련 계정이 있어야 합니다"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
