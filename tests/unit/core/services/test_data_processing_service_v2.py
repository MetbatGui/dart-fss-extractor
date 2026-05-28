import pytest
from decimal import Decimal
from core.domain.models.performance_metrics import FinancialMetrics
from core.services.data_processing_service import DataProcessingService

def test_calculate_annual_from_quarters():
    service = DataProcessingService()
    
    metrics_by_quarter = {
        "1Q": FinancialMetrics(revenue=Decimal("100"), operating_profit=Decimal("10"), net_income=Decimal("5")),
        "2Q": FinancialMetrics(revenue=Decimal("200"), operating_profit=Decimal("20"), net_income=Decimal("10")),
        "3Q": FinancialMetrics(revenue=Decimal("300"), operating_profit=Decimal("30"), net_income=Decimal("15")),
        "4Q": FinancialMetrics(revenue=Decimal("400"), operating_profit=Decimal("40"), net_income=Decimal("20")),
    }
    
    annual = service.calculate_annual_from_quarters(metrics_by_quarter)
    
    assert annual.revenue == Decimal("1000")
    assert annual.operating_profit == Decimal("100")
    assert annual.net_income == Decimal("50")

def test_calculate_annual_from_partial_quarters():
    service = DataProcessingService()
    
    metrics_by_quarter = {
        "1Q": FinancialMetrics(revenue=Decimal("100"), operating_profit=Decimal("10"), net_income=Decimal("5")),
        "2Q": FinancialMetrics(revenue=None, operating_profit=None, net_income=None),
        "3Q": FinancialMetrics(revenue=Decimal("300"), operating_profit=Decimal("30"), net_income=Decimal("15")),
    }
    
    annual = service.calculate_annual_from_quarters(metrics_by_quarter)
    
    # 2Q is None, 4Q is missing. Sum what we have.
    assert annual.revenue == Decimal("400")
    assert annual.operating_profit == Decimal("40")
    assert annual.net_income == Decimal("20")

def test_calculate_annual_with_no_data():
    service = DataProcessingService()
    
    metrics_by_quarter = {
        "1Q": FinancialMetrics(None, None, None),
        "2Q": FinancialMetrics(None, None, None),
    }
    
    annual = service.calculate_annual_from_quarters(metrics_by_quarter)
    assert annual.revenue is None
    assert annual.operating_profit is None
    assert annual.net_income is None
