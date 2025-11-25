"""성적표 및 재무 지표 모델."""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Optional


@dataclass
class FinancialMetrics:
    """재무 지표 (매출액, 영업이익, 당기순이익).
    
    Attributes:
        revenue: 매출액 (수익(매출액))
        operating_profit: 영업이익
        net_income: 당기순이익
    """
    revenue: Optional[Decimal] = None
    operating_profit: Optional[Decimal] = None
    net_income: Optional[Decimal] = None


@dataclass
class QuarterlyMetrics:
    """기업의 분기별 재무 지표.
    
    Attributes:
        corp_name: 기업명
        metrics_by_quarter: 분기별 지표 딕셔너리
            키: "2015Q1", "2015Q2", "2015Q3", "2015Q4", ...
            값: FinancialMetrics 객체
    """
    corp_name: str
    metrics_by_quarter: Dict[str, FinancialMetrics] = field(default_factory=dict)


@dataclass
class AnnualMetrics:
    """기업의 연간 재무 지표.
    
    Attributes:
        corp_name: 기업명
        metrics_by_year: 연도별 지표 딕셔너리
            키: 2015, 2016, 2017, ...
            값: FinancialMetrics 객체
    """
    corp_name: str
    metrics_by_year: Dict[int, FinancialMetrics] = field(default_factory=dict)
