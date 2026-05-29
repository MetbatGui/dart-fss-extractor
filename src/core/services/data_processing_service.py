"""데이터 처리 및 변환 서비스 - 도메인 오케스트레이션 설계 적용."""

import logging
from typing import List, Optional, Dict

from core.domain.models.financial_statement import FinancialStatement, FinancialStatementType
from core.domain.models.performance_metrics import FinancialMetrics, QuarterlyMetrics

logger = logging.getLogger(__name__)


class DataProcessingService:
    """재무 데이터 정제 및 처리를 위한 오케스트레이션 서비스.
    
    - 복잡한 정밀 계산, 스케일 보정, 오염 감지 규칙을 직접 수행하지 않고 도메인 엔티티에 책임을 위임합니다.
    - 계정과목 매칭 키워드 설정을 집중 관리합니다.
    """

    def __init__(self, keywords_config: Optional[Dict[str, List[str]]] = None):
        if keywords_config is not None:
            self.REVENUE_KEYWORDS = keywords_config.get("revenue", [])
            self.OP_PROFIT_KEYWORDS = keywords_config.get("operating_profit", [])
            self.NET_INCOME_KEYWORDS = keywords_config.get("net_income", [])
        else:
            self.REVENUE_KEYWORDS = ["매출액", "수익(매출액)", "영업수익", "매출"]
            self.OP_PROFIT_KEYWORDS = ["영업이익", "영업이익(손실)"]
            self.NET_INCOME_KEYWORDS = ["당기순이익", "당기순이익(손실)", "분기순이익", "분기순이익(손실)", "반기순이익", "반기순이익(손실)"]

    def extract_metrics(self, statement: FinancialStatement, use_cumulative: bool = False) -> FinancialMetrics:
        """재무제표 도메인 엔티티의 계정 조회 행동을 위임 호출하여 지표를 추출합니다."""
        if not statement:
            return FinancialMetrics()
        return FinancialMetrics(
            revenue=statement.find_account_amount(self.REVENUE_KEYWORDS, use_cumulative),
            operating_profit=statement.find_account_amount(self.OP_PROFIT_KEYWORDS, use_cumulative),
            net_income=statement.find_account_amount(self.NET_INCOME_KEYWORDS, use_cumulative)
        )

    def calculate_quarterly_performance(
        self,
        q1_stmt: Optional[FinancialStatement],
        semi_stmt: Optional[FinancialStatement],
        q3_stmt: Optional[FinancialStatement],
        annual_stmt: Optional[FinancialStatement],
        target_fs_type: Optional[FinancialStatementType] = None
    ) -> QuarterlyMetrics:
        """분기별 공시 데이터를 기반으로 실적 복원 및 차감 연산을 도메인 레이어에 트리거합니다."""
        
        # 대표 기업명 추출
        corp_name = ""
        for stmt in [q1_stmt, semi_stmt, q3_stmt, annual_stmt]:
            if stmt and stmt.corp_name:
                corp_name = stmt.corp_name
                break

        return QuarterlyMetrics.calculate_from_statements(
            corp_name=corp_name,
            q1_stmt=q1_stmt,
            semi_stmt=semi_stmt,
            q3_stmt=q3_stmt,
            annual_stmt=annual_stmt,
            revenue_kws=self.REVENUE_KEYWORDS,
            op_profit_kws=self.OP_PROFIT_KEYWORDS,
            net_income_kws=self.NET_INCOME_KEYWORDS,
            target_fs_type=target_fs_type
        )

    def calculate_annual_from_quarters(self, metrics_by_quarter: Dict[str, FinancialMetrics]) -> FinancialMetrics:
        """수립된 분기 실적 목록을 합산하여 연간 실적을 롤업합니다."""
        temp_metrics = QuarterlyMetrics(corp_name="", metrics_by_quarter=metrics_by_quarter)
        return temp_metrics.calculate_annual_from_quarters()
