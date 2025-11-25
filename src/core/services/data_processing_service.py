"""데이터 처리 및 변환 서비스."""

from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict
from datetime import date

from src.core.domain.models.financial_statement import AccountItem, FinancialStatement
from src.core.domain.models.performance_metrics import FinancialMetrics, QuarterlyMetrics


class DataProcessingService:
    """재무 데이터 처리 및 변환을 담당하는 서비스.
    
    - 문자열 -> Decimal 변환
    - 계정과목 매핑 및 추출
    - 기간 정보를 활용한 분기별 실적 계산
    """

    # 계정과목 매핑 키워드 (우선순위 순)
    REVENUE_KEYWORDS = ["매출액", "수익(매출액)", "영업수익", "매출"]
    OP_PROFIT_KEYWORDS = ["영업이익", "영업이익(손실)"]
    NET_INCOME_KEYWORDS = ["당기순이익", "당기순이익(손실)", "분기순이익", "분기순이익(손실)", "반기순이익", "반기순이익(손실)"]

    def extract_metrics(self, statement: FinancialStatement) -> FinancialMetrics:
        """재무제표에서 주요 지표 추출."""
        return FinancialMetrics(
            revenue=self._find_account_value(statement.accounts, self.REVENUE_KEYWORDS),
            operating_profit=self._find_account_value(statement.accounts, self.OP_PROFIT_KEYWORDS),
            net_income=self._find_account_value(statement.accounts, self.NET_INCOME_KEYWORDS)
        )

    def calculate_quarterly_performance(
        self,
        q1_stmt: Optional[FinancialStatement],
        semi_stmt: Optional[FinancialStatement],
        q3_stmt: Optional[FinancialStatement],
        annual_stmt: Optional[FinancialStatement]
    ) -> QuarterlyMetrics:
        """기간 정보를 활용하여 정확한 분기별 실적 계산."""
        
        # 1. 각 보고서에서 지표 추출
        q1_metrics = self.extract_metrics(q1_stmt) if q1_stmt else FinancialMetrics(None, None, None)
        semi_metrics = self.extract_metrics(semi_stmt) if semi_stmt else FinancialMetrics(None, None, None)
        q3_metrics = self.extract_metrics(q3_stmt) if q3_stmt else FinancialMetrics(None, None, None)
        annual_metrics = self.extract_metrics(annual_stmt) if annual_stmt else FinancialMetrics(None, None, None)

        # 2. 분기별 실적 계산
        # Q1: 1분기 보고서 그대로 사용
        q1_final = q1_metrics

        # Q2: 반기(누적) - Q1
        # 반기 보고서가 누적(1.1~6.30)이라고 가정 (대부분 그렇음)
        # 만약 반기가 별도(4.1~6.30)라면 그대로 사용해야 함 (드문 케이스)
        q2_final = self._calculate_diff(semi_metrics, q1_metrics) if semi_stmt and semi_stmt.is_cumulative else semi_metrics

        # Q3: 3분기 보고서 확인
        # 누적(1.1~9.30)이면 -> 3분기 누적 - 반기 누적
        # 별도(7.1~9.30)이면 -> 그대로 사용
        if q3_stmt and q3_stmt.is_cumulative:
            q3_final = self._calculate_diff(q3_metrics, semi_metrics)
        else:
            q3_final = q3_metrics

        # Q4: 연간(누적) - 3분기 누적
        # 3분기 누적 값을 구해야 함
        cumulative_q3 = self._get_cumulative_q3(q1_metrics, semi_metrics, q3_metrics, q3_stmt)
        
        if annual_stmt and cumulative_q3:
            q4_final = self._calculate_diff(annual_metrics, cumulative_q3)
        else:
            # 계산 불가 시 None
            q4_final = FinancialMetrics(None, None, None)

        return QuarterlyMetrics(
            q1=q1_final,
            q2=q2_final,
            q3=q3_final,
            q4=q4_final
        )

    def _get_cumulative_q3(
        self, 
        q1: FinancialMetrics, 
        semi: FinancialMetrics, 
        q3: FinancialMetrics,
        q3_stmt: Optional[FinancialStatement]
    ) -> Optional[FinancialMetrics]:
        """3분기까지의 누적 실적 계산."""
        # Case 1: 3분기 보고서가 이미 누적 데이터인 경우
        if q3_stmt and q3_stmt.is_cumulative:
            return q3

        # Case 2: 3분기 보고서가 별도 데이터인 경우 -> 반기(누적) + 3분기(별도)
        # 반기 데이터가 있어야 함
        if semi.revenue is not None and q3.revenue is not None:
             return self._add_metrics(semi, q3)
             
        return None

    def _calculate_diff(self, minuend: FinancialMetrics, subtrahend: FinancialMetrics) -> FinancialMetrics:
        """지표 간 뺄셈 (minuend - subtrahend)."""
        return FinancialMetrics(
            revenue=self._safe_sub(minuend.revenue, subtrahend.revenue),
            operating_profit=self._safe_sub(minuend.operating_profit, subtrahend.operating_profit),
            net_income=self._safe_sub(minuend.net_income, subtrahend.net_income)
        )

    def _add_metrics(self, a: FinancialMetrics, b: FinancialMetrics) -> FinancialMetrics:
        """지표 간 덧셈 (a + b)."""
        return FinancialMetrics(
            revenue=self._safe_add(a.revenue, b.revenue),
            operating_profit=self._safe_add(a.operating_profit, b.operating_profit),
            net_income=self._safe_add(a.net_income, b.net_income)
        )

    def _safe_sub(self, a: Optional[Decimal], b: Optional[Decimal]) -> Optional[Decimal]:
        """안전한 뺄셈 (None 처리)."""
        if a is None or b is None:
            return None
        return a - b

    def _safe_add(self, a: Optional[Decimal], b: Optional[Decimal]) -> Optional[Decimal]:
        """안전한 덧셈 (None 처리)."""
        if a is None or b is None:
            return None
        return a + b

    def _find_account_value(self, accounts: List[AccountItem], keywords: List[str]) -> Optional[Decimal]:
        """키워드 리스트와 일치하는 계정과목의 값을 찾아 반환."""
        # 1. 정확한 매칭 시도
        for keyword in keywords:
            for account in accounts:
                acc_name = account.account_nm.replace(" ", "")
                key_norm = keyword.replace(" ", "")
                if acc_name == key_norm:
                    return self._parse_amount(account.thstrm_amount)
        
        # 2. 부분 매칭 시도
        for keyword in keywords:
            for account in accounts:
                if keyword in account.account_nm:
                    return self._parse_amount(account.thstrm_amount)
        return None

    def _parse_amount(self, amount_str: str) -> Optional[Decimal]:
        """문자열 금액을 Decimal로 변환."""
        if not amount_str or amount_str == "-":
            return None
        try:
            clean_str = amount_str.replace(",", "")
            if clean_str.startswith("(") and clean_str.endswith(")"):
                clean_str = "-" + clean_str[1:-1]
            return Decimal(clean_str)
        except (InvalidOperation, ValueError):
            return None
