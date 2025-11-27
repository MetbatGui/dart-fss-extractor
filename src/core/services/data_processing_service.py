"""데이터 처리 및 변환 서비스."""

import re
import sys
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict
from datetime import date
from pathlib import Path

# Python 3.11+ 사용 시 tomllib, 이하 버전은 tomli 사용
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        raise ImportError("Python 3.10 이하에서는 'tomli' 패키지가 필요합니다. pip install tomli")

from core.domain.models.financial_statement import AccountItem, FinancialStatement
from core.domain.models.performance_metrics import FinancialMetrics, QuarterlyMetrics


class DataProcessingService:
    """재무 데이터 처리 및 변환을 담당하는 서비스.
    
    - 문자열 -> Decimal 변환
    - 계정과목 매핑 및 추출
    - 기간 정보를 활용한 분기별 실적 계산
    """

    def __init__(self, config_path: Optional[str] = None):
        """초기화.
        
        Args:
            config_path: 계정과목 키워드 설정 파일 경로 (TOML 형식).
                        None이면 기본 경로 사용: config/account_keywords.toml
        """
        if config_path is None:
            # 프로젝트 루트에서 config 디렉토리 찾기
            current_file = Path(__file__)
            project_root = current_file.parent.parent.parent.parent
            config_path = project_root / "config" / "account_keywords.toml"
        else:
            config_path = Path(config_path)
        
        # TOML 설정 파일 로드
        if config_path.exists():
            with open(config_path, "rb") as f:
                config = tomllib.load(f)
            
            keywords = config.get("account_keywords", {})
            self.REVENUE_KEYWORDS = keywords.get("revenue", [])
            self.OP_PROFIT_KEYWORDS = keywords.get("operating_profit", [])
            self.NET_INCOME_KEYWORDS = keywords.get("net_income", [])
        else:
            # 설정 파일이 없으면 기본값 사용 (하위 호환성)
            self.REVENUE_KEYWORDS = ["매출액", "수익(매출액)", "영업수익", "매출"]
            self.OP_PROFIT_KEYWORDS = ["영업이익", "영업이익(손실)"]
            self.NET_INCOME_KEYWORDS = ["당기순이익", "당기순이익(손실)", "분기순이익", "분기순이익(손실)", "반기순이익", "반기순이익(손실)"]

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
        """기간 정보를 활용하여 정확한 분기별 실적 계산.
        
        각 분기별 계산 로직을 헬퍼 메서드로 위임하여 복잡도를 낮춤.
        """
        q1_final = self._calculate_q1(q1_stmt)
        q2_final = self._calculate_q2(q1_stmt, semi_stmt)
        q3_final = self._calculate_q3(semi_stmt, q3_stmt)
        q4_final = self._calculate_q4(q1_stmt, semi_stmt, q3_stmt, annual_stmt)
        
        corp_name = self._extract_corp_name([q1_stmt, semi_stmt, q3_stmt, annual_stmt])
        
        return QuarterlyMetrics(
            corp_name=corp_name,
            metrics_by_quarter={
                "1Q": q1_final,
                "2Q": q2_final,
                "3Q": q3_final,
                "4Q": q4_final
            }
        )

    def _calculate_q1(self, q1_stmt: Optional[FinancialStatement]) -> FinancialMetrics:
        """1분기 실적 계산 (그대로 사용).
        
        Args:
            q1_stmt: 1분기 보고서
        
        Returns:
            1분기 재무 지표
        """
        return self.extract_metrics(q1_stmt) if q1_stmt else FinancialMetrics(None, None, None)

    def _calculate_q2(
        self,
        q1_stmt: Optional[FinancialStatement],
        semi_stmt: Optional[FinancialStatement]
    ) -> FinancialMetrics:
        """2분기 실적 계산 (반기 누적 - 1분기).
        
        Args:
            q1_stmt: 1분기 보고서
            semi_stmt: 반기 보고서
        
        Returns:
            2분기 재무 지표
        """
        if not semi_stmt:
            return FinancialMetrics(None, None, None)
        
        semi_metrics = self.extract_metrics(semi_stmt)
        
        # 반기가 누적(1.1~6.30)이면 1분기를 빼야 함
        if semi_stmt.is_cumulative and q1_stmt:
            q1_metrics = self.extract_metrics(q1_stmt)
            return self._calculate_diff(semi_metrics, q1_metrics)
        
        # 반기가 별도(4.1~6.30)면 그대로 사용
        return semi_metrics

    def _calculate_q3(
        self,
        semi_stmt: Optional[FinancialStatement],
        q3_stmt: Optional[FinancialStatement]
    ) -> FinancialMetrics:
        """3분기 실적 계산.
        
        Args:
            semi_stmt: 반기 보고서
            q3_stmt: 3분기 보고서
        
        Returns:
            3분기 재무 지표
        """
        if not q3_stmt:
            return FinancialMetrics(None, None, None)
        
        q3_metrics = self.extract_metrics(q3_stmt)
        
        # 3분기가 누적(1.1~9.30)이면 반기를 빼야 함
        if q3_stmt.is_cumulative and semi_stmt:
            semi_metrics = self.extract_metrics(semi_stmt)
            return self._calculate_diff(q3_metrics, semi_metrics)
        
        # 3분기가 별도(7.1~9.30)면 그대로 사용
        return q3_metrics

    def _calculate_q4(
        self,
        q1_stmt: Optional[FinancialStatement],
        semi_stmt: Optional[FinancialStatement],
        q3_stmt: Optional[FinancialStatement],
        annual_stmt: Optional[FinancialStatement]
    ) -> FinancialMetrics:
        """4분기 실적 계산 (연간 누적 - 3분기 누적).
        
        Args:
            q1_stmt: 1분기 보고서
            semi_stmt: 반기 보고서
            q3_stmt: 3분기 보고서
            annual_stmt: 연간 보고서
        
        Returns:
            4분기 재무 지표
        """
        if not annual_stmt:
            return FinancialMetrics(None, None, None)
        
        annual_metrics = self.extract_metrics(annual_stmt)
        
        # 3분기까지의 누적 값 계산
        q1_metrics = self.extract_metrics(q1_stmt) if q1_stmt else FinancialMetrics(None, None, None)
        semi_metrics = self.extract_metrics(semi_stmt) if semi_stmt else FinancialMetrics(None, None, None)
        q3_metrics = self.extract_metrics(q3_stmt) if q3_stmt else FinancialMetrics(None, None, None)
        
        cumulative_q3 = self._get_cumulative_q3(q1_metrics, semi_metrics, q3_metrics, q3_stmt)
        
        if cumulative_q3:
            return self._calculate_diff(annual_metrics, cumulative_q3)
        
        return FinancialMetrics(None, None, None)

    def _extract_corp_name(self, statements: List[Optional[FinancialStatement]]) -> str:
        """보고서 목록에서 기업명 추출.
        
        Args:
            statements: 재무제표 목록
        
        Returns:
            기업명 (없으면 빈 문자열)
        """
        for stmt in statements:
            if stmt and stmt.corp_name:
                return stmt.corp_name
        return ""

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

    def _normalize_account_name(self, name: str) -> str:
        """계정과목명에서 괄호와 괄호 안 내용, 공백, 특수문자, 숫자, 영문 등을 제거하고 순수 한글만 반환.
        
        1️⃣ 괄호(`(`, `)`)와 그 안에 있는 모든 문자 제거 (예: "당기순이익(손실)" → "당기순이익")
        2️⃣ 남은 문자열에서 한글이 아닌 모든 문자 제거
        
        Args:
            name: 원본 계정과목명
        
        Returns:
            한글만 남은 문자열
        """
        # 괄호와 괄호 안 내용 제거
        without_parentheses = re.sub(r'\([^)]*\)', '', name)
        # 한글 외 문자 모두 제거 (공백, 숫자, 특수문자, 영문 등)
        return re.sub(r'[^가-힣]', '', without_parentheses)


    def _find_account_value(self, accounts: List[AccountItem], keywords: List[str]) -> Optional[Decimal]:
        """키워드 리스트와 일치하는 계정과목의 값을 찾아 반환.
        
        정확한 매칭을 먼저 시도하고, 없으면 부분 매칭을 시도합니다.
        """
        # 1. 정확한 매칭 시도
        result = self._find_exact_match(accounts, keywords)
        if result is not None:
            return result
        
        # 2. 부분 매칭 시도
        return self._find_partial_match(accounts, keywords)

    def _find_exact_match(self, accounts: List[AccountItem], keywords: List[str]) -> Optional[Decimal]:
        """정확한 계정과목명 매칭.
        
        Args:
            accounts: 계정과목 리스트
            keywords: 검색 키워드 리스트
        
        Returns:
            매칭된 계정의 금액, 없으면 None
        """
        for keyword in keywords:
            key_normalized = self._normalize_account_name(keyword)
            for account in accounts:
                acc_normalized = self._normalize_account_name(account.account_nm)
                if acc_normalized == key_normalized:
                    return self._parse_amount(account.thstrm_amount)
        return None

    def _find_partial_match(self, accounts: List[AccountItem], keywords: List[str]) -> Optional[Decimal]:
        """부분 계정과목명 매칭.
        
        Args:
            accounts: 계정과목 리스트
            keywords: 검색 키워드 리스트
        
        Returns:
            매칭된 계정의 금액, 없으면 None
        """
        for keyword in keywords:
            key_normalized = self._normalize_account_name(keyword)
            for account in accounts:
                acc_normalized = self._normalize_account_name(account.account_nm)
                if key_normalized in acc_normalized:
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
