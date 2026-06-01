"""성적표 및 재무 지표 모델 - 풍부한 도메인 행동 및 완벽한 하위 호환성(LSP) 보장 구현."""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Optional, List

from core.domain.models.amount import Amount
from core.domain.models.financial_statement import FinancialStatement, FinancialStatementType

logger = logging.getLogger(__name__)


@dataclass
class FinancialMetrics:
    """재무 지표 (매출액, 영업이익, 당기순이익) 도메인 모델.
    
    - 하위 호환성을 극대화하기 위해 외부 인터페이스 타입은 기존과 동일하게 Optional[Decimal]을 유지합니다.
    - 내부 계산 및 가감 시에만 Amount VO를 동적으로 활용하여 널 안정성을 보장합니다.
    """
    revenue: Optional[Decimal] = None
    operating_profit: Optional[Decimal] = None
    net_income: Optional[Decimal] = None

    def __post_init__(self):
        # 만약 생성 시점에 실수로 Amount 객체가 직접 들어왔을 경우를 대비한 가드
        if isinstance(self.revenue, Amount):
            self.revenue = self.revenue.value
        if isinstance(self.operating_profit, Amount):
            self.operating_profit = self.operating_profit.value
        if isinstance(self.net_income, Amount):
            self.net_income = self.net_income.value

        # float이 들어왔을 경우 Decimal로 정규화
        if isinstance(self.revenue, float):
            self.revenue = Decimal(str(self.revenue))
        if isinstance(self.operating_profit, float):
            self.operating_profit = Decimal(str(self.operating_profit))
        if isinstance(self.net_income, float):
            self.net_income = Decimal(str(self.net_income))

    @property
    def is_valid(self) -> bool:
        """유효한 데이터가 존재 유무 확인."""
        return (self.revenue is not None or 
                self.operating_profit is not None or 
                self.net_income is not None)

    def subtract(self, other: 'FinancialMetrics') -> 'FinancialMetrics':
        """피감수에서 감수를 차감하여 분기별 순 수치를 연산합니다."""
        return FinancialMetrics(
            revenue=(Amount(self.revenue) - Amount(other.revenue)).value,
            operating_profit=(Amount(self.operating_profit) - Amount(other.operating_profit)).value,
            net_income=(Amount(self.net_income) - Amount(other.net_income)).value
        )

    def add(self, other: 'FinancialMetrics') -> 'FinancialMetrics':
        """실적을 누적 합산합니다."""
        return FinancialMetrics(
            revenue=(Amount(self.revenue) + Amount(other.revenue)).value,
            operating_profit=(Amount(self.operating_profit) + Amount(other.operating_profit)).value,
            net_income=(Amount(self.net_income) + Amount(other.net_income)).value
        )

    def sanitize(self, label: str, corp_name: str) -> 'FinancialMetrics':
        """매출액 음수 감지 시 None으로 치환하는 자가 클렌징 가드."""
        if self.revenue is not None and self.revenue < 0:
            logger.warning(f"[{corp_name} {label}] 매출액 음수 감지 ({self.revenue}). None으로 치환합니다.")
            return FinancialMetrics(
                revenue=None,
                operating_profit=self.operating_profit,
                net_income=self.net_income
            )
        return self


@dataclass
class QuarterlyMetrics:
    """기업의 분기별 재무 지표 엔티티."""
    corp_name: str
    metrics_by_quarter: Dict[str, FinancialMetrics] = field(default_factory=dict)
    annual_metrics: Optional[FinancialMetrics] = None

    @classmethod
    def calculate_from_statements(
        cls,
        corp_name: str,
        q1_stmt: Optional[FinancialStatement],
        semi_stmt: Optional[FinancialStatement],
        q3_stmt: Optional[FinancialStatement],
        annual_stmt: Optional[FinancialStatement],
        revenue_kws: List[str],
        op_profit_kws: List[str],
        net_income_kws: List[str],
        target_fs_type: Optional[FinancialStatementType] = None
    ) -> 'QuarterlyMetrics':
        """각 분기 보고서 실적을 분석 및 복원하여 최종 분기 실적 및 연간 지표를 도출합니다."""
        
        # 0. 각 보고서별 자릿수(Scale) 불일치 자동 감지 및 보정 적용
        statements = [s for s in [q1_stmt, semi_stmt, q3_stmt, annual_stmt] if s]
        FinancialStatement.normalize_scales(statements)

        # 도메인 모델에 누적금액(cumulative_amount)이 존재하여 파싱되었는지 판별하는 헬퍼
        def check_has_cumulative(stmt: Optional[FinancialStatement]) -> bool:
            if not stmt:
                return False
            all_keywords = revenue_kws + op_profit_kws + net_income_kws
            for item in stmt.accounts:
                if item.account_nm.strip() in all_keywords:
                    if not item.cumulative_amount.is_none:
                        return True
            return False

        has_q2_cum_flag = check_has_cumulative(semi_stmt)
        has_q3_cum_flag = check_has_cumulative(q3_stmt)

        # CFS/OFS 여부 동적 체크
        has_cfs_by_report = {
            "1Q": q1_stmt.fs_type == FinancialStatementType.CONSOLIDATED if q1_stmt else False,
            "2Q": semi_stmt.fs_type == FinancialStatementType.CONSOLIDATED if semi_stmt else False,
            "3Q": q3_stmt.fs_type == FinancialStatementType.CONSOLIDATED if q3_stmt else False,
            "Annual": annual_stmt.fs_type == FinancialStatementType.CONSOLIDATED if annual_stmt else False,
        }

        # 유형 검증 및 데이터 추출
        def extract(stmt: Optional[FinancialStatement], report_key: str, use_cumulative: bool = False) -> FinancialMetrics:
            if not stmt:
                return FinancialMetrics()
            
            # Fallback 지원
            if target_fs_type and stmt.fs_type != target_fs_type:
                if target_fs_type == FinancialStatementType.CONSOLIDATED and stmt.fs_type == FinancialStatementType.SEPARATE:
                    if not has_cfs_by_report.get(report_key, False):
                        logger.info(f"[{stmt.corp_name} {report_key}] CFS 공시가 없어 OFS를 Fallback 수용합니다.")
                    else:
                        return FinancialMetrics()
                else:
                    return FinancialMetrics()
            
            return FinancialMetrics(
                revenue=stmt.find_account_amount(revenue_kws, use_cumulative).value,
                operating_profit=stmt.find_account_amount(op_profit_kws, use_cumulative).value,
                net_income=stmt.find_account_amount(net_income_kws, use_cumulative).value
            )

        # 각 시점별 단독 및 누적 데이터 추출
        q1_single = extract(q1_stmt, "1Q", use_cumulative=False)
        q1_cum = extract(q1_stmt, "1Q", use_cumulative=True)
        if not q1_cum.is_valid and q1_single.is_valid:
            q1_cum = q1_single
        elif not q1_single.is_valid and q1_cum.is_valid:
            q1_single = q1_cum

        semi_single = extract(semi_stmt, "2Q", use_cumulative=False)
        semi_cum = extract(semi_stmt, "2Q", use_cumulative=True)

        q3_single = extract(q3_stmt, "3Q", use_cumulative=False)
        q3_cum = extract(q3_stmt, "3Q", use_cumulative=True)

        ann_cum = extract(annual_stmt, "Annual", use_cumulative=True)

        # 누적치 결정 로직 캡슐화
        def resolve_cumulative(stmt_cum: FinancialMetrics, stmt_single: FinancialMetrics, fallback_cum: FinancialMetrics, has_cum: bool) -> FinancialMetrics:
            if not has_cum:
                return fallback_cum
            if stmt_cum.is_valid:
                # 가짜 누적 검사
                if (stmt_cum.revenue == stmt_single.revenue and 
                    stmt_cum.operating_profit == stmt_single.operating_profit and 
                    stmt_cum.net_income == stmt_single.net_income):
                    return fallback_cum
                return stmt_cum
            return fallback_cum

        q1_final_cum = q1_cum

        # 2분기 가짜 단독 공시 오염 가드 (이전 누계 대조)
        is_fake_2q = (
            has_q2_cum_flag and
            q1_final_cum.revenue is not None and q1_final_cum.revenue > 0 and
            semi_single.revenue is not None and semi_single.revenue == semi_cum.revenue and
            semi_single.revenue > q1_final_cum.revenue
        )
        if is_fake_2q:
            logger.info(f"[{corp_name} 2Q] 가짜 단독 공시 오염 감지 -> 누적으로 치환하여 차감 복원합니다.")
            semi_cum = semi_single
            semi_single = FinancialMetrics()

        q2_final_cum = resolve_cumulative(semi_cum, semi_single, q1_final_cum.add(semi_single), has_q2_cum_flag)

        # 3분기 가짜 단독 공시 오염 가드 (이전 누계 대조)
        is_fake_3q = (
            has_q3_cum_flag and
            q2_final_cum.revenue is not None and q2_final_cum.revenue > 0 and
            q3_single.revenue is not None and q3_single.revenue == q3_cum.revenue and
            q3_single.revenue > q2_final_cum.revenue
        )
        if is_fake_3q:
            logger.info(f"[{corp_name} 3Q] 가짜 단독 공시 오염 감지 -> 누적으로 치환하여 차감 복원합니다.")
            q3_cum = q3_single
            q3_single = FinancialMetrics()

        q3_final_cum = resolve_cumulative(q3_cum, q3_single, q2_final_cum.add(q3_single), has_q3_cum_flag)

        # 각 시점별 실제 분기 실적 복원 및 차감 역산
        q1_final_single = q1_single
        
        q2_final_single = FinancialMetrics()
        if semi_stmt is not None:
            q2_final_single = semi_single if semi_single.is_valid else q2_final_cum.subtract(q1_final_cum)
            
        q3_final_single = FinancialMetrics()
        if q3_stmt is not None:
            q3_final_single = q3_single if q3_single.is_valid else q3_final_cum.subtract(q2_final_cum)
        
        q4_final_single = FinancialMetrics()
        if annual_stmt is not None and ann_cum.revenue is not None and q3_final_cum.revenue is not None:
            q4_final_single = ann_cum.subtract(q3_final_cum)

        metrics = {
            "1Q": q1_final_single.sanitize("1Q", corp_name),
            "2Q": q2_final_single.sanitize("2Q", corp_name),
            "3Q": q3_final_single.sanitize("3Q", corp_name),
            "4Q": q4_final_single.sanitize("4Q", corp_name)
        }

        return cls(
            corp_name=corp_name,
            metrics_by_quarter=metrics,
            annual_metrics=ann_cum.sanitize("Annual", corp_name)
        )

    def calculate_annual_from_quarters(self) -> FinancialMetrics:
        """수립된 분기 실적을 기반으로 연간 총 실적을 합산합니다."""
        total = FinancialMetrics()
        for q in ["1Q", "2Q", "3Q", "4Q"]:
            m = self.metrics_by_quarter.get(q)
            if m:
                total = total.add(m)
        return total


