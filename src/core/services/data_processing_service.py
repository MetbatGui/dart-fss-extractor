"""데이터 처리 및 변환 서비스."""

import re
import logging
from typing import List, Optional, Dict
import sys

# Python 3.11+ 사용 시 tomllib, 이하 버전은 tomli 사용
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        raise ImportError("Python 3.10 이하에서는 'tomli' 패키지가 필요합니다. pip install tomli")

from core.domain.models.financial_statement import AccountItem, FinancialStatement, FinancialStatementType
from core.domain.models.performance_metrics import FinancialMetrics, QuarterlyMetrics

logger = logging.getLogger(__name__)

class DataProcessingService:
    """재무 데이터 처리 및 변환을 담당하는 서비스."""

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
        """재무제표에서 주요 지표 추출."""
        return FinancialMetrics(
            revenue=self._find_account_value(statement.accounts, self.REVENUE_KEYWORDS, use_cumulative),
            operating_profit=self._find_account_value(statement.accounts, self.OP_PROFIT_KEYWORDS, use_cumulative),
            net_income=self._find_account_value(statement.accounts, self.NET_INCOME_KEYWORDS, use_cumulative)
        )

    def calculate_quarterly_performance(
        self,
        q1_stmt: Optional[FinancialStatement],
        semi_stmt: Optional[FinancialStatement],
        q3_stmt: Optional[FinancialStatement],
        annual_stmt: Optional[FinancialStatement],
        target_fs_type: Optional[FinancialStatementType] = None
    ) -> QuarterlyMetrics:
        """분기 실적 계산 (누적/단독 금액 구분 및 강력한 Fallback 대응)."""
        
        # 0. 공시 자릿수(스케일) 불일치 자동 정규화 가드 적용
        self._normalize_statement_scales(q1_stmt, semi_stmt, q3_stmt, annual_stmt)
        
        # 0-1. 도메인 모델에 누적금액(cumulative_amount)이 물리적으로 존재하여 파싱되었는지 판별하는 헬퍼
        def check_has_add(stmt: Optional[FinancialStatement]) -> bool:
            if not stmt or not stmt.accounts:
                return False
            all_keywords = self.REVENUE_KEYWORDS + self.OP_PROFIT_KEYWORDS + self.NET_INCOME_KEYWORDS
            for item in stmt.accounts:
                if item.account_nm.strip() in all_keywords:
                    if item.cumulative_amount and item.cumulative_amount.strip() not in ["", "-"]:
                        return True
            return False

        has_q2_add = check_has_add(semi_stmt)
        has_q3_add = check_has_add(q3_stmt)

        # 1. 분기별 연결재무제표(CFS) 존재 여부 동적 체크
        has_cfs_by_report = {}
        for r_key, stmt in [("1Q", q1_stmt), ("2Q", semi_stmt), ("3Q", q3_stmt), ("Annual", annual_stmt)]:
            if stmt:
                actual_fs_type = stmt.fs_type.value if hasattr(stmt.fs_type, "value") else stmt.fs_type
                # FinancialStatementType.CONSOLIDATED = "CFS"
                has_cfs_by_report[r_key] = (actual_fs_type == "CFS")
            else:
                has_cfs_by_report[r_key] = False

        # 유형 검증 및 데이터 추출 헬퍼 (CFS가 기대되는데 OFS만 존재할 경우 동적 Fallback 적용)
        def get_valid_m(stmt: Optional[FinancialStatement], report_key: str, use_cumulative: bool = False) -> FinancialMetrics:
            if not stmt: return FinancialMetrics(None, None, None)
            
            # Enum 또는 문자열 대응
            actual_fs_type = stmt.fs_type.value if hasattr(stmt.fs_type, "value") else stmt.fs_type
            expected_fs_type = target_fs_type.value if target_fs_type and hasattr(target_fs_type, "value") else target_fs_type

            if target_fs_type and actual_fs_type != expected_fs_type:
                # 기대 유형이 연결(CFS)인데 실제 유형이 별도(OFS)인 경우:
                # 해당 분기 보고서 시점에 연결(CFS)이 전혀 존재하지 않는다면 OFS 데이터를 차선책 Fallback으로 수용
                if expected_fs_type == "CFS" and actual_fs_type == "OFS":
                    if not has_cfs_by_report.get(report_key, False):
                        logger.info(f"[{stmt.corp_name} {report_key}] CFS가 공시되지 않아 OFS 데이터를 Fallback 수용합니다.")
                    else:
                        logger.warning(f"유형 불일치 무시: {actual_fs_type} != {expected_fs_type}")
                        return FinancialMetrics(None, None, None)
                else:
                    logger.warning(f"유형 불일치 무시: {actual_fs_type} != {expected_fs_type}")
                    return FinancialMetrics(None, None, None)
            return self.extract_metrics(stmt, use_cumulative)

        # 각 분기별로 단독(single) 및 누적(cumulative) 수치를 각각 추출
        q1_single = get_valid_m(q1_stmt, "1Q", use_cumulative=False)
        q1_cum = get_valid_m(q1_stmt, "1Q", use_cumulative=True)
        # 1분기는 단독이 곧 누적이므로 상호 보완
        if q1_cum.revenue is None and q1_single.revenue is not None:
            q1_cum = q1_single
        elif q1_single.revenue is None and q1_cum.revenue is not None:
            q1_single = q1_cum

        semi_single = get_valid_m(semi_stmt, "2Q", use_cumulative=False)
        semi_cum = get_valid_m(semi_stmt, "2Q", use_cumulative=True)

        q3_single = get_valid_m(q3_stmt, "3Q", use_cumulative=False)
        q3_cum = get_valid_m(q3_stmt, "3Q", use_cumulative=True)

        ann_cum = get_valid_m(annual_stmt, "Annual", use_cumulative=True)

        # 2. 독립적 분기 실적 매핑 및 순차 누적 (현분기 누적 다이렉트 차감 및 상호 결측 복원 알고리즘 적용)
        corp_name = self._extract_corp_name([q1_stmt, semi_stmt, q3_stmt, annual_stmt])
        
        def is_metrics_valid(m: FinancialMetrics) -> bool:
            return m.revenue is not None or m.operating_profit is not None or m.net_income is not None

        def resolve_cumulative(stmt_cum: FinancialMetrics, stmt_single: FinancialMetrics, fallback_cum: FinancialMetrics, has_add: bool) -> FinancialMetrics:
            if not has_add:
                # 물리적인 누적 데이터가 도메인 레벨에 없었다면, fallback_cum(순차 누적 합산)을 무조건적으로 우선 신뢰하여 사용
                return fallback_cum
            
            if is_metrics_valid(stmt_cum):
                # 가짜 누적(Fake Cumulative) 감지 가드:
                # 단독 실적과 누적 실적이 1원도 안 틀리고 완벽히 일치한다면, 누적 필드 결측으로 간주하고 fallback_cum(순차 누적 합산)을 차용합니다.
                if (stmt_cum.revenue == stmt_single.revenue and 
                    stmt_cum.operating_profit == stmt_single.operating_profit and 
                    stmt_cum.net_income == stmt_single.net_income):
                    return fallback_cum
                return stmt_cum
            return fallback_cum

        # 각 시점별 누적치(현분기 누적) 결정
        q1_final_cum = q1_cum
        
        # 2분기 가짜 단독 공시 오염 검사 (이전 누계 대조식 가드):
        # 2Q 누적액이 도메인에 명시적으로 존재(has_q2_add)하고, 1Q 누적액이 존재하고 0보다 크며, 
        # 2Q 단독과 2Q 누적이 완벽히 같으면서, 2Q 단독 수치가 1Q 누적보다 크다면 ➔ 2Q 단독은 사실 2Q 누계임이 확실합니다.
        is_fake_2q = False
        if (has_q2_add and
            q1_final_cum.revenue is not None and q1_final_cum.revenue > 0 and 
            semi_single.revenue is not None and semi_single.revenue == semi_cum.revenue and 
            semi_single.revenue > q1_final_cum.revenue):
            is_fake_2q = True

        if is_fake_2q:
            logger.info(f"[{corp_name} 2Q] 이전 누계 대조식 가드로 가짜 단독(누적 오염)을 감지했습니다. 차감 복원합니다.")
            semi_cum = semi_single
            semi_single = FinancialMetrics(None, None, None)

        q2_final_cum = resolve_cumulative(semi_cum, semi_single, self._add_metrics(q1_final_cum, semi_single), has_q2_add)

        # 3분기 가짜 단독 공시 오염 검사 (이전 누계 대조식 가드):
        # 3Q 누적액이 도메인에 명시적으로 존재(has_q3_add)하고, 2Q 누적액이 존재하고 0보다 크며, 
        # 3Q 단독과 3Q 누적이 완벽히 같으면서, 3Q 단독 수치가 2Q 누적보다 크다면 ➔ 3Q 단독은 사실 3Q 누계임이 확실합니다.
        is_fake_3q = False
        if (has_q3_add and
            q2_final_cum.revenue is not None and q2_final_cum.revenue > 0 and 
            q3_single.revenue is not None and q3_single.revenue == q3_cum.revenue and 
            q3_single.revenue > q2_final_cum.revenue):
            is_fake_3q = True

        if is_fake_3q:
            logger.info(f"[{corp_name} 3Q] 이전 누계 대조식 가드로 가짜 단독(누적 오염)을 감지했습니다. 차감 복원합니다.")
            q3_cum = q3_single
            q3_single = FinancialMetrics(None, None, None)

        q3_final_cum = resolve_cumulative(q3_cum, q3_single, self._add_metrics(q2_final_cum, q3_single), has_q3_add)

        # 각 시점별 단독(분기) 실적 복원 및 차감 역산
        q1_final_single = q1_single
        
        q2_final_single = FinancialMetrics(None, None, None)
        if semi_stmt is not None:
            q2_final_single = semi_single if is_metrics_valid(semi_single) else self._calculate_diff(q2_final_cum, q1_final_cum)
            
        q3_final_single = FinancialMetrics(None, None, None)
        if q3_stmt is not None:
            q3_final_single = q3_single if is_metrics_valid(q3_single) else self._calculate_diff(q3_final_cum, q2_final_cum)
        
        # 4분기: Annual 누적 - 3분기 누적
        q4_final_single = FinancialMetrics(None, None, None)
        if annual_stmt is not None and ann_cum.revenue is not None and q3_final_cum.revenue is not None:
            q4_final_single = self._calculate_diff(ann_cum, q3_final_cum)

        # 3. 매출액 음수 방어 및 클렌징 로직 적용
        def sanitize_metrics(m: FinancialMetrics, label: str) -> FinancialMetrics:
            if m.revenue is not None and m.revenue < 0:
                logger.warning(f"[{corp_name} {label}] 매출액 음수 감지 ({m.revenue}). None으로 치환합니다.")
                return FinancialMetrics(revenue=None, operating_profit=m.operating_profit, net_income=m.net_income)
            return m

        metrics = {
            "1Q": sanitize_metrics(q1_final_single, "1Q"),
            "2Q": sanitize_metrics(q2_final_single, "2Q"),
            "3Q": sanitize_metrics(q3_final_single, "3Q"),
            "4Q": sanitize_metrics(q4_final_single, "4Q")
        }

        return QuarterlyMetrics(corp_name=corp_name, metrics_by_quarter=metrics, annual_metrics=sanitize_metrics(ann_cum, "Annual"))

    def _calculate_diff(self, minuend: FinancialMetrics, subtrahend: FinancialMetrics) -> FinancialMetrics:
        def safe_sub(a, b): return a - b if a is not None and b is not None else None
        return FinancialMetrics(revenue=safe_sub(minuend.revenue, subtrahend.revenue), 
                                operating_profit=safe_sub(minuend.operating_profit, subtrahend.operating_profit),
                                net_income=safe_sub(minuend.net_income, subtrahend.net_income))

    def _add_metrics(self, a: FinancialMetrics, b: FinancialMetrics) -> FinancialMetrics:
        def safe_add(v1, v2):
            if v1 is None: return v2
            if v2 is None: return v1
            return v1 + v2
        return FinancialMetrics(revenue=safe_add(a.revenue, b.revenue), 
                                operating_profit=safe_add(a.operating_profit, b.operating_profit),
                                net_income=safe_add(a.net_income, b.net_income))

    def _extract_corp_name(self, statements: List[Optional[FinancialStatement]]) -> str:
        for stmt in statements:
            if stmt and stmt.corp_name: return stmt.corp_name
        return ""

    def _find_account_value(self, accounts: List[AccountItem], keywords: List[str], use_cumulative: bool = False) -> Optional[int]:
        # 자본-손익 오매칭 가드를 위한 당기순이익 규모 레퍼런스 탐색
        reference_net_income = None
        for item in accounts:
            nm = item.account_nm.strip()
            # 자본과 오인될 수 없는 명확한 일반 당기순이익 키워드 값 탐색
            if nm in ["당기순이익", "당기순이익(손실)", "분기순이익", "분기순이익(손실)", "반기순이익", "반기순이익(손실)"]:
                if use_cumulative:
                    val_str = item.cumulative_amount if item.cumulative_amount else item.amount
                else:
                    val_str = item.amount
                if val_str and val_str.strip() not in ["", "-"]:
                    try:
                        reference_net_income = abs(int(re.sub(r'[^0-9-]', '', val_str)))
                        break  # 레퍼런스 값 하나를 확보하면 즉시 종료
                    except (ValueError, TypeError):
                        continue

        # [지능형 분할 매출 합산 가드]
        # keywords가 매출액 계정군이고, 통합 매출 계정이 없는 특수 공시 양식인 경우 세부 분할 계정(수출/내수)을 자동 합산
        is_revenue_search = any(kw in ["매출액", "영업수익", "매출"] for kw in keywords)
        if is_revenue_search:
            has_integrated_revenue = False
            for item in accounts:
                statement_type = item.statement_type
                if statement_type and statement_type.strip().upper() == "BS":
                    continue
                if item.account_nm.strip() in ["매출액", "영업수익", "매출"]:
                    has_integrated_revenue = True
                    break
            
            if not has_integrated_revenue:
                export_val = None
                domestic_val = None
                for item in accounts:
                    statement_type = item.statement_type
                    if statement_type and statement_type.strip().upper() == "BS":
                        continue
                    nm = item.account_nm.strip()
                    val_str = item.cumulative_amount if use_cumulative and item.cumulative_amount else item.amount
                    if not val_str or val_str.strip() in ["", "-"]:
                        continue
                    try:
                        val = int(re.sub(r'[^0-9-]', '', val_str))
                        if "수출" in nm:
                            export_val = val
                        elif "내수" in nm:
                            domestic_val = val
                    except (ValueError, TypeError):
                        continue
                
                if export_val is not None and domestic_val is not None:
                    logger.info(f"[CORE ENGINE GUARDIAN] 분할 매출 계정 감지 및 합산 처리 완료: 수출({export_val}) + 내수({domestic_val}) = {export_val + domestic_val}")
                    return export_val + domestic_val

        # 1. 완전 일치 우선순위 탐색 (키워드 순서 준수)
        for kw in keywords:
            for item in accounts:
                # 안전장치 1: statement_type이 명시적으로 BS(재무상태표)인 자본 항목은 손익 항목 매칭에서 전면 배제
                statement_type = item.statement_type
                if statement_type and statement_type.strip().upper() == "BS":
                    continue
                
                name = item.account_nm.strip()
                if name == kw:
                    if use_cumulative:
                        val_str = item.cumulative_amount if item.cumulative_amount else item.amount
                    else:
                        val_str = item.amount
                        
                    if not val_str or val_str.strip() in ["", "-"]:
                        continue
                    try:
                        clean_val = int(re.sub(r'[^0-9-]', '', val_str))
                        
                        # 안전장치 2: 키워드가 지배주주지분 관련 항목인데, 값이 당기순이익 규모(레퍼런스)의 1.1배를 초과하면 (자본 또는 포괄이익) 제외
                        if kw in ["지배기업의 소유주지분", "지배기업 소유주지분", "지배기업의소유주지분"]:
                            if reference_net_income is not None and abs(clean_val) > reference_net_income * 1.1:
                                continue
                                
                        # 최초 매칭(First Match) 전략: 손익계산서 상에서 당기순이익 항목은 포괄이익 항목보다 항상 먼저 기재되므로 즉시 반환
                        return clean_val
                    except (ValueError, TypeError):
                        continue

        # 2. 부분 일치 우선순위 탐색 (키워드 순서 준수)
        for kw in keywords:
            for item in accounts:
                # 안전장치 1: statement_type이 명시적으로 BS인 경우 배제
                statement_type = item.statement_type
                if statement_type and statement_type.strip().upper() == "BS":
                    continue
                    
                name = item.account_nm.strip()
                if kw in name:
                    if use_cumulative:
                        val_str = item.cumulative_amount if item.cumulative_amount else item.amount
                    else:
                        val_str = item.amount
                        
                    if not val_str or val_str.strip() in ["", "-"]:
                        continue
                    try:
                        clean_val = int(re.sub(r'[^0-9-]', '', val_str))
                        
                        # 안전장치 2: 과도한 스케일 자본/포괄이익 항목 필터링
                        if kw in ["지배기업의 소유주지분", "지배기업 소유주지분", "지배기업의소유주지분"]:
                            if reference_net_income is not None and abs(clean_val) > reference_net_income * 1.1:
                                continue
                                
                        return clean_val
                    except (ValueError, TypeError):
                        continue
                
        return None

    def _normalize_statement_scales(
        self,
        q1_stmt: Optional[FinancialStatement],
        semi_stmt: Optional[FinancialStatement],
        q3_stmt: Optional[FinancialStatement],
        annual_stmt: Optional[FinancialStatement]
    ) -> None:
        """각 분기 재무제표 간의 자릿수(스케일) 불일치를 자동으로 감지하고 지배적인 스케일에 맞게 보정합니다."""
        statements = [stmt for stmt in [q1_stmt, semi_stmt, q3_stmt, annual_stmt] if stmt]
        if len(statements) < 2:
            return

        import math

        # 각 보고서의 대표값(0이 아닌 수치들의 중간값의 절대값)을 구함
        scales = []
        stmt_to_scale = {}
        
        for stmt in statements:
            vals = []
            for item in stmt.accounts:
                for attr in ["amount", "cumulative_amount"]:
                    val_str = getattr(item, attr, None)
                    if val_str and val_str.strip() not in ["", "-"]:
                        try:
                            val = abs(int(re.sub(r'[^0-9-]', '', val_str)))
                            if val > 0:
                                vals.append(val)
                        except (ValueError, TypeError):
                            continue
            if vals:
                vals.sort()
                median_val = vals[len(vals) // 2]
                stmt_to_scale[id(stmt)] = median_val
                scales.append(median_val)

        if len(scales) < 2:
            return

        # 각 스케일의 log10 값을 구함
        log_scales = [math.log10(s) for s in scales]
        # log10 스케일들의 중앙값을 구하여 기준 스케일로 설정
        log_scales.sort()
        base_log = log_scales[len(log_scales) // 2]

        # 각 보고서에 대해 기준 스케일과의 차이를 분석하여 보정 배율(10의 거듭제곱) 계산
        for stmt in statements:
            stmt_scale = stmt_to_scale.get(id(stmt))
            if not stmt_scale:
                continue
            
            diff = math.log10(stmt_scale) - base_log
            exponent = round(diff)
            
            # 1000배(자릿수 차이 3) 이상 차이가 나면 보정 적용
            if abs(exponent) >= 3:
                factor = 10 ** abs(exponent)
                multiply = exponent < 0
                
                logger.warning(
                    f"[{stmt.corp_name or '알수없음'} ({stmt.bsns_year} {stmt.reprt_type})] 스케일 불일치 감지. "
                    f"대표값: {stmt_scale:.1e}, 기준값: {10**base_log:.1e}. "
                    f"자릿수 차이: {exponent}. {'곱하기' if multiply else '나누기'} {factor} 보정을 적용합니다."
                )
                
                for item in stmt.accounts:
                    for attr in ["amount", "cumulative_amount"]:
                        val_str = getattr(item, attr, None)
                        if val_str and val_str.strip() not in ["", "-"]:
                            try:
                                clean_str = re.sub(r'[^0-9-]', '', val_str)
                                # 원래 음수 기호 보존
                                is_negative = val_str.strip().startswith("-")
                                val = abs(int(clean_str))
                                
                                if multiply:
                                    new_val = val * factor
                                else:
                                    new_val = int(round(val / factor))
                                    
                                if is_negative:
                                    new_val = -new_val
                                    
                                setattr(item, attr, str(new_val))
                            except (ValueError, TypeError):
                                continue

    def calculate_annual_from_quarters(self, metrics_by_quarter: Dict[str, FinancialMetrics]) -> FinancialMetrics:
        """분기 실적 목록을 합산하여 연간 실적 산출."""
        total_revenue = None
        total_op = None
        total_net = None
        
        for q in ["1Q", "2Q", "3Q", "4Q"]:
            m = metrics_by_quarter.get(q)
            if m:
                if m.revenue is not None:
                    total_revenue = (total_revenue or 0) + m.revenue
                if m.operating_profit is not None:
                    total_op = (total_op or 0) + m.operating_profit
                if m.net_income is not None:
                    total_net = (total_net or 0) + m.net_income
        
        return FinancialMetrics(revenue=total_revenue, operating_profit=total_op, net_income=total_net)
