"""데이터 처리 및 변환 서비스."""

import re
import logging
from decimal import Decimal
from typing import List, Optional, Dict
from pathlib import Path
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

    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            current_file = Path(__file__)
            project_root = current_file.parent.parent.parent.parent
            config_path = project_root / "config" / "account_keywords.toml"
        else:
            config_path = Path(config_path)
        
        if config_path.exists():
            with open(config_path, "rb") as f:
                config = tomllib.load(f)
            keywords = config.get("account_keywords", {})
            self.REVENUE_KEYWORDS = keywords.get("revenue", [])
            self.OP_PROFIT_KEYWORDS = keywords.get("operating_profit", [])
            self.NET_INCOME_KEYWORDS = keywords.get("net_income", [])
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

        # 2. 독립적 분기 실적 매핑 및 역산 (초정밀 하이브리드 계산 엔진)
        corp_name = self._extract_corp_name([q1_stmt, semi_stmt, q3_stmt, annual_stmt])
        
        # 1분기는 단독이 곧 누적
        q1_final_cum = q1_cum
        q1_final_single = q1_single

        # 2분기 계산
        q2_final_single = FinancialMetrics(None, None, None)
        q2_final_cum = q1_final_cum
        
        if semi_stmt:
            # 누적 여부 지능적 판정:
            # 1) semi_stmt.is_cumulative 가 True 이거나
            # 2) 추출된 2Q 누적 매출액이 존재하고, 1Q 누적 매출액보다 크거나 같다면 -> 누적 공시가 확실함
            is_cum = False
            if getattr(semi_stmt, "is_cumulative", False):
                is_cum = True
            elif semi_cum.revenue is not None and q1_final_cum.revenue is not None:
                if semi_cum.revenue >= q1_final_cum.revenue:
                    is_cum = True
            
            if is_cum:
                q2_final_cum = semi_cum
                q2_final_single = self._calculate_diff(q2_final_cum, q1_final_cum)
            else:
                q2_final_single = semi_single
                q2_final_cum = self._add_metrics(q1_final_cum, q2_final_single)
        
        # 3분기 계산
        q3_final_single = FinancialMetrics(None, None, None)
        q3_final_cum = q2_final_cum
        
        if q3_stmt:
            # 누적 여부 지능적 판정:
            # 1) q3_stmt.is_cumulative 가 True 이거나
            # 2) 추출된 3Q 누적 매출액이 존재하고, 2Q 누적 매출액보다 크거나 같다면 -> 누적 공시가 확실함
            is_cum = False
            if getattr(q3_stmt, "is_cumulative", False):
                is_cum = True
            elif q3_cum.revenue is not None and q2_final_cum.revenue is not None:
                if q3_cum.revenue >= q2_final_cum.revenue:
                    is_cum = True
            
            if is_cum:
                q3_final_cum = q3_cum
                q3_final_single = self._calculate_diff(q3_final_cum, q2_final_cum)
            else:
                q3_final_single = q3_single
                q3_final_cum = self._add_metrics(q2_final_cum, q3_final_single)

        # 4분기 계산: Annual 누적 - Q3 누적 (DART API에서 단독 4Q를 주지 않으므로 항상 역산)
        q4_final_single = FinancialMetrics(None, None, None)
        if ann_cum.revenue is not None and q3_final_cum.revenue is not None:
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
                    val_str = item.thstrm_add_amount if hasattr(item, "thstrm_add_amount") and item.thstrm_add_amount else item.thstrm_amount
                else:
                    val_str = item.thstrm_amount
                if val_str and val_str.strip() not in ["", "-"]:
                    try:
                        reference_net_income = abs(int(re.sub(r'[^0-9-]', '', val_str)))
                        break  # 레퍼런스 값 하나를 확보하면 즉시 종료
                    except (ValueError, TypeError):
                        continue

        # 1. 완전 일치 우선순위 탐색 (키워드 순서 준수)
        for kw in keywords:
            for item in accounts:
                # 안전장치 1: sj_div가 명시적으로 BS(재무상태표)인 자본 항목은 손익 항목 매칭에서 전면 배제
                sj_div = getattr(item, "sj_div", None)
                if sj_div and sj_div.strip().upper() == "BS":
                    continue
                
                name = item.account_nm.strip()
                if name == kw:
                    if use_cumulative:
                        val_str = item.thstrm_add_amount if hasattr(item, "thstrm_add_amount") and item.thstrm_add_amount else item.thstrm_amount
                    else:
                        val_str = item.thstrm_amount
                        
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
                # 안전장치 1: sj_div가 명시적으로 BS인 경우 배제
                sj_div = getattr(item, "sj_div", None)
                if sj_div and sj_div.strip().upper() == "BS":
                    continue
                    
                name = item.account_nm.strip()
                if kw in name:
                    if use_cumulative:
                        val_str = item.thstrm_add_amount if hasattr(item, "thstrm_add_amount") and item.thstrm_add_amount else item.thstrm_amount
                    else:
                        val_str = item.thstrm_amount
                        
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
                for attr in ["thstrm_amount", "thstrm_add_amount"]:
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
                    for attr in ["thstrm_amount", "thstrm_add_amount"]:
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
