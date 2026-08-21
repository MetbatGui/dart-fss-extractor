"""재무제표 도메인 모델 - 풍부한 도메인 모델(Rich Domain Model) 구현."""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from core.domain.models.amount import Amount

logger = logging.getLogger(__name__)


class ReportType(Enum):
    """보고서 타입."""

    ANNUAL = "11011"
    SEMI_ANNUAL = "11012"
    Q1 = "11013"
    Q3 = "11014"


class FinancialStatementType(Enum):
    """재무제표 구분."""

    CONSOLIDATED = "CFS"  # 연결
    SEPARATE = "OFS"  # 개별


@dataclass
class AccountItem:
    """계정과목 항목."""

    account_nm: str  # 계정과목명
    amount: str | Amount  # 당기금액
    cumulative_amount: str | Amount | None = None  # 당기누적금액
    period_name: str | None = None  # 항목 기간명
    statement_type: str | None = None  # 재무제표 구분 (BS, IS 등)

    def __post_init__(self):
        # 하위 호환성 및 안정성을 위한 Amount 강제 캐스팅
        if not isinstance(self.amount, Amount):
            self.amount = Amount(self.amount)
        if self.cumulative_amount is not None and not isinstance(
            self.cumulative_amount, Amount
        ):
            self.cumulative_amount = Amount(self.cumulative_amount)
        elif self.cumulative_amount is None:
            self.cumulative_amount = Amount(None)

    @property
    def amount_value(self) -> Amount:
        """__post_init__이 보장하는 정규화된 Amount 뷰 (당기금액)."""
        assert isinstance(self.amount, Amount)
        return self.amount

    @property
    def cumulative_value(self) -> Amount:
        """__post_init__이 보장하는 정규화된 Amount 뷰 (당기누적금액)."""
        assert isinstance(self.cumulative_amount, Amount)
        return self.cumulative_amount


@dataclass
class FinancialStatement:
    """재무제표 엔티티 (Rich Domain Model)."""

    corp_code: str
    corp_name: str
    bsns_year: int
    reprt_type: ReportType
    fs_type: FinancialStatementType
    accounts: list[AccountItem]
    extracted_at: datetime = field(default_factory=datetime.now)
    rcept_no: str | None = None  # 접수번호 (실제 제출/마감일자 역산용)

    # 기간 정보 (정확한 계산을 위해 추가)
    start_date: date | None = None
    end_date: date | None = None
    is_cumulative: bool = False  # True면 누적 데이터 (예: 1.1 ~ 6.30)

    def find_account_amount(
        self, keywords: list[str], use_cumulative: bool = False
    ) -> Amount:
        """지정된 우선순위 키워드에 해당하는 계정과목 금액을 안전하게 반환합니다.

        - BS(재무상태표) 항목은 손익 매칭에서 전면 제외하는 안전 가드가 동작합니다.
        - 통합 매출 계정이 없고 세부 분할 계정(수출/내수)만 있는 특수 공시 양식일 경우 자동 합산하여 반환합니다.
        """
        # 자본-손익 오매칭 방어를 위한 당기순이익 레퍼런스 값 파악
        ref_net_income: Amount | None = None
        ref_keywords = [
            "당기순이익",
            "당기순이익(손실)",
            "당기순손실",
            "분기순이익",
            "분기순이익(손실)",
            "분기순손실",
            "반기순이익",
            "반기순이익(손실)",
            "반기순손실",
        ]
        for item in self.accounts:
            if item.statement_type and item.statement_type.strip().upper() in ("BS", "CF"):
                continue
            if item.account_nm.strip() in ref_keywords:
                val = (
                    item.cumulative_value
                    if use_cumulative and not item.cumulative_value.is_none
                    else item.amount_value
                )
                if not val.is_none:
                    ref_net_income = abs(val)
                    break

        # [지능형 분할 매출 합산 가드]
        is_revenue_search = any(
            kw in ["매출액", "수익(매출액)", "영업수익", "매출"] for kw in keywords
        )
        if is_revenue_search:
            has_integrated = any(
                item.account_nm.strip() in ["매출액", "영업수익", "매출"]
                and not (
                    item.statement_type and item.statement_type.strip().upper() in ("BS", "CF")
                )
                for item in self.accounts
            )
            if not has_integrated:
                export_val = Amount(None)
                domestic_val = Amount(None)
                for item in self.accounts:
                    if (
                        item.statement_type
                        and item.statement_type.strip().upper() in ("BS", "CF")
                    ):
                        continue
                    nm = item.account_nm.strip()
                    val = (
                        item.cumulative_value
                        if use_cumulative and not item.cumulative_value.is_none
                        else item.amount_value
                    )
                    if val.is_none:
                        continue
                    if "수출" in nm:
                        export_val = val
                    elif "내수" in nm:
                        domestic_val = val

                if not export_val.is_none and not domestic_val.is_none:
                    logger.info(
                        f"[CORE DOMAIN MODEL] 분할 매출 합산 처리: 수출({export_val}) + 내수({domestic_val}) = {export_val + domestic_val}"
                    )
                    return export_val + domestic_val

        # 1. 완전 일치 우선순위 검색
        for kw in keywords:
            for item in self.accounts:
                if item.statement_type and item.statement_type.strip().upper() in ("BS", "CF"):
                    continue
                if item.account_nm.strip() == kw:
                    val = (
                        item.cumulative_value
                        if use_cumulative and not item.cumulative_value.is_none
                        else item.amount_value
                    )
                    if val.is_none:
                        continue
                    # 지배주주지분 오매칭 필터링 가드
                    if kw in [
                        "지배기업의 소유주지분",
                        "지배기업 소유주지분",
                        "지배기업의소유주지분",
                    ]:
                        if (
                            ref_net_income is not None
                            and abs(val) > ref_net_income * 1.1
                        ):
                            continue
                    return val

        # 2. 부분 일치 우선순위 검색
        for kw in keywords:
            for item in self.accounts:
                if item.statement_type and item.statement_type.strip().upper() in ("BS", "CF"):
                    continue
                nm_stripped = item.account_nm.strip()
                # "기본주당계속영업당기순이익"처럼 EPS(주당) 계정이 "당기순이익"을
                # 부분 문자열로 포함해 총액 대신 주당 금액이 오매칭되는 것을 방지
                if "주당" in nm_stripped:
                    continue
                if kw in nm_stripped:
                    val = (
                        item.cumulative_value
                        if use_cumulative and not item.cumulative_value.is_none
                        else item.amount_value
                    )
                    if val.is_none:
                        continue
                    if kw in [
                        "지배기업의 소유주지분",
                        "지배기업 소유주지분",
                        "지배기업의소유주지분",
                    ]:
                        if (
                            ref_net_income is not None
                            and abs(val) > ref_net_income * 1.1
                        ):
                            continue
                    return val

        # 3. 당기순이익 최후 폴백: "분기연결순이익"처럼 접두어가 붙어 부분일치도
        # 실패하는 변형을 넓게 잡기 위해 "순이익"/"순손실"로 끝나는 계정을 탐색한다.
        is_net_income_search = any("순이익" in kw for kw in keywords)
        if is_net_income_search:
            for item in self.accounts:
                # SCE(자본변동표)에는 "현금흐름위험회피순손익" 같은 기타포괄손익
                # 세부항목도 "순손익"으로 끝나 오매칭될 수 있어 함께 제외한다.
                if item.statement_type and item.statement_type.strip().upper() in (
                    "BS",
                    "CF",
                    "SCE",
                ):
                    continue
                nm = item.account_nm.strip()
                # "법인세비용차감전순이익"(세전이익)은 순이익이 아니므로 폴백에서 제외
                if "법인세" in nm:
                    continue
                # 진짜 순이익 계정은 항상 "당기/분기/반기"로 시작한다.
                # "현금흐름위험회피순손익"처럼 접미사만 같은 기타포괄손익
                # 세부항목(헤지/환산/재측정 등)을 배제하기 위한 접두어 가드.
                if not nm.startswith(("당기", "분기", "반기")):
                    continue
                if (
                    nm.endswith("순이익")
                    or nm.endswith("순손실")
                    or nm.endswith("순손익")
                ):
                    val = (
                        item.cumulative_value
                        if use_cumulative and not item.cumulative_value.is_none
                        else item.amount_value
                    )
                    if val.is_none:
                        continue
                    return val

        return Amount(None)

    @staticmethod
    def normalize_scales(statements: list["FinancialStatement"]) -> None:
        """대표 수치들의 대조 분석을 통해 보고서 간 자릿수(Scale) 불일치를 자동으로 보정합니다."""
        valid_stmts = [s for s in statements if s]
        if len(valid_stmts) < 2:
            return

        import math

        scales = []
        stmt_to_scale = {}

        for stmt in valid_stmts:
            vals = []
            for item in stmt.accounts:
                for attr in ["amount", "cumulative_amount"]:
                    amt_obj = getattr(item, attr)
                    if amt_obj and not amt_obj.is_none:
                        val = abs(int(amt_obj))
                        if val > 0:
                            vals.append(val)
            if vals:
                vals.sort()
                median_val = vals[len(vals) // 2]
                stmt_to_scale[id(stmt)] = median_val
                scales.append(median_val)

        if len(scales) < 2:
            return

        log_scales = [math.log10(s) for s in scales]
        log_scales.sort()
        base_log = log_scales[len(log_scales) // 2]

        for stmt in valid_stmts:
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
                    f"[{stmt.corp_name} ({stmt.bsns_year} {stmt.reprt_type})] 도메인 스케일 불일치 감지. "
                    f"대표값: {stmt_scale:.1e}, 기준값: {10**base_log:.1e}. "
                    f"자릿수 차이: {exponent}. 보정 배율: {factor}"
                )

                # 내부 값 객체 스케일 변환 적용
                scale_multiplier = (
                    factor if multiply else (Decimal(1) / Decimal(str(factor)))
                )
                for item in stmt.accounts:
                    item.amount = item.amount_value.scale(scale_multiplier)
                    if not item.cumulative_value.is_none:
                        item.cumulative_amount = item.cumulative_value.scale(
                            scale_multiplier
                        )
