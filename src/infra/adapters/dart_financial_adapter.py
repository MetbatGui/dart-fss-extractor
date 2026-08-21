"""DART API 재무제표 어댑터."""

import json
import logging
import os
import time
from datetime import date, datetime
from pathlib import Path

import requests

from core.domain.models.financial_statement import (
    AccountItem,
    FinancialStatement,
    FinancialStatementType,
    ReportType,
)
from core.ports.api_financial_collector_port import ApiFinancialCollectorPort
from core.ports.financial_statement_port import FinancialStatementPort
from infra.adapters.dart_response_parser import DartResponseParser

logger = logging.getLogger(__name__)


class DartFinancialAdapter(FinancialStatementPort, ApiFinancialCollectorPort):
    """DART API REST (JSON) 기반 재무제표 수집 어댑터.

    - 연결재무제표 우선 조회, 실패 시 개별재무제표로 fallback
    - 로컬 캐싱 지원 (데이터 없음 상태 포함)
    """

    _API_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    _NO_DATA_MARKER = "NO_DATA"

    def __init__(
        self,
        api_key: str | None = None,
        use_cache: bool = True,
        cache_dir: str | None = None,
        cache_only: bool = False,
    ):
        """초기화.

        Args:
            api_key: DART API 키 (None이면 환경변수에서 읽음)
            use_cache: 캐시 사용 여부
            cache_dir: 캐시 저장 경로 (None이면 OUTPUT_DIRECTORY 환경변수 기반 기본 경로 사용)
            cache_only: True면 캐시에 없는 데이터는 API 호출 없이 건너뜀 (네트워크 요청 전무)
        """
        resolved_api_key = api_key or os.getenv("DART_API_KEY")
        if not resolved_api_key and not cache_only:
            raise OSError("DART_API_KEY가 설정되지 않았습니다.")
        self._api_key: str = resolved_api_key or ""
        self._use_cache = use_cache
        self._cache_only = cache_only
        if cache_dir:
            self._cache_dir = Path(cache_dir).resolve()
        else:
            self._cache_dir = (
                Path(os.getenv("OUTPUT_DIRECTORY", "./data")).resolve()
                / "financial_statements"
            )
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._call_count = 0

    @property
    def call_count(self) -> int:
        """API 호출 횟수 반환."""
        return self._call_count

    def get_financial_statement(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        prefer_consolidated: bool = True,
    ) -> FinancialStatement | None:
        """재무제표 조회 (통합 조회 메서드 활용)."""
        results = self.get_all_statements(corp_code, year, report_type)

        # 우선순위에 따라 반환
        fs_types = self._get_fs_type_priority(prefer_consolidated)
        for fs_type in fs_types:
            if fs_type in results:
                return results[fs_type]
        return None

    def get_all_statements(
        self, corp_code: str, year: int, report_type: ReportType
    ) -> dict[FinancialStatementType, FinancialStatement]:
        """연결과 개별 재무제표를 각각 DART API로 조회."""
        results = {}
        missing_types = []

        # 1. 캐시 확인
        for fs_type in [
            FinancialStatementType.CONSOLIDATED,
            FinancialStatementType.SEPARATE,
        ]:
            cached = self._load_from_cache(corp_code, year, report_type, fs_type)
            if cached:
                if cached != self._NO_DATA_MARKER:
                    results[fs_type] = cached
            else:
                missing_types.append(fs_type)

        # 2. 누락된 유형이 있으면 각각 API 호출 (cache_only 모드에서는 건너뜀)
        if self._cache_only:
            return results

        for fs_type in missing_types:
            params = self._build_api_params(corp_code, year, report_type, fs_type)

            try:
                data = self._request_with_rate_limit_retry(params, corp_code, year, fs_type)

                # 파싱 (CFS 또는 OFS 추출)
                new_results = DartResponseParser.parse_all(
                    data, corp_code, year, report_type
                )

                # 결과 캐싱 및 병합
                if fs_type in new_results:
                    fs = new_results[fs_type]
                    self._save_to_cache(fs)
                    results[fs_type] = fs
                elif data.get("status") == "013":
                    # DART가 명시적으로 "조회된 데이타가 없습니다"라고 응답한 경우에만
                    # '데이터 없음'으로 영구 캐시한다. 그 외(레이트리밋 "020", 인증오류,
                    # 서버에러 등)는 일시적 실패일 수 있으므로 캐시하지 않고 다음 실행에서 재시도한다.
                    self._save_negative_cache(corp_code, year, report_type, fs_type)
                else:
                    logger.error(
                        f"DART API 비정상 응답으로 데이터 없음 확정 불가 (재시도 대상): "
                        f"{corp_code} {year} {report_type.value} ({fs_type.value}) "
                        f"status={data.get('status')}"
                    )

            except Exception as e:
                logger.error(
                    f"API call failed for {corp_code} {year} {report_type.value} ({fs_type.value}): {e}"
                )

        return results

    def _request_with_rate_limit_retry(
        self,
        params: dict[str, str],
        corp_code: str,
        year: int,
        fs_type: FinancialStatementType,
        max_retries: int = 3,
        backoff_seconds: float = 5.0,
    ) -> dict:
        """DART API 호출. 레이트리밋(status 020) 응답이면 잠시 대기 후 재시도한다.

        재시도를 다 소진해도 020이면 마지막 응답을 그대로 반환한다 (호출부에서
        013이 아니므로 음성 캐시를 남기지 않고 다음 실행에서 재시도하게 된다).
        """
        for attempt in range(max_retries + 1):
            self._call_count += 1
            response = requests.get(self._API_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "020":
                return data

            if attempt < max_retries:
                logger.warning(
                    f"DART API 레이트리밋(020) 감지, {backoff_seconds}초 대기 후 재시도 "
                    f"({attempt + 1}/{max_retries}): {corp_code} {year} ({fs_type.value})"
                )
                time.sleep(backoff_seconds)

        return data

    def _get_fs_type_priority(
        self, prefer_consolidated: bool
    ) -> list[FinancialStatementType]:
        """재무제표 종류 우선순위 반환.

        Args:
            prefer_consolidated: 연결재무제표 우선 여부

        Returns:
            우선순위 리스트 ([연결, 개별] 또는 [개별, 연결])
        """
        if prefer_consolidated:
            return [
                FinancialStatementType.CONSOLIDATED,
                FinancialStatementType.SEPARATE,
            ]
        return [FinancialStatementType.SEPARATE, FinancialStatementType.CONSOLIDATED]

    def _save_negative_cache(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType,
    ) -> None:
        """'데이터 없음' 상태를 캐시에 저장."""
        if not self._use_cache:
            return

        cache_path = self._get_cache_path(corp_code, year, report_type, fs_type)
        data = {
            "status": "013",
            "message": "조회된 데이타가 없습니다.",
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_type": report_type.value,
            "fs_type": fs_type.value,
            "cached_at": datetime.now().isoformat(),
        }

        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _build_api_params(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType,
    ) -> dict[str, str]:
        """API 요청 파라미터 생성.

        Args:
            corp_code: 기업 코드
            year: 사업 연도
            report_type: 보고서 종류
            fs_type: 재무제표 종류

        Returns:
            API 요청 파라미터 딕셔너리
        """
        return {
            "crtfc_key": self._api_key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": report_type.value,
            "fs_div": fs_type.value,
        }

    def _get_cache_path(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType,
    ) -> Path:
        """캐시 파일 경로 생성."""
        corp_dir = self._cache_dir / corp_code
        corp_dir.mkdir(parents=True, exist_ok=True)

        report_name = {
            ReportType.ANNUAL: "annual",
            ReportType.SEMI_ANNUAL: "semi",
            ReportType.Q1: "q1",
            ReportType.Q3: "q3",
        }.get(report_type, "unknown")

        filename = f"{year}_{report_name}_{fs_type.value}.json"
        return corp_dir / filename

    def _save_to_cache(self, statement: FinancialStatement) -> None:
        """캐시에 저장."""
        if not self._use_cache:
            return

        cache_path = self._get_cache_path(
            statement.corp_code,
            statement.bsns_year,
            statement.reprt_type,
            statement.fs_type,
        )

        data = {
            "corp_code": statement.corp_code,
            "corp_name": statement.corp_name,
            "bsns_year": statement.bsns_year,
            "reprt_type": statement.reprt_type.value,
            "fs_type": statement.fs_type.value,
            "accounts": [
                {
                    "account_nm": acc.account_nm,
                    "thstrm_amount": str(acc.amount),
                    "thstrm_add_amount": str(acc.cumulative_amount),
                    "thstrm_nm": acc.period_name,
                    "sj_div": acc.statement_type,
                }
                for acc in statement.accounts
            ],
            "extracted_at": statement.extracted_at.isoformat(),
            "start_date": statement.start_date.isoformat()
            if statement.start_date
            else None,
            "end_date": statement.end_date.isoformat() if statement.end_date else None,
            "is_cumulative": statement.is_cumulative,
        }

        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_from_cache(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType,
    ) -> FinancialStatement | str | None:
        """캐시에서 로드. "데이터 없음"으로 확인된 캐시는 _NO_DATA_MARKER 문자열을 반환한다."""
        if not self._use_cache:
            return None

        cache_path = self._get_cache_path(corp_code, year, report_type, fs_type)
        if not cache_path.exists():
            return None

        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            if data.get("status") == "013":
                return self._NO_DATA_MARKER

            accounts = [
                AccountItem(
                    account_nm=item["account_nm"],
                    amount=item["thstrm_amount"],
                    cumulative_amount=item.get("thstrm_add_amount", ""),
                    period_name=item.get("thstrm_nm"),
                    statement_type=item.get("sj_div"),
                )
                for item in data["accounts"]
            ]

            start_date = (
                date.fromisoformat(data["start_date"])
                if data.get("start_date")
                else None
            )
            end_date = (
                date.fromisoformat(data["end_date"]) if data.get("end_date") else None
            )
            is_cumulative = data.get("is_cumulative", False)

            return FinancialStatement(
                corp_code=data["corp_code"],
                corp_name=data["corp_name"],
                bsns_year=data["bsns_year"],
                reprt_type=ReportType(data["reprt_type"]),
                fs_type=FinancialStatementType(data["fs_type"]),
                accounts=accounts,
                extracted_at=datetime.fromisoformat(data["extracted_at"]),
                start_date=start_date,
                end_date=end_date,
                is_cumulative=is_cumulative,
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def get_disclosures(
        self, bgn_de: str, end_de: str, pblntf_ty: str = "A"
    ) -> list[dict]:
        """지정된 날짜 범위의 정기 공시 목록 조회 (페이지네이션 지원)."""
        list_url = "https://opendart.fss.or.kr/api/list.json"
        all_disclosures = []
        page_no = 1
        page_count = 100

        while True:
            params = {
                "crtfc_key": self._api_key,
                "bgn_de": bgn_de,
                "end_de": end_de,
                "pblntf_ty": pblntf_ty,
                "page_no": str(page_no),
                "page_count": str(page_count),
            }
            try:
                self._call_count += 1
                response = requests.get(list_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                status = data.get("status")
                if status == "013":  # 데이터 없음
                    break
                if status != "000":  # 기타 에러
                    logger.error(f"DART 공시목록 조회 에러: {data.get('message')}")
                    break

                disclosures = data.get("list", [])
                all_disclosures.extend(disclosures)

                # 마지막 페이지에 도달하면 종료 (status가 "013"으로 떨어지지 않는 응답 대비)
                try:
                    total_page = int(data.get("total_page", page_no))
                except (TypeError, ValueError):
                    total_page = page_no
                if page_no >= total_page:
                    break

                page_no += 1
            except Exception as e:
                logger.error(f"DART 공시목록 조회 중 예외 발생: {e}")
                break

        return all_disclosures

    def get_settlement_month(
        self, corp_code: str, max_retries: int = 3, retry_backoff: float = 2.0
    ) -> int:
        """DART 기업개황 API를 통해 결산월을 조회합니다.

        조회된 결과는 정수로 반환합니다. 이 값은 Company 메타데이터에 최초 1회만
        저장되어 이후 재조회되지 않으므로, 일시적 커넥션 오류로 실패해도 기본값
        12를 반환해서는 안 됩니다 (실제 결산월이 12월이 아닌 기업이 영구히 잘못된
        값으로 고정되는 것을 방지). 재시도로도 끝내 확인하지 못하면 예외를 그대로
        전파해 호출부(financial_collection_service)가 재시도 대상으로 처리하게 합니다.
        """
        url = "https://opendart.fss.or.kr/api/company.json"
        params = {"crtfc_key": self._api_key, "corp_code": corp_code}
        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                self._call_count += 1
                resp = requests.get(url, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                if data.get("status") == "000":
                    acc_mt = data.get("acc_mt", "12")
                    logger.info(
                        f"DART에서 기업({corp_code})의 결산월 조회 성공: {acc_mt}월"
                    )
                    return int(acc_mt)
                logger.warning(
                    f"DART 기업개황 API 응답 이상 ({data.get('status')}): {data.get('message')}"
                )
                return 12
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    wait = retry_backoff * attempt
                    logger.warning(
                        f"  ⚠️ [재시도 {attempt}/{max_retries}] 결산월 조회 커넥션 오류로 "
                        f"{wait}초 대기 후 재시도 ({corp_code}): {e}"
                    )
                    time.sleep(wait)
        raise RuntimeError(
            f"결산월 조회 실패 (max_retries={max_retries}, corp_code={corp_code}): {last_error}"
        )
