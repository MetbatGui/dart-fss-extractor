"""DART 공시 검색 및 로컬 XBRL 파싱 기반 통합 수집 어댑터."""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import requests

from core.ports.xbrl_financial_collector_port import XbrlFinancialCollectorPort
from core.ports.download_port import DownloadPort
from core.ports.xbrl_parser_port import XbrlParserPort
from core.domain.models.financial_statement import (
    FinancialStatement,
    FinancialStatementType,
    ReportType,
)
from infra.adapters.dart_download_adapter import DartDownloadAdapter
from infra.adapters.local_xbrl_parser_adapter import LocalXbrlParserAdapter

logger = logging.getLogger(__name__)


class DartXbrlFinancialAdapter(XbrlFinancialCollectorPort):
    """공시 검색 + 로컬 XBRL ZIP 다운로드 + XML 파싱을 조율하는 수집 어댑터."""

    _LIST_URL = "https://opendart.fss.or.kr/api/list.json"

    def __init__(
        self,
        api_key: Optional[str] = None,
        download_port: Optional[DownloadPort] = None,
        xbrl_parser_port: Optional[XbrlParserPort] = None,
        cache_dir: Optional[Path] = None
    ):
        """초기화.

        Args:
            api_key: DART API Key
            download_port: XBRL ZIP 다운로드 어댑터 (기본: DartDownloadAdapter)
            xbrl_parser_port: 로컬 XBRL XML 파서 어댑터 (기본: LocalXbrlParserAdapter)
            cache_dir: ZIP 파일 저장 캐시 디렉토리
        """
        import os
        self._api_key = api_key or os.getenv("DART_API_KEY")
        self._download_port = download_port or DartDownloadAdapter(api_key=self._api_key)
        self._xbrl_parser_port = xbrl_parser_port or LocalXbrlParserAdapter()
        self._cache_dir = cache_dir or Path("data/xbrl_zip")
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._call_count = 0

    @property
    def call_count(self) -> int:
        return self._call_count

    def get_disclosures(
        self,
        bgn_de: str,
        end_de: str,
        pblntf_ty: str = "A"
    ) -> List[Dict]:
        """지정된 날짜 범위의 정기 공시 목록을 DART API로 조회."""
        if not self._api_key:
            raise EnvironmentError("DART_API_KEY가 설정되지 않았습니다.")

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
                "page_count": str(page_count)
            }
            try:
                self._call_count += 1
                response = requests.get(self._LIST_URL, params=params, timeout=20)
                response.raise_for_status()
                data = response.json()

                status = data.get("status")
                if status == "013":  # 조회 결과 없음
                    break
                if status != "000":
                    logger.error(f"[DART API 오류] 상태코드: {status}, 메시지: {data.get('message')}")
                    break

                disclosures = data.get("list", [])
                all_disclosures.extend(disclosures)

                if len(disclosures) < page_count:
                    break
                page_no += 1
            except Exception as e:
                logger.error(f"[DART 공시 검색 실패]: {e}")
                break

        return all_disclosures

    def collect_from_disclosure(
        self,
        rcept_no: str,
        corp_code: str,
        corp_name: str,
        year: int,
        report_type: ReportType,
        acc_month: int = 12
    ) -> Dict[FinancialStatementType, FinancialStatement]:
        """접수번호(rcept_no)에 대해 로컬 캐시 검사/다운로드 후 XBRL XML 파싱 수행."""
        zip_file_path = self._cache_dir / f"{rcept_no}.zip"
        zip_data = None

        if zip_file_path.is_file():
            logger.info(f"  💾 [로컬 캐시 히트] 원본 XBRL ZIP 파일 사용: {zip_file_path.name}")
            zip_data = zip_file_path.read_bytes()
        else:
            logger.info(f"  📡 [XBRL 다운로드] 접수번호={rcept_no} 다운로드 시도...")
            try:
                fetched_data = self._download_port.download_xbrl_zip(rcept_no)
                if fetched_data and isinstance(fetched_data, (bytes, bytearray)):
                    zip_data = fetched_data
                    zip_file_path.write_bytes(zip_data)
                    logger.info(f"  💾 [다운로드 성공] 원본 ZIP 파일 캐시 저장: {zip_file_path.name}")
            except Exception as down_err:
                logger.warning(f"  ⚠️ 원본 ZIP 다운로드 중 경고/오류: {down_err}")

        if not zip_data:
            return {}

        try:
            return self._xbrl_parser_port.parse_xbrl_zip(
                zip_data=zip_data,
                corp_code=corp_code,
                corp_name=corp_name,
                year=year,
                report_type=report_type,
                acc_month=acc_month
            )
        except Exception as parse_err:
            logger.error(f"  ❌ [{corp_name}] 로컬 XBRL ZIP 파싱 실패: {parse_err}")
            return {}
