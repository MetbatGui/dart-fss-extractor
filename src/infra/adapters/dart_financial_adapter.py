"""DART API 재무제표 어댑터."""

import json
import os
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict
import requests

from core.domain.models.financial_statement import (
    AccountItem,
    FinancialStatement,
    FinancialStatementType,
    ReportType,
)
from core.ports.financial_statement_port import FinancialStatementPort
from infra.adapters.dart_response_parser import DartResponseParser


class DartFinancialAdapter(FinancialStatementPort):
    """DART API를 통한 재무제표 조회 어댑터.
    
    - 연결재무제표 우선 조회, 실패 시 개별재무제표로 fallback
    - 로컬 캐싱 지원
    """

    _API_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    _CACHE_DIR = Path(os.getenv("OUTPUT_DIRECTORY", "./data")).resolve() / "financial_statements"

    def __init__(self, api_key: Optional[str] = None, use_cache: bool = True):
        """초기화.
        
        Args:
            api_key: DART API 키 (None이면 환경변수에서 읽음)
            use_cache: 캐시 사용 여부
        """
        self._api_key = api_key or os.getenv("DART_API_KEY")
        if not self._api_key:
            raise EnvironmentError("DART_API_KEY가 설정되지 않았습니다.")
        self._use_cache = use_cache
        self._CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def get_financial_statement(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        prefer_consolidated: bool = True
    ) -> Optional[FinancialStatement]:
        """재무제표 조회."""
        # 1. 캐시 확인
        if self._use_cache:
            # 우선순위에 따라 캐시 확인
            if prefer_consolidated:
                cached = self._load_from_cache(corp_code, year, report_type, FinancialStatementType.CONSOLIDATED)
                if cached:
                    return cached
                cached = self._load_from_cache(corp_code, year, report_type, FinancialStatementType.SEPARATE)
                if cached:
                    return cached
            else:
                cached = self._load_from_cache(corp_code, year, report_type, FinancialStatementType.SEPARATE)
                if cached:
                    return cached
                cached = self._load_from_cache(corp_code, year, report_type, FinancialStatementType.CONSOLIDATED)
                if cached:
                    return cached

        # 2. API 호출 - 양방향 fallback
        if prefer_consolidated:
            # CFS 우선 → OFS fallback
            statement = self._fetch_from_api(corp_code, year, report_type, FinancialStatementType.CONSOLIDATED)
            if statement:
                self._save_to_cache(statement)
                return statement
            
            statement = self._fetch_from_api(corp_code, year, report_type, FinancialStatementType.SEPARATE)
            if statement:
                self._save_to_cache(statement)
            return statement
        else:
            # OFS 우선 → CFS fallback
            statement = self._fetch_from_api(corp_code, year, report_type, FinancialStatementType.SEPARATE)
            if statement:
                self._save_to_cache(statement)
                return statement
            
            statement = self._fetch_from_api(corp_code, year, report_type, FinancialStatementType.CONSOLIDATED)
            if statement:
                self._save_to_cache(statement)
            return statement

    def _fetch_from_api(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType
    ) -> Optional[FinancialStatement]:
        """DART API 호출 및 파싱.
        
        API 호출과 응답 파싱을 DartResponseParser에 위임합니다.
        """
        params = self._build_api_params(corp_code, year, report_type, fs_type)
        
        try:
            response = requests.get(self._API_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # 파싱 로직을 DartResponseParser에 위임
            return DartResponseParser.parse_financial_statement(
                data, corp_code, year, report_type, fs_type
            )
        
        except (requests.RequestException, json.JSONDecodeError, KeyError):
            return None
    
    def _build_api_params(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType
    ) -> Dict[str, str]:
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
        fs_type: FinancialStatementType
    ) -> Path:
        """캐시 파일 경로 생성."""
        corp_dir = self._CACHE_DIR / corp_code
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
            statement.fs_type
        )

        data = {
            "corp_code": statement.corp_code,
            "corp_name": statement.corp_name,
            "bsns_year": statement.bsns_year,
            "reprt_type": statement.reprt_type.value,
            "fs_type": statement.fs_type.value,
            "accounts": [
                {"account_nm": acc.account_nm, "thstrm_amount": acc.thstrm_amount}
                for acc in statement.accounts
            ],
            "extracted_at": statement.extracted_at.isoformat(),
            "start_date": statement.start_date.isoformat() if statement.start_date else None,
            "end_date": statement.end_date.isoformat() if statement.end_date else None,
            "is_cumulative": statement.is_cumulative
        }

        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_from_cache(
        self,
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType
    ) -> Optional[FinancialStatement]:
        """캐시에서 로드."""
        if not self._use_cache:
            return None

        cache_path = self._get_cache_path(corp_code, year, report_type, fs_type)
        if not cache_path.exists():
            return None

        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            accounts = [
                AccountItem(
                    account_nm=item["account_nm"],
                    thstrm_amount=item["thstrm_amount"]
                )
                for item in data["accounts"]
            ]

            start_date = date.fromisoformat(data["start_date"]) if data.get("start_date") else None
            end_date = date.fromisoformat(data["end_date"]) if data.get("end_date") else None
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
                is_cumulative=is_cumulative
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            return None
