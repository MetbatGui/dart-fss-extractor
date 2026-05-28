"""DART API 응답 파싱 유틸리티."""

import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import date, datetime

logger = logging.getLogger(__name__)

from core.domain.models.financial_statement import (
    FinancialStatement,
    AccountItem,
    ReportType,
    FinancialStatementType
)


class DartResponseParser:
    """DART API 응답을 도메인 모델로 변환하는 파서.
    
    DART API의 JSON 응답을 FinancialStatement 도메인 모델로 변환합니다.
    """
    
    @staticmethod
    def parse_all(
        response_data: Dict[str, Any],
        corp_code: str,
        year: int,
        report_type: ReportType
    ) -> Dict[FinancialStatementType, FinancialStatement]:
        """API 응답에서 연결과 개별 재무제표를 모두 파싱.
        
        Returns:
            {FinancialStatementType.CONSOLIDATED: FS, FinancialStatementType.SEPARATE: FS} 형식의 딕셔너리
        """
        results = {}
        if not DartResponseParser._is_valid_response(response_data):
            return results
            
        items = response_data.get("list", [])
        if not items:
            return results
            
        corp_name = items[0].get("corp_name", "")
        start_date, end_date, is_cumulative = DartResponseParser._parse_date_info(items, report_type)
        
        # 연결(CFS)과 개별(OFS) 각각 시도
        for fs_type in [FinancialStatementType.CONSOLIDATED, FinancialStatementType.SEPARATE]:
            accounts = DartResponseParser._parse_accounts(items, fs_type)
            if accounts:
                results[fs_type] = FinancialStatement(
                    corp_code=corp_code,
                    corp_name=corp_name,
                    bsns_year=year,
                    reprt_type=report_type,
                    fs_type=fs_type,
                    accounts=accounts,
                    extracted_at=datetime.now(),
                    start_date=start_date,
                    end_date=end_date,
                    is_cumulative=is_cumulative
                )
        return results

    @staticmethod
    def parse_financial_statement(
        response_data: Dict[str, Any],
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType
    ) -> Optional[FinancialStatement]:
        """API 응답을 단일 FinancialStatement로 변환 (기존 호환성용)."""
        all_results = DartResponseParser.parse_all(response_data, corp_code, year, report_type)
        return all_results.get(fs_type)
    
    @staticmethod
    def _is_valid_response(data: Dict[str, Any]) -> bool:
        """응답 유효성 검증.
        
        Args:
            data: DART API 응답 데이터
        
        Returns:
            유효하면 True, 아니면 False
        """
        status = data.get("status")
        if status != "000":
            msg = data.get('message', 'N/A')
            if status == "013":
                # "조회된 데이타가 없습니다" - 빈번하게 발생하므로 INFO 레벨로 기록
                logger.info(f"DART API: {msg} (Status: 013)")
            else:
                logger.error(f"DART API Error - Status: {status}, Message: {msg}")
            return False
        return True
    
    @staticmethod
    def _parse_accounts(items: List[Dict[str, Any]], fs_type: FinancialStatementType) -> List[AccountItem]:
        """계정과목 리스트 파싱 (유형별 필터링 포함).
        
        Args:
            items: API 응답의 list 필드
            fs_type: 필터링할 재무제표 종류
        
        Returns:
            파싱된 계정과목 리스트
        """
        # FinancialStatementType.CONSOLIDATED -> "CFS"
        # FinancialStatementType.SEPARATE -> "OFS"
        target_div = "CFS" if fs_type == FinancialStatementType.CONSOLIDATED else "OFS"
        
        return [
            AccountItem(
                account_nm=item.get("account_nm", ""),
                amount=item.get("thstrm_amount", ""),
                cumulative_amount=item.get("thstrm_add_amount", ""),
                period_name=item.get("thstrm_nm", ""),
                statement_type=item.get("sj_div", "")
            )
            for item in items
            if (not item.get("fs_div") or str(item.get("fs_div")).strip() == "" or item.get("fs_div") == target_div)
        ]
    
    @staticmethod
    def _parse_date_info(items: List[Dict[str, Any]], report_type: ReportType) -> Tuple[Optional[date], Optional[date], bool]:
        """날짜 정보 파싱.
        
        Args:
            items: API 응답의 list 필드
            report_type: 보고서 종류
        
        Returns:
            (시작일, 종료일, 누적 여부) 튜플
        """
        if not items:
            return None, None, False
        
        # 기본값 설정 (보고서 종류에 따른 추정)
        # 1분기, 반기, 3분기, 연간 보고서는 기본적으로 해당 시점까지의 누적 실적을 포함함
        is_cumulative = report_type in [ReportType.Q1, ReportType.SEMI_ANNUAL, ReportType.Q3, ReportType.ANNUAL]
        
        # thstrm_nm(항목명)에 "누적"이 포함되어 있는지 확인하여 누적 여부 판단 보강
        thstrm_nm = items[0].get("thstrm_nm", "")
        has_cumulative_keyword = "누적" in thstrm_nm

        thstrm_dt = items[0].get("thstrm_dt", "")
        if not thstrm_dt:
            # 날짜가 없으면 보고서 종류와 키워드에 의존
            return None, None, (report_type == ReportType.ANNUAL or has_cumulative_keyword)
        
        try:
            # "2023.01.01 ~ 2023.09.30" 형식 파싱 시도
            if "~" in thstrm_dt:
                dates = thstrm_dt.split("~")
                if len(dates) == 2:
                    start_str = dates[0].strip()
                    end_str = dates[1].strip()
                    start_date = datetime.strptime(start_str, "%Y.%m.%d").date()
                    end_date = datetime.strptime(end_str, "%Y.%m.%d").date()
                    
                    # 시작일이 1월 1일이면 확실히 누적
                    is_cumulative = (start_date.month == 1 and start_date.day == 1)
                    return start_date, end_date, is_cumulative
            
            # 범위 형식인데 잘못 구획된 형태(예: "2023.01.01 - 2023.06.30")에 대한 방어
            if "-" in thstrm_dt and thstrm_dt.count(".") > 2:
                raise ValueError("비정상적인 범위 형식 날짜 구조 감지")
            
            # 단일 날짜인 경우 (예: "2023.09.30")
            # 일단 보고서 유형에 따른 기본값 유지 (DataProcessingService에서 지능적으로 최종 판단함)
            is_cumulative = is_cumulative or has_cumulative_keyword
            
            clean_dt = thstrm_dt.strip().replace(".", "-")
            if len(clean_dt) >= 10:
                end_date = datetime.strptime(clean_dt[:10].replace("-", "."), "%Y.%m.%d").date()
                return None, end_date, is_cumulative
            
            return None, None, is_cumulative
        
        except ValueError as e:
            logger.warning(f"Date parsing failed: {thstrm_dt}, Error: {e}")
            return None, None, is_cumulative
