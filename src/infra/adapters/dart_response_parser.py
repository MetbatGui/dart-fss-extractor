"""DART API 응답 파싱 유틸리티."""

from typing import List, Optional, Dict, Any, Tuple
from datetime import date, datetime

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
    def parse_financial_statement(
        response_data: Dict[str, Any],
        corp_code: str,
        year: int,
        report_type: ReportType,
        fs_type: FinancialStatementType
    ) -> Optional[FinancialStatement]:
        """API 응답을 FinancialStatement로 변환.
        
        Args:
            response_data: DART API 응답 데이터
            corp_code: 기업 코드
            year: 사업 연도
            report_type: 보고서 종류
            fs_type: 재무제표 종류
        
        Returns:
            파싱된 재무제표, 실패 시 None
        """
        # 상태 코드 확인
        if not DartResponseParser._is_valid_response(response_data):
            return None
        
        # 데이터 항목 추출
        items = response_data.get("list", [])
        if not items:
            print(f"No data - corp_code={corp_code}, year={year}, report={report_type.value}, fs={fs_type.value}")
            return None
        
        # 계정과목 파싱
        accounts = DartResponseParser._parse_accounts(items)
        
        # 기업명 추출
        corp_name = items[0].get("corp_name", "")
        
        # 날짜 정보 파싱
        start_date, end_date, is_cumulative = DartResponseParser._parse_date_info(items)
        
        return FinancialStatement(
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
            print(f"API Error - Status: {status}, Message: {data.get('message', 'N/A')}")
            return False
        return True
    
    @staticmethod
    def _parse_accounts(items: List[Dict[str, Any]]) -> List[AccountItem]:
        """계정과목 리스트 파싱.
        
        Args:
            items: API 응답의 list 필드
        
        Returns:
            파싱된 계정과목 리스트
        """
        return [
            AccountItem(
                account_nm=item.get("account_nm", ""),
                thstrm_amount=item.get("thstrm_amount", "")
            )
            for item in items
        ]
    
    @staticmethod
    def _parse_date_info(items: List[Dict[str, Any]]) -> Tuple[Optional[date], Optional[date], bool]:
        """날짜 정보 파싱.
        
        Args:
            items: API 응답의 list 필드
        
        Returns:
            (시작일, 종료일, 누적 여부) 튜플
        """
        if not items:
            return None, None, False
        
        thstrm_dt = items[0].get("thstrm_dt", "")
        if not thstrm_dt:
            return None, None, False
        
        try:
            # "2023.01.01 ~ 2023.09.30" 형식 파싱
            dates = thstrm_dt.split("~")
            if len(dates) != 2:
                return None, None, False
            
            start_str = dates[0].strip()
            end_str = dates[1].strip()
            start_date = datetime.strptime(start_str, "%Y.%m.%d").date()
            end_date = datetime.strptime(end_str, "%Y.%m.%d").date()
            
            # 누적 여부 판단 (시작일이 1월 1일인지 확인)
            is_cumulative = (start_date.month == 1 and start_date.day == 1)
            
            return start_date, end_date, is_cumulative
        
        except ValueError as e:
            print(f"[WARNING] Date parsing failed: {thstrm_dt}, Error: {e}")
            return None, None, False
