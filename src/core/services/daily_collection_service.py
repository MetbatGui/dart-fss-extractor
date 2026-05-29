"""당일 공시 기반 데일리 배치 증분 수집 서비스."""

import re
import logging
from datetime import datetime
from typing import List, Dict, Set, Optional
import pandas as pd

from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.repository_port import RepositoryPort
from core.domain.models.financial_statement import ReportType, FinancialStatementType
from core.domain.models.performance_metrics import QuarterlyMetrics
from core.domain.models.company import Company
from core.services.data_processing_service import DataProcessingService

logger = logging.getLogger(__name__)


class DailyCollectionService:
    """매일 등록되는 DART 공시 목록을 스캔하여 대상 기업만 핀포인트로 증분 수집하는 서비스."""

    # DART 결산월 → ReportType 및 분기 텍스트 매핑
    MONTH_TO_PERIOD = {
        "03": (ReportType.Q1, "1Q"),
        "06": (ReportType.SEMI_ANNUAL, "2Q"),
        "09": (ReportType.Q3, "3Q"),
        "12": (ReportType.ANNUAL, "4Q")
    }

    def __init__(
        self,
        corp_code_port: CorpCodePort,
        financial_port: FinancialStatementPort,
        repository_port: RepositoryPort,
        processing_service: DataProcessingService
    ):
        self._corp_code_port = corp_code_port
        self._financial_port = financial_port
        self._repository_port = repository_port
        self._processing_service = processing_service

    def collect_daily_disclosures(
        self,
        target_company_names: List[str],
        start_date: str,
        end_date: str
    ) -> Dict[str, List[str]]:
        """지정된 날짜 범위 동안 접수된 공시 목록을 스캔하고, 대상 기업과 매칭하여 실적을 수집합니다.
        
        Args:
            target_company_names: 수집 대상 기업명 리스트 (예: ["삼성전자", "SK하이닉스"])
            start_date: 검색 시작일 (YYYYMMDD)
            end_date: 검색 종료일 (YYYYMMDD)
            
        Returns:
            수집 결과 요약 (성공한 기업 코드 목록, 실패한 기업 코드 목록)
        ```
        """
        logger.info(f"📅 데일리 공시 스캔 시작: {start_date} ~ {end_date} (대상 기업 수: {len(target_company_names)})")
        
        # 1. 대상 기업들의 고유 DART corp_code 조회 및 셋(Set) 변환
        codes = self._corp_code_port.get_codes(target_company_names)
        target_corp_codes: Set[str] = {c for c in codes if c}
        corp_code_to_name: Dict[str, str] = {c: n for n, c in zip(target_company_names, codes) if c}

        if not target_corp_codes:
            logger.warning("유효한 수집 대상 DART 기업코드가 존재하지 않습니다.")
            return {"success": [], "failed": []}

        # 2. DART 정기 공시(유형 'A') 목록 API 호출
        disclosures = self._financial_port.get_disclosures(bgn_de=start_date, end_de=end_date, pblntf_ty="A")
        logger.info(f"🔍 기간 내 총 {len(disclosures)}건의 정기 공시가 감지되었습니다.")

        success_codes = []
        failed_codes = []

        # 3. 감지된 공시 스캔 및 필터링
        for disc in disclosures:
            corp_code = disc.get("corp_code")
            if corp_code not in target_corp_codes:
                continue  # 수집 대상 기업이 아니면 스킵

            report_nm = disc.get("report_nm", "")
            rcept_no = disc.get("rcept_no", "")
            rm = disc.get("rm", "")
            corp_name = corp_code_to_name[corp_code]

            logger.info(f"🎯 대상 기업 공시 포착: [{corp_name}] {report_nm} (접수번호: {rcept_no})")

            # 4. 공시 시기(연도, 분기) 및 정정 여부 동적 분석
            period_info = self.parse_report_period(report_nm, rm)
            if not period_info:
                logger.warning(f"  ❌ 공시 제목에서 결산 연월을 추출하지 못했습니다: {report_nm}")
                continue

            target_year = period_info["year"]
            target_quarter = period_info["quarter"]
            is_amendment = period_info["is_amendment"]

            logger.info(f"  📊 판별 정보: 대상 연도={target_year}, 분기={target_quarter}, 정정공시 여부={is_amendment}")

            # 5. 상세 데이터 핀포인트 수집 조율
            success = self._process_single_disclosure(
                corp_code=corp_code,
                corp_name=corp_name,
                year=target_year,
                quarter=target_quarter,
                is_amendment=is_amendment
            )

            if success:
                success_codes.append(corp_code)
            else:
                failed_codes.append(corp_code)

        logger.info(f"🏁 데일리 공시 수집 종료. 성공: {len(success_codes)}건, 실패: {len(failed_codes)}건")
        return {"success": success_codes, "failed": failed_codes}

    def parse_report_period(self, report_nm: str, rm: str = "") -> Optional[Dict]:
        """보고서 제목 및 비고 필드에서 실제 대상 연도, 분기, 정정 여부를 판별합니다."""
        # 괄호 안의 YYYY.MM 패턴 검색
        match = re.search(r"\((\d{4})\.(\d{2})\)", report_nm)
        if not match:
            return None

        year = int(match.group(1))
        month = match.group(2)

        period = self.MONTH_TO_PERIOD.get(month)
        if not period:
            return None

        report_type, quarter_str = period
        
        # 정정 여부 판별 (제목 내 기재정정/정정 텍스트 혹은 비고 필드 '정' 마크)
        is_amendment = "[기재정정]" in report_nm or "정정" in report_nm or "정" in rm.strip()

        return {
            "year": year,
            "quarter": quarter_str,
            "report_type": report_type,
            "is_amendment": is_amendment
        }

    def _process_single_disclosure(
        self,
        corp_code: str,
        corp_name: str,
        year: int,
        quarter: str,
        is_amendment: bool
    ) -> bool:
        """단일 공시 건에 대해 DART 상세 데이터를 수집하고 SQLite DB에 저장 조율합니다."""
        try:
            # 1. 기업 메타데이터 로드 또는 생성
            company = self._repository_port.load_company_metadata(corp_code)
            if not company:
                company = Company(code=corp_code, name=corp_name)

            # 정정공시가 아니고 이미 해당 연도가 성공한 경우 API 호출 방지를 위해 패스 가드
            if not is_amendment and year in company.success_years:
                logger.info(f"  💡 [{corp_name}] {year}년 데이터가 이미 성공적으로 저장되어 있어 스킵합니다.")
                return True

            # 2. 실적 역산을 위해 해당 연도의 1Q~4Q 보고서 전체를 수집 (안정성 보장)
            # (향후 부분 수집 고도화 가능하나, 분기 역산 정밀 복원을 위해 연간 패키지 수집 규칙 유지)
            statements = []
            for q_num in [1, 2, 3, 4]:
                rep_type = {1: ReportType.Q1, 2: ReportType.SEMI_ANNUAL, 3: ReportType.Q3, 4: ReportType.ANNUAL}[q_num]
                
                # 연결 재무제표 우선 조회
                stmt = self._financial_port.get_financial_statement(corp_code, year, rep_type)
                statements.append(stmt)

            # 계산에 유효한 데이터가 아예 없는 경우 에러 처리
            if not any(statements):
                logger.warning(f"  ❌ [{corp_name}] {year}년의 재무보고서 데이터가 존재하지 않습니다.")
                company.mark_failure(year)
                self._repository_port.save_company_metadata(company)
                return False

            # 3. 분기별 실적 가독 및 오염 역산 복원 작동 (Rich Domain Model 구동)
            # 연결(CFS) 및 개별(OFS) 둘 다 적재를 지원하도록 루프 처리
            for fs_type in [FinancialStatementType.CONSOLIDATED, FinancialStatementType.SEPARATE]:
                type_label = "CFS" if fs_type == FinancialStatementType.CONSOLIDATED else "OFS"
                
                metrics = QuarterlyMetrics.calculate_from_statements(
                    corp_name=corp_name,
                    q1_stmt=statements[0],
                    semi_stmt=statements[1],
                    q3_stmt=statements[2],
                    annual_stmt=statements[3],
                    revenue_kws=self._processing_service.REVENUE_KEYWORDS,
                    op_profit_kws=self._processing_service.OP_PROFIT_KEYWORDS,
                    net_income_kws=self._processing_service.NET_INCOME_KEYWORDS,
                    target_fs_type=fs_type
                )

                # 데이터프레임으로 하위 호환 매핑 및 DB 파티션 쓰기
                # 분기 데이터 구성
                quarter_rows = []
                for q_str in ["1Q", "2Q", "3Q", "4Q"]:
                    m = metrics.metrics_by_quarter.get(q_str)
                    if m and m.is_valid:
                        quarter_rows.append({
                            "기업명": corp_name,
                            "연도": year,
                            "구분": "분기",
                            "분기": q_str,
                            "구분_상세": "연결" if fs_type == FinancialStatementType.CONSOLIDATED else "개별",
                            "매출액": m.revenue,
                            "영업이익": m.operating_profit,
                            "당기순이익": m.net_income
                        })

                # 연간 데이터 구성
                if metrics.annual_metrics and metrics.annual_metrics.is_valid:
                    quarter_rows.append({
                        "기업명": corp_name,
                        "연도": year,
                        "구분": "연간",
                        "분기": "연간",
                        "구분_상세": "연결" if fs_type == FinancialStatementType.CONSOLIDATED else "개별",
                        "매출액": metrics.annual_metrics.revenue,
                        "영업이익": metrics.annual_metrics.operating_profit,
                        "당기순이익": metrics.annual_metrics.net_income
                    })

                if quarter_rows:
                    df = pd.DataFrame(quarter_rows)
                    dataset_name = f"financial_data_{type_label.lower()}"
                    # SQLite 저장 (동일 키 존재 시 덮어쓰기)
                    self._repository_port.save_partition(dataset_name, corp_code, df)

            # 4. 수집 완료 상태 마킹 및 메타데이터 갱신
            company.mark_success(year)
            self._repository_port.save_company_metadata(company)
            logger.info(f"  ✅ [{corp_name}] {year}년 재무 정보 적재 완료")
            return True

        except Exception as e:
            logger.error(f"  ❌ [{corp_name}] 수집/적재 도중 오류 발생: {e}")
            return False
