"""당일 공시 기반 데일리 배치 증분 수집 서비스."""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional
import pandas as pd

from pathlib import Path
from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.xbrl_financial_collector_port import XbrlFinancialCollectorPort
from core.ports.repository_port import RepositoryPort
from core.ports.cache_port import CachePort
from core.ports.download_port import DownloadPort
from core.ports.xbrl_parser_port import XbrlParserPort
from core.domain.models.financial_statement import ReportType, FinancialStatementType
from core.domain.models.company import Company
from core.domain.models.disclosure_period import DisclosurePeriod
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
        cache_port: Optional[CachePort] = None,
        download_port: Optional[DownloadPort] = None,
        xbrl_parser_port: Optional[XbrlParserPort] = None,
        processing_service: Optional[DataProcessingService] = None,
        xbrl_collector_port: Optional[XbrlFinancialCollectorPort] = None
    ):
        self._corp_code_port = corp_code_port
        self._financial_port = financial_port
        self._repository_port = repository_port
        self._cache_port = cache_port
        self._download_port = download_port
        self._xbrl_parser_port = xbrl_parser_port
        self._processing_service = processing_service or DataProcessingService()
        self._xbrl_collector_port = xbrl_collector_port

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

        current_time = datetime.now().isoformat()

        # 2-1. 캐시 사전 일괄 로드 및 만료 제거
        try:
            cache_dict = self._cache_port.load_all()
            cache_dict = {
                rcp_no: val for rcp_no, val in cache_dict.items()
                if val.get("expired_at", "") > current_time
            }
            logger.info(f"💾 로드된 유효 공시 캐시 개수: {len(cache_dict)}개")
        except Exception as e:
            logger.error(f"  ❌ 캐시 로드 중 예외 발생 (비어있는 캐시로 진행): {e}")
            cache_dict = {}

        cache_updated = False

        # 3. 감지된 공시 스캔 및 필터링
        for disc in disclosures:
            corp_code = disc.get("corp_code")
            if corp_code not in target_corp_codes:
                continue  # 수집 대상 기업이 아니면 스킵

            report_nm = disc.get("report_nm", "")
            rcept_no = disc.get("rcept_no", "")
            rm = disc.get("rm", "")
            corp_name = corp_code_to_name[corp_code]

            # 3-1. 일괄 로드된 캐시 필터링 검사 (메모리 O(1) 매칭)
            if rcept_no in cache_dict:
                valid_cache = cache_dict[rcept_no]
                logger.info(f"  💡 [캐시 히트] [{corp_name}] {report_nm} ({rcept_no}) - 이미 최근 3일 이내에 수집된 기록이 존재하여 스킵합니다. (만료일시: {valid_cache.get('expired_at')})")
                success_codes.append(corp_code)
                continue

            logger.info(f"🎯 대상 기업 공시 포착: [{corp_name}] {report_nm} (접수번호: {rcept_no})")

            # 기업 정보 조회해서 결산월 얻어오기
            company = self._get_or_create_company(corp_code, corp_name)
            settlement_month = company.settlement_month

            # 4. 공시 시기(연도, 분기) 및 정정 여부 동적 분석
            period_info = self.parse_report_period(report_nm, settlement_month, rm)
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
                is_amendment=is_amendment,
                rcept_no=rcept_no
            )

            if success:
                success_codes.append(corp_code)
                # 수집 성공 시 메모리 캐시 딕셔너리에 추가
                collected_time = datetime.now()
                expired_time = collected_time + timedelta(days=3)
                cache_dict[rcept_no] = {
                    "corp_code": corp_code,
                    "corp_name": corp_name,
                    "report_nm": report_nm,
                    "collected_at": collected_time.isoformat(),
                    "expired_at": expired_time.isoformat()
                }
                cache_updated = True
                logger.info(f"  [메모리 캐시 갱신] {corp_name} / {rcept_no} (만료예정: {expired_time.isoformat()})")
            else:
                failed_codes.append(corp_code)

        # 6. 캐시 갱신 건이 존재하면 최종 디스크에 단 1회 일괄 기록
        if cache_updated:
            try:
                self._cache_port.save_all(cache_dict)
                logger.info("💾 갱신된 수집 캐시 딕셔너리를 디스크(JSON 파일)에 일괄 영속화 완료했습니다.")
            except Exception as e:
                logger.error(f"  ❌ 수집 캐시 파일 일괄 영속화 실패: {e}")

        logger.info(f"🏁 데일리 공시 수집 종료. 성공: {len(success_codes)}건, 실패: {len(failed_codes)}건")
        return {"success": success_codes, "failed": failed_codes}

    def parse_report_period(self, report_nm: str, settlement_month: int = 12, rm: str = "") -> Optional[Dict]:
        """보고서 제목 및 비고 필드에서 실제 DART 기준 대상 연도, 분기, 정정 여부를 판별합니다.

        판별 규칙 자체는 도메인 모델(DisclosurePeriod)에 위임하고, 기존 호출부와의
        호환성을 위해 Dict 형태로 변환하여 반환합니다.
        """
        period = DisclosurePeriod.from_report_title(report_nm, settlement_month, rm)
        if not period:
            return None

        return {
            "year": period.year,
            "quarter": period.quarter,
            "report_type": period.report_type,
            "is_amendment": period.is_amendment
        }

    def _process_single_disclosure(
        self,
        corp_code: str,
        corp_name: str,
        year: int,
        quarter: str,
        is_amendment: bool,
        rcept_no: str
    ) -> bool:
        """단일 공시 건의 XBRL 데이터를 수집해 SQLite DB에 저장합니다.

        정기 수집에서는 OpenDART 재무제표 API로 폴백하지 않습니다. XBRL을
        다운로드하거나 파싱하지 못하면 실패 이력을 남겨 다음 실행에서 재시도합니다.
        """
        try:
            # 1. 기업 메타데이터 로드 또는 생성
            company = self._get_or_create_company(corp_code, corp_name)

            # 정정공시가 아니고 이미 해당 연도가 성공한 경우 API 호출 방지를 위해 패스 가드
            if not is_amendment and year in company.success_years:
                logger.info(f"  💡 [{corp_name}] {year}년 데이터가 이미 성공적으로 저장되어 있어 스킵합니다.")
                return True

            # 2. XBRL 파싱 수집 실행 (XbrlCollectorPort 또는 로컬 파서 포트 사용)
            statements = {}
            report_type_mapping = {
                "1Q": ReportType.Q1,
                "2Q": ReportType.SEMI_ANNUAL,
                "3Q": ReportType.Q3,
                "4Q": ReportType.ANNUAL
            }
            rep_type = report_type_mapping.get(quarter, ReportType.Q1)

            if self._xbrl_collector_port:
                statements = self._xbrl_collector_port.collect_from_disclosure(
                    rcept_no=rcept_no,
                    corp_code=corp_code,
                    corp_name=corp_name,
                    year=year,
                    report_type=rep_type,
                    acc_month=company.settlement_month
                )
            else:
                # 레거시 직접 파싱 방식 보장
                cache_dir = Path("data/xbrl_zip")
                cache_dir.mkdir(parents=True, exist_ok=True)
                zip_file_path = cache_dir / f"{rcept_no}.zip"

                zip_data = None
                if zip_file_path.is_file():
                    logger.info(f"  💾 [로컬 캐시 히트] 원본 XBRL ZIP 파일 발견: {zip_file_path.name}")
                    zip_data = zip_file_path.read_bytes()
                elif self._download_port:
                    logger.info(f"  📡 [다운로드 시도] XBRL ZIP 다운로드 시작 (접수번호: {rcept_no})")
                    try:
                        fetched_data = self._download_port.download_xbrl_zip(rcept_no)
                        if fetched_data and isinstance(fetched_data, (bytes, bytearray)):
                            zip_data = fetched_data
                            zip_file_path.write_bytes(zip_data)
                            logger.info(f"  💾 [다운로드 성공] 원본 ZIP 파일 캐시 저장 완료: {zip_file_path.name}")
                    except Exception as down_err:
                        logger.warning(f"  ⚠️ 원본 ZIP 다운로드 또는 로컬 쓰기 중 경고: {down_err}")

                if zip_data and self._xbrl_parser_port:
                    try:
                        statements = self._xbrl_parser_port.parse_xbrl_zip(
                            zip_data=zip_data,
                            corp_code=corp_code,
                            corp_name=corp_name,
                            year=year,
                            report_type=rep_type,
                            acc_month=company.settlement_month
                        )
                    except Exception as parse_err:
                        logger.error(f"  ❌ [{corp_name}] 로컬 XBRL ZIP 파싱 실패: {parse_err}")

            # 4. 파싱 성공 시 DB 적재
            if statements:
                logger.info(f"  🎯 [{corp_name}] 로컬 파싱 데이터 수집 성공. 저장소 적재를 가동합니다.")
                for fs_type, stmt in statements.items():
                    type_label = "cfs" if fs_type == FinancialStatementType.CONSOLIDATED else "ofs"
                    
                    # 분기 데이터 구성
                    quarter_rows = []
                    
                    # 수치 찾기 (매출/영익/순익)
                    rev_amount = stmt.find_account_amount(list(self._processing_service.REVENUE_KEYWORDS))
                    op_amount = stmt.find_account_amount(list(self._processing_service.OP_PROFIT_KEYWORDS))
                    ni_amount = stmt.find_account_amount(list(self._processing_service.NET_INCOME_KEYWORDS))

                    # 캘린더 분기 보정
                    c_year, c_quarter = year, quarter
                    try:
                        c_year, c_quarter = company.to_calendar_period(year, quarter)
                    except Exception as e:
                        logger.error(f"[{corp_name}] 캘린더 분기 보정 중 오류: {e}")

                    quarter_rows.append({
                        "기업명": corp_name,
                        "연도": c_year,
                        "구분": "분기",
                        "분기": c_quarter,
                        "구분_상세": "연결" if fs_type == FinancialStatementType.CONSOLIDATED else "개별",
                        "매출액": int(rev_amount) if not rev_amount.is_none else None,
                        "영업이익": int(op_amount) if not op_amount.is_none else None,
                        "당기순이익": int(ni_amount) if not ni_amount.is_none else None
                    })
                    
                    # 연간 데이터 구성 (사업보고서인 경우 추가 적재)
                    if rep_type == ReportType.ANNUAL:
                        quarter_rows.append({
                            "기업명": corp_name,
                            "연도": year,
                            "구분": "연간",
                            "분기": "연간",
                            "구분_상세": "연결" if fs_type == FinancialStatementType.CONSOLIDATED else "개별",
                            "매출액": int(rev_amount) if not rev_amount.is_none else None,
                            "영업이익": int(op_amount) if not op_amount.is_none else None,
                            "당기순이익": int(ni_amount) if not ni_amount.is_none else None
                        })

                    if quarter_rows:
                        df = pd.DataFrame(quarter_rows)
                        dataset_name = f"financial_data_{type_label}"
                        self._repository_port.save_partition(dataset_name, corp_code, df)
                
                company.mark_success(year)
                self._repository_port.save_company_metadata(company)
                logger.info(f"  ✅ [{corp_name}] 로컬 XBRL 기반으로 {year}년 재무 정보 적재 완료")
                return True

            logger.warning(f"  ❌ [{corp_name}] XBRL 데이터를 수집하지 못했습니다. API 폴백 없이 다음 실행에서 재시도합니다.")
            company.mark_failure(year)
            self._repository_port.save_company_metadata(company)
            return False

        except Exception as e:
            logger.error(f"  ❌ [{corp_name}] 수집/적재 도중 오류 발생: {e}")
            if 'company' in locals():
                company.mark_failure(year)
                self._repository_port.save_company_metadata(company)
            return False

    def _get_or_create_company(self, corp_code: str, corp_name: str) -> Company:
        """기업 메타데이터를 조회하고, 없으면 결산월을 조회해 신규 생성 후 저장합니다."""
        company = self._repository_port.load_company_metadata(corp_code)
        if not company:
            try:
                settlement_month = self._financial_port.get_settlement_month(corp_code)
            except Exception as e:
                logger.error(f"신규 기업 {corp_name} 결산월 조회 실패: {e}")
                settlement_month = 12
            company = Company(code=corp_code, name=corp_name, settlement_month=settlement_month)
            self._repository_port.save_company_metadata(company)
        return company
