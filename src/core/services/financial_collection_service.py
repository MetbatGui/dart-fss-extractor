"""재무 데이터 수집 총괄 서비스."""

import time
import logging
from typing import List, Dict
import pandas as pd

from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.repository_port import RepositoryPort
from core.ports.export_port import ExportPort
from core.domain.models.financial_statement import ReportType
from core.services.data_processing_service import DataProcessingService
from core.domain.models.performance_metrics import QuarterlyMetrics
from core.domain.models.company import Company

logger = logging.getLogger(__name__)


class FinancialCollectionService:
    """재무 데이터 수집 및 저장을 총괄하는 서비스.
    
    - 기업명 리스트를 받아 기업코드로 변환
    - 지정된 기간(연도) 동안 반복하며 재무제표 수집
    - 수집된 데이터를 가공하여 분기별 실적 산출
    - 최종 결과를 엑셀 파일로 저장
    """

    def __init__(
        self,
        corp_code_port: CorpCodePort,
        financial_port: FinancialStatementPort,
        repository_port: RepositoryPort,
        export_port: ExportPort,
        processing_service: DataProcessingService
    ):
        self._corp_code_port = corp_code_port
        self._financial_port = financial_port
        self._repository_port = repository_port
        self._export_port = export_port
        self._processing_service = processing_service

    def collect_and_save(
        self,
        company_names: List[str],
        start_year: int,
        end_year: int,
        output_path: str,
        skip_failed: bool = True,
        force_recollect: bool = False
    ) -> None:
        """기업 리스트와 기간에 대해 데이터를 수집하고 저장합니다.
        
        Args:
            company_names: 수집 대상 기업명 리스트
            start_year: 시작 연도 (예: 2021)
            end_year: 종료 연도 (예: 2023)
            output_path: 저장할 엑셀 파일 경로
            skip_failed: 이미 실패 이력이 있는 연도를 건너뛸지 여부 (기본값 True)
            force_recollect: 이미 성공한 연도도 다시 수집할지 여부 (기본값 False)
        """
        # 1. 기업 코드 조회
        logger.info("기업 코드 조회 중...")
        codes = self._corp_code_port.get_codes(company_names)
        
        # 기업명 -> 코드 매핑 (유효한 것만)
        target_companies = []
        for name, code in zip(company_names, codes):
            if code:
                target_companies.append((name, code))
            else:
                logger.warning(f"기업 코드를 찾을 수 없음: {name}")

        dataset_name = "financial_data_raw"
        total_companies = len(target_companies)
        
        logger.info(f"총 {total_companies}개 기업에 대한 수집 작업을 시작합니다. (이어하기 가능, 실패스킵: {skip_failed})")

        # 2. 기업별 순회 (개별 저장)
        for idx, (name, code) in enumerate(target_companies, 1):
            
            # Company 객체 로드 또는 생성
            company = self._repository_port.load_company_metadata(code)
            if not company:
                company = Company(code=code, name=name)

            # 2-1. 저장소 데이터와 메타데이터 동기화 (Sync)
            # 메타데이터에는 없지만 실제 파티션 파일에는 데이터가 있을 수 있음
            if self._repository_port.exists(dataset_name, code):
                existing_df = self._repository_port.load_partition(dataset_name, code)
                if not existing_df.empty and "연도" in existing_df.columns:
                    repo_years = existing_df["연도"].unique().tolist()
                    for y in repo_years:
                        if y not in company.success_years:
                            company.mark_success(int(y))
                            logger.info(f"[{name}] 저장소에서 {y}년 데이터 발견 -> 메타데이터 동기화.")

            # 2-2. 스마트 건너뛰기: 요청한 모든 연도가 이미 '성공'했거나 '실패(스킵시)'했는지 확인
            target_years = set(range(start_year, end_year + 1))
            finished_years = set()
            if not force_recollect:
                finished_years.update(company.success_years)
            if skip_failed:
                finished_years.update(company.failed_years)
            
            # 요청한 연도가 이미 완료된 연도의 부분집합이면 -> 건너뜀
            if not force_recollect and target_years.issubset(finished_years):
                 logger.info(f"[{idx}/{total_companies}] {name} ({code}) - 요청한 기간({start_year}~{end_year}) 데이터가 이미 수집되었거나 실패 기록이 있습니다. 건너뜁니다.")
                 continue
            
            # 실패 이력 로그
            failed_in_range = [y for y in company.failed_years if y in target_years]
            if failed_in_range and not skip_failed:
                logger.info(f"[{idx}/{total_companies}] {name} ({code}) - 실패 이력({failed_in_range}) 재시도 및 누락 데이터 수집 시작...")
            else:
                logger.info(f"[{idx}/{total_companies}] {name} ({code}) - 데이터 수집 시작 (기간: {start_year}~{end_year})...")
            
            company_data = []

            # 3. 연도별 순회
            for year in range(start_year, end_year + 1):
                # 이미 성공한 연도라면 건너뛰기 (강제 재수집이 아닐 때만)
                if not force_recollect and year in company.success_years:
                    continue
                
                # 실패한 연도이고, 스킵 옵션이 켜져 있으면 건너뛰기
                if skip_failed and year in company.failed_years:
                    continue

                try:
                    # 각 보고서 조회
                    time.sleep(0.1) 
                    q1 = self._financial_port.get_financial_statement(code, year, ReportType.Q1)
                    
                    time.sleep(0.1)
                    semi = self._financial_port.get_financial_statement(code, year, ReportType.SEMI_ANNUAL)
                    
                    time.sleep(0.1)
                    q3 = self._financial_port.get_financial_statement(code, year, ReportType.Q3)
                    
                    time.sleep(0.1)
                    annual = self._financial_port.get_financial_statement(code, year, ReportType.ANNUAL)

                    # 분기 실적 계산
                    metrics = self._processing_service.calculate_quarterly_performance(q1, semi, q3, annual)
                    
                    # 데이터 리스트에 추가
                    self._append_to_list(company_data, name, year, metrics)
                    
                    # 성공 기록
                    company.mark_success(year)
                    
                except Exception as e:
                    logger.error(f"{name} {year}년 데이터 수집 중 오류 발생: {e}")
                    company.mark_failure(year)
                    continue
            
            # 4. 개별 기업 데이터 저장 (Partition) - Merge Logic
            if company_data:
                new_df = pd.DataFrame(company_data)
                
                # 기존 파티션 로드 (Merge)
                if self._repository_port.exists(dataset_name, code):
                    try:
                        existing_df = self._repository_port.load_partition(dataset_name, code)
                        if not existing_df.empty:
                            # 기존 데이터 + 새 데이터 병합
                            merged_df = pd.concat([existing_df, new_df])
                            # 중복 제거 (기업명, 연도, 분기, 구분 기준) - 최신 데이터를 남기려면 drop_duplicates의 keep 전략 확인 필요
                            # 여기서는 단순히 중복된 '키'가 있으면 나중에 추가된 것(새 데이터)을 유지하거나,
                            # 기존 데이터를 유지하거나 정책 결정 필요.
                            # 보통 재수집은 '갱신' 목적이므로, 새 데이터를 우선할 수 있으나,
                            # 단순 concat 후 drop_duplicates는 모든 컬럼이 같아야 지워짐.
                            # 키 기준으로 중복 제거:
                            merged_df = merged_df.drop_duplicates(subset=["기업명", "연도", "분기", "구분"], keep="last")
                            
                            # 다시 sort
                            merged_df = merged_df.sort_values(by=["연도", "분기"])
                            
                            self._repository_port.save_partition(dataset_name, code, merged_df)
                            logger.info(f"[{idx}/{total_companies}] {name} 기존 데이터와 병합하여 저장 완료.")
                        else:
                             self._repository_port.save_partition(dataset_name, code, new_df)
                             logger.info(f"[{idx}/{total_companies}] {name} 저장 완료.")
                    except Exception as e:
                        logger.error(f"[{idx}/{total_companies}] {name} 병합 저장 중 오류: {e}")
                        # 병합 실패 시 새 데이터라도 저장 시도? 아니면 보존?
                        # 안전을 위해 에러 로그만 남기고 기존 데이터 보존
                        pass
                else:
                    self._repository_port.save_partition(dataset_name, code, new_df)
                    logger.info(f"[{idx}/{total_companies}] {name} 신규 저장 완료.")
                
            # 메타데이터 저장 (수집 결과 업데이트)
            self._repository_port.save_company_metadata(company)
            
            if company.failed_years:
                logger.warning(f"[{idx}/{total_companies}] {name} 일부 실패: {company.failed_years}")

        # 5. 수집 완료 후 전체 데이터 통합하여 엑셀 내보내기
        try:
            logger.info("모든 기업 수집 완료. 통합 엑셀 파일 생성 중...")
            all_df = self._repository_port.load_all(dataset_name)
            if not all_df.empty:
                final_dfs = {}
                DIVISOR = 1_000_000  # 백만 원 단위
                
                # 분기 데이터 피벗
                df_quarter = all_df[all_df["구분"] == "분기"].copy()
                if not df_quarter.empty:
                    # '기간' 컬럼이 없는 기존 수집 데이터 호환 가드
                    if "기간" not in df_quarter.columns:
                        df_quarter["기간"] = df_quarter["연도"].astype(int).astype(str) + "." + df_quarter["분기"].astype(str)
                    df_quarter = df_quarter.drop_duplicates(subset=["기업명", "기간"])
                    final_dfs["매출액_분기"] = df_quarter.pivot(index=["기업명"], columns="기간", values="매출액")
                    final_dfs["영업이익_분기"] = df_quarter.pivot(index=["기업명"], columns="기간", values="영업이익")
                    final_dfs["당기순이익_분기"] = df_quarter.pivot(index=["기업명"], columns="기간", values="당기순이익")
                
                # 연간 데이터 피벗
                df_annual = all_df[all_df["구분"] == "연간"].copy()
                if not df_annual.empty:
                    df_annual = df_annual.drop_duplicates(subset=["기업명", "연도"])
                    final_dfs["매출액_연간"] = df_annual.pivot(index=["기업명"], columns="연도", values="매출액")
                    final_dfs["영업이익_연간"] = df_annual.pivot(index=["기업명"], columns="연도", values="영업이익")
                    final_dfs["당기순이익_연간"] = df_annual.pivot(index=["기업명"], columns="연도", values="당기순이익")
                
                # 단위 변환
                for sheet_name, sheet_df in final_dfs.items():
                    final_dfs[sheet_name] = (sheet_df.apply(pd.to_numeric, errors='coerce') / DIVISOR).round(0)
                
                self._export_port.export_excel(final_dfs, output_path)
                logger.info(f"성공적으로 통합 엑셀 파일을 저장했습니다: {output_path}")
            else:
                logger.warning("저장소에 수집된 데이터가 없어 엑셀 생성을 건너뜁니다.")
        except Exception as e:
            logger.error(f"통합 엑셀 파일 생성 중 오류 발생: {e}")

    def _append_to_list(
        self, 
        data_list: List[Dict], 
        name: str, 
        year: int, 
        metrics: QuarterlyMetrics
    ) -> None:
        """계산된 지표를 리스트에 추가 (Long Format)."""
        
        # 1. 분기 데이터 추가
        for q in ["1Q", "2Q", "3Q", "4Q"]:
            m = metrics.metrics_by_quarter.get(q)
            if m:
                data_list.append({
                    "기업명": name,
                    "연도": year,
                    "구분": "분기",
                    "분기": q,
                    "매출액": m.revenue,
                    "영업이익": m.operating_profit,
                    "당기순이익": m.net_income
                })

        # 2. 연간 데이터 추가
        # QuarterlyMetrics.annual_metrics가 있으면 이를 사용하고, 없으면 분기 합산으로 처리.
        
        if metrics.annual_metrics and metrics.annual_metrics.revenue is not None:
            # 원본 연간 데이터 사용
            data_list.append({
                "기업명": name,
                "연도": year,
                "구분": "연간",
                "분기": "연간",
                "매출액": metrics.annual_metrics.revenue,
                "영업이익": metrics.annual_metrics.operating_profit,
                "당기순이익": metrics.annual_metrics.net_income
            })
        else:
            # 백업: 1~4Q 합산으로 처리
            annual = self._processing_service.calculate_annual_from_quarters(metrics.metrics_by_quarter)
            if annual.revenue is not None or annual.operating_profit is not None or annual.net_income is not None:
                data_list.append({
                    "기업명": name,
                    "연도": year,
                    "구분": "연간",
                    "분기": "연간",  # Pivot시 사용 안함
                    "매출액": annual.revenue,
                    "영업이익": annual.operating_profit,
                    "당기순이익": annual.net_income
                })
