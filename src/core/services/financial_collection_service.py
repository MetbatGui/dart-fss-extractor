"""재무 데이터 수집 총괄 서비스."""

import time
import logging
from typing import List, Optional, Dict
from datetime import datetime
import pandas as pd

from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.storage_port import StoragePort
from core.domain.models.financial_statement import ReportType, FinancialStatementType
from core.services.data_processing_service import DataProcessingService
from core.domain.models.performance_metrics import QuarterlyMetrics

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
        storage_port: StoragePort,
        processing_service: DataProcessingService
    ):
        self._corp_code_port = corp_code_port
        self._financial_port = financial_port
        self._storage_port = storage_port
        self._processing_service = processing_service

    def collect_and_save(
        self,
        company_names: List[str],
        start_year: int,
        end_year: int,
        output_path: str
    ) -> None:
        """기업 리스트와 기간에 대해 데이터를 수집하고 저장합니다.
        
        Args:
            company_names: 수집 대상 기업명 리스트
            start_year: 시작 연도 (예: 2021)
            end_year: 종료 연도 (예: 2023)
            output_path: 저장할 엑셀 파일 경로
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

        # 데이터 수집 (Long Format으로 임시 저장)
        # 구조: [{"기업명": "A", "연도": 2023, "분기": "1Q", "매출액": 100, ...}, ...]
        collected_data = []

        total_companies = len(target_companies)
        
        # 2. 기업별 순회
        for idx, (name, code) in enumerate(target_companies, 1):
            logger.info(f"[{idx}/{total_companies}] {name} ({code}) 데이터 수집 시작...")
            
            # 3. 연도별 순회
            for year in range(start_year, end_year + 1):
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
                    self._append_to_list(collected_data, name, year, metrics)
                    
                except Exception as e:
                    logger.error(f"{name} {year}년 데이터 수집 중 오류 발생: {e}")
                    continue

        # 4. DataFrame 변환 및 Pivot (Wide Format)
        if not collected_data:
            logger.warning("수집된 데이터가 없습니다.")
            return

        df_base = pd.DataFrame(collected_data)
        
        # 결과 저장용 딕셔너리
        final_dfs = {}
        
        # (1) 분기 데이터 처리
        # 필터링: 분기 데이터만 (1Q, 2Q, 3Q, 4Q)
        df_quarter = df_base[df_base["구분"] == "분기"].copy()
        if not df_quarter.empty:
            # 컬럼 생성: "2023.1Q"
            df_quarter["기간"] = df_quarter["연도"].astype(int).astype(str) + "." + df_quarter["분기"].astype(str)
            
            # Pivot: 행=기업명, 열=기간, 값=매출액/영업이익/당기순이익
            final_dfs["매출액_분기"] = df_quarter.pivot(index="기업명", columns="기간", values="매출액")
            final_dfs["영업이익_분기"] = df_quarter.pivot(index="기업명", columns="기간", values="영업이익")
            final_dfs["당기순이익_분기"] = df_quarter.pivot(index="기업명", columns="기간", values="당기순이익")

        # (2) 연간 데이터 처리
        # 필터링: 연간 데이터만
        df_annual = df_base[df_base["구분"] == "연간"].copy()
        if not df_annual.empty:
            # Pivot: 행=기업명, 열=연도, 값=매출액/영업이익/당기순이익
            final_dfs["매출액_연간"] = df_annual.pivot(index="기업명", columns="연도", values="매출액")
            final_dfs["영업이익_연간"] = df_annual.pivot(index="기업명", columns="연도", values="영업이익")
            final_dfs["당기순이익_연간"] = df_annual.pivot(index="기업명", columns="연도", values="당기순이익")

        logger.info(f"결과 저장 중: {output_path}")
        self._storage_port.save_excel_with_sheets(final_dfs, output_path)
        logger.info("완료.")

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

        # 2. 연간 데이터 추가 (단순 합산)
        # metrics.metrics_by_quarter["4Q"]는 4분기 '별도' 실적이므로,
        # 연간 실적은 1~4Q 합산으로 처리하거나, 별도 로직이 있다면 적용.
        # 여기서는 1~4Q 합산으로 처리.
        
        total_revenue = 0
        total_op = 0
        total_net = 0
        has_data = False

        for q in ["1Q", "2Q", "3Q", "4Q"]:
            m = metrics.metrics_by_quarter.get(q)
            if m:
                if m.revenue is not None: total_revenue += m.revenue
                if m.operating_profit is not None: total_op += m.operating_profit
                if m.net_income is not None: total_net += m.net_income
                has_data = True
        
        if has_data:
            data_list.append({
                "기업명": name,
                "연도": year,
                "구분": "연간",
                "분기": "연간", # Pivot시 사용 안함
                "매출액": total_revenue,
                "영업이익": total_op,
                "당기순이익": total_net
            })
