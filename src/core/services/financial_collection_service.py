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

        # 결과 저장용 컨테이너
        # 구조: {시트명: DataFrame}
        # 시트명 예시: "매출액_분기", "영업이익_연간" 등
        all_results: Dict[str, List[Dict]] = {
            "매출액_분기": [], "영업이익_분기": [], "당기순이익_분기": [],
            "매출액_연간": [], "영업이익_연간": [], "당기순이익_연간": []
        }

        total_companies = len(target_companies)
        
        # 2. 기업별 순회
        for idx, (name, code) in enumerate(target_companies, 1):
            logger.info(f"[{idx}/{total_companies}] {name} ({code}) 데이터 수집 시작...")
            
            company_quarterly_data = {}  # 연도별 분기 데이터 임시 저장
            
            # 3. 연도별 순회
            for year in range(start_year, end_year + 1):
                try:
                    # 각 보고서 조회 (1Q, 반기, 3Q, 사업보고서)
                    # API 호출 간 딜레이 추가 (DART 제한 고려)
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
                    
                    # 결과 수집
                    self._append_to_results(all_results, name, year, metrics)
                    
                except Exception as e:
                    logger.error(f"{name} {year}년 데이터 수집 중 오류 발생: {e}")
                    continue

        # 4. DataFrame 변환 및 저장
        final_dfs = {}
        for sheet_name, data_list in all_results.items():
            if data_list:
                df = pd.DataFrame(data_list)
                # 컬럼 순서 조정 (기업명, 연도, 1Q, 2Q, 3Q, 4Q 또는 연간)
                cols = ["기업명", "연도"] + [c for c in df.columns if c not in ["기업명", "연도"]]
                final_dfs[sheet_name] = df[cols]
            else:
                final_dfs[sheet_name] = pd.DataFrame()

        logger.info(f"결과 저장 중: {output_path}")
        self._storage_port.save_excel_with_sheets(final_dfs, output_path)
        logger.info("완료.")

    def _append_to_results(
        self, 
        results: Dict[str, List[Dict]], 
        name: str, 
        year: int, 
        metrics: QuarterlyMetrics
    ) -> None:
        """계산된 지표를 결과 리스트에 추가."""
        
        # 분기별 데이터
        row_base = {"기업명": name, "연도": year}
        
        # 매출액
        row_rev = row_base.copy()
        row_rev.update({
            "1Q": metrics.metrics_by_quarter["1Q"].revenue,
            "2Q": metrics.metrics_by_quarter["2Q"].revenue,
            "3Q": metrics.metrics_by_quarter["3Q"].revenue,
            "4Q": metrics.metrics_by_quarter["4Q"].revenue,
        })
        results["매출액_분기"].append(row_rev)

        # 영업이익
        row_op = row_base.copy()
        row_op.update({
            "1Q": metrics.metrics_by_quarter["1Q"].operating_profit,
            "2Q": metrics.metrics_by_quarter["2Q"].operating_profit,
            "3Q": metrics.metrics_by_quarter["3Q"].operating_profit,
            "4Q": metrics.metrics_by_quarter["4Q"].operating_profit,
        })
        results["영업이익_분기"].append(row_op)

        # 당기순이익
        row_net = row_base.copy()
        row_net.update({
            "1Q": metrics.metrics_by_quarter["1Q"].net_income,
            "2Q": metrics.metrics_by_quarter["2Q"].net_income,
            "3Q": metrics.metrics_by_quarter["3Q"].net_income,
            "4Q": metrics.metrics_by_quarter["4Q"].net_income,
        })
        results["당기순이익_분기"].append(row_net)

        # 연간 데이터 (4Q 누적값이 연간 실적과 동일하다고 가정하거나, 별도 연간 합계 계산)
        # 여기서는 편의상 1Q+2Q+3Q+4Q 합계를 연간으로 사용하거나, 
        # DataProcessingService에서 연간 누적값을 따로 줄 수도 있음.
        # 현재 로직상 metrics.metrics_by_quarter["4Q"]는 4분기 '별도' 실적임.
        # 연간 실적은 1~4Q 합산으로 처리.
        
        def safe_sum(q_dict, key_attr):
            vals = [
                getattr(metrics.metrics_by_quarter[q], key_attr) 
                for q in ["1Q", "2Q", "3Q", "4Q"]
                if getattr(metrics.metrics_by_quarter[q], key_attr) is not None
            ]
            return sum(vals) if vals else None

        # 연간 합계
        results["매출액_연간"].append({**row_base, "값": safe_sum(metrics.metrics_by_quarter, "revenue")})
        results["영업이익_연간"].append({**row_base, "값": safe_sum(metrics.metrics_by_quarter, "operating_profit")})
        results["당기순이익_연간"].append({**row_base, "값": safe_sum(metrics.metrics_by_quarter, "net_income")})
