"""증분 업데이트 서비스."""

import logging
import shutil
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pandas as pd

from core.ports.file_reader_port import FileReaderPort
from core.ports.corp_code_port import CorpCodePort
from core.ports.financial_statement_port import FinancialStatementPort
from core.ports.storage_port import StoragePort
from core.services.data_processing_service import DataProcessingService
from core.domain.models.financial_statement import ReportType

logger = logging.getLogger(__name__)


class IncrementalUpdateService:
    """누락된 분기 데이터를 증분 업데이트하는 서비스."""
    
    # 분기 → ReportType 매핑
    QUARTER_TO_REPORT = {
        1: ReportType.Q1,
        2: ReportType.SEMI_ANNUAL,
        3: ReportType.Q3,
        4: ReportType.ANNUAL,
    }
    
    # 처리 대상 시트 (분기별만)
    QUARTERLY_SHEETS = ["매출액_분기", "영업이익_분기", "당기순이익_분기"]
    
    def __init__(
        self,
        file_reader: FileReaderPort,
        corp_code_port: CorpCodePort,
        financial_port: FinancialStatementPort,
        storage_port: StoragePort,
        processing_service: DataProcessingService,
        max_api_calls: int = 9950
    ):
        self._file_reader = file_reader
        self._corp_code_port = corp_code_port
        self._financial_port = financial_port
        self._storage_port = storage_port
        self._processing_service = processing_service
        
        self._max_api_calls = max_api_calls
        self._current_api_calls = 0

    def update_missing_quarters(
        self,
        file_path: str,
        target_year: int,
        target_quarter: int,
        auto_backup: bool = True
    ) -> None:
        """특정 분기의 누락된 데이터를 업데이트합니다.
        
        Args:
            file_path: 업데이트할 엑셀 파일 경로
            target_year: 대상 연도 (예: 2025)
            target_quarter: 대상 분기 (1, 2, 3, 4)
            auto_backup: 자동 백업 여부
        """
        target_period = f"{target_year}.{target_quarter}Q"
        logger.info(f"🚀 증분 업데이트 시작: {target_period} (파일: {file_path})")
        
        # 1. 파일 백업
        if auto_backup:
            self._backup_file(file_path)
            
        # 2. 기존 파일 읽기
        logger.info("기존 파일 읽는 중...")
        try:
            existing_sheets = self._file_reader.read_excel_with_sheets(file_path)
        except FileNotFoundError:
            logger.error(f"파일을 찾을 수 없습니다: {file_path}")
            return

        # 3. 누락 기업 찾기
        missing_companies = self.find_missing_companies(existing_sheets, target_period)
        if not missing_companies:
            logger.info(f"✨ 누락된 데이터가 없습니다. ({target_period})")
            return
            
        logger.info(f"📋 누락 기업 {len(missing_companies)}개 발견: {missing_companies[:5]}...")
        
        # 4. 데이터 수집 (연도 전체)
        collected_data = []
        processed_count = 0
        
        for idx, company_name in enumerate(missing_companies, 1):
            # API 호출 제한 체크
            if self._current_api_calls >= self._max_api_calls:
                logger.warning(f"⚠️ API 호출 제한 도달! ({self._current_api_calls}/{self._max_api_calls})")
                logger.info("작업을 중단하고 현재까지 수집된 데이터를 저장합니다.")
                break
                
            logger.info(f"[{idx}/{len(missing_companies)}] {company_name} 데이터 수집 중... (API 호출: {self._current_api_calls})")
            
            try:
                # 기업 코드 조회
                corp_code = self._corp_code_port.get_code(company_name)
                if not corp_code:
                    logger.warning(f"  ❌ 기업 코드를 찾을 수 없음: {company_name}")
                    continue
                
                # 해당 연도 전체 데이터 수집
                company_data = self._collect_year_for_company(company_name, corp_code, target_year)
                
                if company_data:
                    collected_data.extend(company_data)
                    processed_count += 1
                    
            except Exception as e:
                logger.error(f"  ❌ {company_name} 수집 실패: {e}")
                continue
        
        if not collected_data:
            logger.warning("수집된 데이터가 없습니다.")
            return

        # 5. 수집된 데이터 변환 (Wide Format)
        logger.info(f"📊 수집된 데이터 변환 중... ({len(collected_data)}개 항목)")
        new_sheets = self._convert_to_wide_format(collected_data)
        
        # 6. 병합 (기존 데이터 우선)
        logger.info("🔄 데이터 병합 중...")
        merged_sheets = self.merge_quarterly_data(existing_sheets, new_sheets)
        
        # 7. 저장
        logger.info(f"💾 결과 저장 중: {file_path}")
        self._storage_port.save_excel_with_sheets(merged_sheets, file_path)
        logger.info(f"✅ 업데이트 완료! (처리된 기업: {processed_count}개, 총 API 호출: {self._current_api_calls}회)")

    def find_missing_companies(
        self,
        sheets: Dict[str, pd.DataFrame],
        target_period: str
    ) -> List[str]:
        """특정 분기가 누락된 기업 목록을 찾습니다."""
        # 매출액_분기 시트 기준
        revenue_sheet = sheets.get("매출액_분기")
        if revenue_sheet is None:
            logger.warning("'매출액_분기' 시트가 없습니다. 모든 기업을 대상으로 간주할 수 없으므로 빈 리스트 반환.")
            return []
        
        missing = []
        
        # 컬럼 자체가 없는 경우: 모든 기업이 누락
        if target_period not in revenue_sheet.columns:
            logger.info(f"'{target_period}' 컬럼이 없습니다. 모든 기업을 수집 대상으로 합니다.")
            return revenue_sheet.index.tolist()
            
        # 컬럼은 있지만 값이 NaN인 경우
        for company in revenue_sheet.index:
            if pd.isna(revenue_sheet.loc[company, target_period]):
                missing.append(company)
        
        return missing

    def _collect_year_for_company(
        self,
        company_name: str,
        corp_code: str,
        year: int
    ) -> List[Dict]:
        """특정 기업의 해당 연도 전체(1Q~4Q) 데이터를 수집합니다."""
        statements = []
        
        # 1Q, 반기, 3Q, 연간 보고서 순차 수집
        for q_num in [1, 2, 3, 4]:
            report_type = self.QUARTER_TO_REPORT[q_num]
            
            # API 호출 (여기서 카운팅은 정확히 하려면 FinancialPort를 래핑하거나
            # Adapter가 호출 여부를 알려줘야 함. 현재는 단순화를 위해 요청 시마다 증가로 가정하되,
            # 실제로는 캐시 히트 시 호출이 안 일어날 수 있음. 
            # 보수적으로 요청 시마다 카운트 증가)
            
            # TODO: 정확한 카운팅을 위해 FinancialPort가 호출 여부를 반환하도록 개선 필요
            # 현재는 요청 시마다 무조건 카운트 증가 (보수적 접근)
            self._current_api_calls += 1
            
            time.sleep(0.1) # 부하 방지
            stmt = self._financial_port.get_financial_statement(corp_code, year, report_type)
            statements.append(stmt)

        # 분기 실적 계산
        # statements 리스트 순서: [1Q, Semi, 3Q, Annual] (None일 수 있음)
        metrics = self._processing_service.calculate_quarterly_performance(
            statements[0], statements[1], statements[2], statements[3]
        )
        
        # Long Format 변환
        data_list = []
        for q in ["1Q", "2Q", "3Q", "4Q"]:
            m = metrics.metrics_by_quarter.get(q)
            if m:
                data_list.append({
                    "기업명": company_name,
                    "연도": year,
                    "분기": q,
                    "매출액": m.revenue,
                    "영업이익": m.operating_profit,
                    "당기순이익": m.net_income
                })
        
        return data_list

    def _convert_to_wide_format(self, data_list: List[Dict]) -> Dict[str, pd.DataFrame]:
        """Long Format 데이터를 Wide Format(시트별)으로 변환합니다."""
        if not data_list:
            return {}
            
        df = pd.DataFrame(data_list)
        df["기간"] = df["연도"].astype(str) + "." + df["분기"]
        
        # 단위 변환 (백만원)
        for col in ["매출액", "영업이익", "당기순이익"]:
            if col in df.columns:
                df[col] = (df[col] / 1_000_000).round(0)
        
        sheets = {}
        if not df.empty:
            sheets["매출액_분기"] = df.pivot(index="기업명", columns="기간", values="매출액")
            sheets["영업이익_분기"] = df.pivot(index="기업명", columns="기간", values="영업이익")
            sheets["당기순이익_분기"] = df.pivot(index="기업명", columns="기간", values="당기순이익")
            
        return sheets

    def merge_quarterly_data(
        self,
        existing_sheets: Dict[str, pd.DataFrame],
        new_sheets: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """기존 데이터와 새 데이터를 병합합니다 (기존 데이터 우선)."""
        merged_sheets = {}
        
        for sheet_name in existing_sheets.keys():
            existing_df = existing_sheets[sheet_name]
            
            # 분기별 시트가 아니면 그대로 유지
            if sheet_name not in self.QUARTERLY_SHEETS:
                merged_sheets[sheet_name] = existing_df
                continue
            
            new_df = new_sheets.get(sheet_name)
            if new_df is None or new_df.empty:
                merged_sheets[sheet_name] = existing_df
                continue
            
            # 기존 데이터 우선 병합: existing의 NaN만 new로 채움
            # combine_first는 호출하는 객체(existing)가 우선임
            merged_df = existing_df.combine_first(new_df)
            
            # 정렬
            merged_df = merged_df.sort_index(axis=0)  # 기업명 정렬
            
            # 컬럼 정렬 (자연 정렬: 2024.1Q, 2024.2Q, ...)
            def sort_key(col):
                try:
                    # 컬럼명이 문자열이 아니거나 형식이 다를 수 있음
                    col_str = str(col)
                    if '.' in col_str:
                        year, quarter = col_str.split('.')
                        quarter_num = int(quarter[0])  # "1Q" -> 1
                        return (int(year), quarter_num)
                    return (0, 0)
                except:
                    return (0, 0)
            
            sorted_cols = sorted(merged_df.columns, key=sort_key)
            merged_df = merged_df[sorted_cols]
            
            merged_sheets[sheet_name] = merged_df
        
        return merged_sheets

    def _backup_file(self, file_path: str) -> str:
        """파일을 백업합니다."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = file_path.replace(".xlsx", f"_backup_{timestamp}.xlsx")
        try:
            shutil.copy2(file_path, backup_path)
            logger.info(f"📦 백업 완료: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"백업 실패: {e}")
            return ""
