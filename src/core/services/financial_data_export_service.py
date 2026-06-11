"""통합 재무 데이터 엑셀 내보내기 비즈니스 서비스."""

import os
import logging
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd

from core.ports.repository_port import RepositoryPort
from core.ports.export_port import ExportPort
from core.services.data_processing_service import DataProcessingService

logger = logging.getLogger(__name__)


class FinancialDataExportService:
    """연결/개별 재무 데이터를 병합하고 피벗/가공하여 최종 엑셀을 생성하는 조율 서비스."""

    def __init__(
        self,
        repository_port: RepositoryPort,
        export_port: ExportPort,
        processing_service: DataProcessingService
    ):
        self._repository_port = repository_port
        self._export_port = export_port
        self._processing_service = processing_service

    def export_integrated_financial_data(self, output_path: str) -> bool:
        """연결 우선 / 개별 보완 및 로우 백업 통합 방식으로 엑셀 파일을 최종 동기화합니다.

        Args:
            output_path: 저장할 최종 엑셀 파일 경로.

        Returns:
            성공 여부.
        """
        logger.info("연결(CFS) 우선 / 개별(OFS) 대체 전략으로 영속성 저장소 데이터 병합을 시작합니다...")

        # 1. 데이터 로드 (각 데이터셋 조회)
        cfs_df = self._repository_port.load_all("financial_data_cfs")
        ofs_df = self._repository_port.load_all("financial_data_ofs")
        raw_df = self._repository_port.load_all("financial_data_raw")

        # 2. 데이터 병합 가공
        cfs_df = cfs_df if (cfs_df is not None and not cfs_df.empty) else pd.DataFrame()
        ofs_df = ofs_df if (ofs_df is not None and not ofs_df.empty) else pd.DataFrame()
        raw_df = raw_df if (raw_df is not None and not raw_df.empty) else pd.DataFrame()

        cfs_codes = set(cfs_df["종목코드"].unique()) if "종목코드" in cfs_df.columns else set()
        ofs_codes = set(ofs_df["종목코드"].unique()) if "종목코드" in ofs_df.columns else set()
        v2_codes = cfs_codes | ofs_codes

        logger.info(f"수집 현황: 연결(CFS)={len(cfs_codes)}개 기업, 개별(OFS)={len(ofs_codes)}개 기업 (총 {len(v2_codes)}개)")

        all_dfs = []

        # 2-1. V2 연결 우선 / 개별 대체 병합
        for code in v2_codes:
            if code in cfs_codes:
                sub_df = cfs_df[cfs_df["종목코드"] == code].copy()
                sub_df["데이터출처"] = "연결(CFS)"
                all_dfs.append(sub_df)
            elif code in ofs_codes:
                sub_df = ofs_df[ofs_df["종목코드"] == code].copy()
                sub_df["데이터출처"] = "개별(OFS)"
                all_dfs.append(sub_df)

        # 2-2. 기존 Raw 캐시 중 V2에 없는 데이터 추가 보완
        if "종목코드" in raw_df.columns and not raw_df.empty:
            raw_codes = set(raw_df["종목코드"].unique())
            additional_codes = raw_codes - v2_codes
            logger.info(f"Raw 백업 통합: 총 {len(raw_codes)}개 기업 중 미포함된 {len(additional_codes)}개 기업의 캐시 데이터를 추가 통합합니다.")
            
            for code in additional_codes:
                sub_df = raw_df[raw_df["종목코드"] == code].copy()
                
                # 컬럼명이 다른 경우 표준화 가드
                if sub_df.shape[1] == 7:
                    sub_df.columns = ["기업명", "연도", "구분", "분기", "매출액", "영업이익", "당기순이익"]
                    sub_df["구분_상세"] = "연결"
                
                sub_df["종목코드"] = code
                sub_df["데이터출처"] = "기존수집(RAW)"
                all_dfs.append(sub_df)

        if not all_dfs:
            logger.error("병합할 재무 데이터가 존재하지 않습니다.")
            return False

        df_base = pd.concat(all_dfs, ignore_index=True)

        # 3. 비정상 캐시 오염 기업 필터 가드
        df_temp = df_base.copy()
        df_temp["매출액_abs"] = pd.to_numeric(df_temp["매출액"], errors="coerce").abs().fillna(0)
        df_temp["영업이익_abs"] = pd.to_numeric(df_temp["영업이익"], errors="coerce").abs().fillna(0)
        df_temp["당기순이익_abs"] = pd.to_numeric(df_temp["당기순이익"], errors="coerce").abs().fillna(0)
        df_temp["실적합"] = df_temp["매출액_abs"] + df_temp["영업이익_abs"] + df_temp["당기순이익_abs"]

        zero_performance_corps = df_temp.groupby("종목코드")["실적합"].sum()
        invalid_codes = zero_performance_corps[zero_performance_corps == 0].index.tolist()

        if invalid_codes:
            for code in invalid_codes:
                corps_with_code = df_base[df_base["종목코드"] == code]
                if not corps_with_code.empty:
                    corp_name = corps_with_code["기업명"].iloc[0]
                    logger.warning(f"[FILTER GUARD] 비정상 캐시 오염 기업 제거 완료: {corp_name} ({code}) - 모든 실적 데이터가 0.0 또는 결측치입니다.")
            df_base = df_base[~df_base["종목코드"].isin(invalid_codes)]

        # 4. 기업명 복원 가드 (한글명이 숫자로 깨져 있는 경우)
        needs_restoration = df_base["기업명"].astype(str).str.strip().str.match(r'^\d+$').any()
        if needs_restoration:
            logger.info("[GUARDIAN] 오염된 숫자형 기업명이 감지되었습니다. 2중 Fallback 매핑 사전을 구축하여 복원을 진행합니다...")
            mapping_dict = self._load_corp_code_mappings()

            def restore_name(row):
                name_val = str(row["기업명"]).strip()
                code_val = str(row["종목코드"]).strip()
                if name_val in mapping_dict:
                    return mapping_dict[name_val]
                if name_val.isdigit() or name_val == code_val:
                    if code_val in mapping_dict:
                        return mapping_dict[code_val]
                return row["기업명"]

            df_base["기업명"] = df_base.apply(restore_name, axis=1)
            logger.info("[GUARDIAN] 오염 기업명 한글 복원 처리를 완벽하게 완료했습니다.")
        else:
            logger.info("[GUARDIAN] 검사 결과, 원본 기업명이 정상적인 한글 형태로 확인되어 고속 진행합니다.")

        # 5. 데이터 피벗 가공 및 스케일링
        final_dfs = {}

        # 5-1. 분기 데이터 피벗
        df_quarter = df_base[df_base["구분"] == "분기"].copy()
        if not df_quarter.empty:
            if "기간" not in df_quarter.columns:
                df_quarter["기간"] = df_quarter["연도"].astype(int).astype(str) + "." + df_quarter["분기"].astype(str)
            df_quarter = df_quarter.drop_duplicates(subset=["종목코드", "기간"])
            
            final_dfs["매출액_분기"] = df_quarter.pivot(index=["기업명", "종목코드"], columns="기간", values="매출액")
            final_dfs["영업이익_분기"] = df_quarter.pivot(index=["기업명", "종목코드"], columns="기간", values="영업이익")
            final_dfs["당기순이익_분기"] = df_quarter.pivot(index=["기업명", "종목코드"], columns="기간", values="당기순이익")

        # 5-2. 연간 데이터 피벗
        df_annual = df_base[df_base["구분"] == "연간"].copy()
        if not df_annual.empty:
            df_annual = df_annual.drop_duplicates(subset=["종목코드", "연도"])
            
            final_dfs["매출액_연간"] = df_annual.pivot(index=["기업명", "종목코드"], columns="연도", values="매출액")
            final_dfs["영업이익_연간"] = df_annual.pivot(index=["기업명", "종목코드"], columns="연도", values="영업이익")
            final_dfs["당기순이익_연간"] = df_annual.pivot(index=["기업명", "종목코드"], columns="연도", values="당기순이익")

        # 5-3. 금액 단위 조정 (원 -> 백만 원) 및 반올림
        DIVISOR = 1_000_000
        for sheet_name, sheet_df in final_dfs.items():
            if sheet_df is not None and not sheet_df.empty:
                final_dfs[sheet_name] = (sheet_df.apply(pd.to_numeric, errors='coerce') / DIVISOR).round(0)

        # 6. 최종 엑셀 내보내기
        logger.info(f"통합 결과 엑셀 파일 생성 중: {output_path} (단위: 백만 원)")
        try:
            self._export_port.export_excel(final_dfs, output_path)
            logger.info(f"[SUCCESS] 통합 엑셀 저장 완료: {output_path}")
            return True
        except Exception as e:
            logger.error(f"통합 엑셀 저장 실패 ({output_path}): {e}")
            return False

    def _load_corp_code_mappings(self) -> Dict[str, str]:
        """CORPCODE.xml 및 corps.csv로부터 고유번호 -> 한글 기업명 매핑 테이블을 구축합니다."""
        mappings = {}
        
        # 1. corps.csv 로드
        corps_csv = Path("data/corps.csv")
        if corps_csv.exists():
            try:
                df_csv = pd.read_csv(corps_csv, header=None, names=["name", "code"], dtype=str)
                for _, row in df_csv.iterrows():
                    mappings[row["code"]] = row["name"]
            except Exception as e:
                logger.error(f"corps.csv 로드 실패: {e}")
                
        # 2. CORPCODE.xml 로드 및 보강
        xml_path = Path("data/corp_code/CORPCODE.xml")
        if xml_path.exists():
            try:
                import xml.etree.ElementTree as ET
                tree = ET.parse(xml_path)
                root = tree.getroot()
                for list_node in root.findall("list"):
                    corp_code = list_node.find("corp_code").text
                    corp_name = list_node.find("corp_name").text
                    if corp_code and corp_name:
                        mappings[corp_code] = corp_name
            except Exception as e:
                logger.error(f"CORPCODE.xml 파싱 중 오류 발생: {e}")
                
        return mappings
