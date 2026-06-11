"""데일리 배치 및 자동 스케줄링용 구동 스크립트."""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from typing import List

from core.services.daily_collection_service import DailyCollectionService
from core.services.data_processing_service import DataProcessingService
from infra.adapters.corp_code_adapter import CorpCodeAdapter
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from infra.adapters.sqlite.sqlite_repository_adapter import SqliteRepositoryAdapter
from infra.adapters.excel_export_adapter import ExcelExportAdapter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DailyScheduler")


def parse_arguments():
    parser = argparse.ArgumentParser(description='DART 데일리 자동 증분 수집 및 엑셀 갱신 스크립트')
    parser.add_argument('--bgn-de', type=str, help='검색 시작일자 (YYYYMMDD, 기본값: 어제)')
    parser.add_argument('--end-de', type=str, help='검색 종료일자 (YYYYMMDD, 기본값: 오늘)')
    parser.add_argument('--companies', type=str, default="data/target_companies.csv", help='대상 기업 목록 파일 경로')
    parser.add_argument('--output', type=str, default="output/financial_data_result.xlsx", help='최종 피벗 엑셀 저장 경로')
    return parser.parse_args()


def load_target_companies(file_path: Path) -> List[str]:
    """대상 기업 목록 파일에서 유효 기업명들을 로드합니다."""
    if not file_path.exists():
        logger.error(f"대상 기업 목록 파일이 존재하지 않습니다: {file_path}")
        return []
    try:
        if file_path.suffix == '.csv':
            df = pd.read_csv(file_path)
        elif file_path.suffix == '.xlsx':
            df = pd.read_excel(file_path)
        else:
            logger.error(f"지원하지 않는 파일 형식: {file_path.suffix}")
            return []

        # 지능적인 컬럼 매핑 시도
        target_col = None
        for col in df.columns:
            col_str = str(col).strip()
            if col_str in ["기업명", "종목명", "회사명", "corp_name"]:
                target_col = col
                break
        
        if target_col is not None:
            return df[target_col].dropna().astype(str).str.strip().unique().tolist()
        
        # 첫 번째 열 사용 (Fallback)
        return df.iloc[:, 0].dropna().astype(str).str.strip().unique().tolist()
    except Exception as e:
        logger.error(f"기업 목록 파일 로드 중 실패: {e}")
        return []


def main():
    load_dotenv()
    
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        sys.exit(1)

    args = parse_arguments()

    # 날짜 범위 설정 (인자가 없을 시 어제~오늘로 자동 산정)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    bgn_de = args.bgn_de or yesterday.strftime("%Y%m%d")
    end_de = args.end_de or today.strftime("%Y%m%d")

    logger.info(f"🚀 데일리 수집 구동 시작 (스캔 범위: {bgn_de} ~ {end_de})")

    # 대상 기업 목록 로드
    companies_path = Path(args.companies)
    company_names = load_target_companies(companies_path)
    if not company_names:
        logger.error("수집할 대상 기업이 없습니다. 스케줄러를 종료합니다.")
        sys.exit(1)

    logger.info(f"로드된 수집 대상 기업: {len(company_names)}개")

    # 어댑터 및 서비스 초기화
    corp_code_adapter = CorpCodeAdapter()
    financial_adapter = DartFinancialAdapter(api_key=api_key, use_cache=True)
    repository_adapter = SqliteRepositoryAdapter()
    export_adapter = ExcelExportAdapter()
    processing_service = DataProcessingService()

    daily_service = DailyCollectionService(
        corp_code_port=corp_code_adapter,
        financial_port=financial_adapter,
        repository_port=repository_adapter,
        processing_service=processing_service
    )

    # 1. 당일 공시 스캔 및 증분 적재 수행 (SQLite 트랜잭션 수호)
    daily_service.collect_daily_disclosures(
        target_company_names=company_names,
        start_date=bgn_de,
        end_date=end_de
    )

    # 2. SQLite 데이터베이스에 최종 갱신 성공 데이터가 1건이라도 있는 경우, 엑셀 갱신 단방향 트리거
    # (안정성을 위해 매일 갱신된 최신 DB 스냅샷을 엑셀로 원자적으로 덮어쓰기)
    try:
        logger.info("💾 SQLite 최신 영속성 스냅샷 기반으로 최종 엑셀을 단방향 갱신(Export)합니다.")
        all_df = repository_adapter.load_all("financial_data_cfs")
        
        if not all_df.empty:
            final_dfs = {}
            DIVISOR = 1_000_000 # 백만원 단위 조정
            
            # 1Q~4Q 분기별 피벗
            df_quarter = all_df[all_df["구분"] == "분기"].copy()
            if not df_quarter.empty:
                df_quarter["기간"] = df_quarter["연도"].astype(int).astype(str) + "." + df_quarter["분기"].astype(str)
                df_quarter = df_quarter.drop_duplicates(subset=["기업명", "기간"])
                
                final_dfs["매출액_분기별"] = df_quarter.pivot(index="기업명", columns="기간", values="매출액")
                final_dfs["영업이익_분기별"] = df_quarter.pivot(index="기업명", columns="기간", values="영업이익")
                final_dfs["당기순이익_분기별"] = df_quarter.pivot(index="기업명", columns="기간", values="당기순이익")
            
            # 연간 피벗
            df_annual = all_df[all_df["구분"] == "연간"].copy()
            if not df_annual.empty:
                df_annual = df_annual.drop_duplicates(subset=["기업명", "연도"])
                
                final_dfs["매출액_연간"] = df_annual.pivot(index="기업명", columns="연도", values="매출액")
                final_dfs["영업이익_연간"] = df_annual.pivot(index="기업명", columns="연도", values="영업이익")
                final_dfs["당기순이익_연간"] = df_annual.pivot(index="기업명", columns="연도", values="당기순이익")

            # 금액 단위 조정 (백만원)
            for sheet_name, sheet_df in final_dfs.items():
                final_dfs[sheet_name] = (sheet_df.apply(pd.to_numeric, errors='coerce') / DIVISOR).round(0)

            # 최종 엑셀 쓰기
            output_file = Path(args.output)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            export_adapter.export_excel(final_dfs, str(output_file))
            logger.info(f"✨ 통합 결과 엑셀이 안전하게 동기화되었습니다: {args.output}")

            # 구글 드라이브 자동 업로드 기믹
            drive_folder_id = os.getenv("GOOGLE_DRIVE_FINANCIAL_STATEMENTS_ID")
            if drive_folder_id:
                try:
                    logger.info("☁️ 구글 드라이브 업로드를 시작합니다...")
                    from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter
                    
                    token_path = "secrets/token.json"
                    client_secret_path = "secrets/client_secret.json"
                    
                    if not os.path.exists(token_path):
                        logger.warning(f"구글 드라이브 토큰 파일이 존재하지 않아 업로드를 생략합니다: {token_path}")
                    else:
                        drive_adapter = GoogleDriveAdapter(
                            token_file=token_path,
                            root_folder_id=drive_folder_id,
                            client_secret_file=client_secret_path if os.path.exists(client_secret_path) else None
                        )
                        
                        if output_file.exists():
                            file_data = output_file.read_bytes()
                            success = drive_adapter.put_file("재무제표.xlsx", file_data)
                            if success:
                                logger.info("✨ 구글 드라이브 업로드 완료 (재무제표.xlsx)")
                            else:
                                logger.error("❌ 구글 드라이브 업로드 실패")
                        else:
                            logger.warning(f"업로드할 로컬 엑셀 파일이 존재하지 않습니다: {output_file}")
                except Exception as drive_err:
                    logger.error(f"구글 드라이브 업로드 연동 중 오류 발생: {drive_err}", exc_info=True)
        else:
            logger.warning("SQLite DB에 실적 데이터가 전혀 존재하지 않아 엑셀 동기화를 보류합니다.")
            
    except Exception as e:
        logger.error(f"최종 엑셀 동기화 내보내기 실패: {e}")
        repository_adapter.close()
        sys.exit(1)

    repository_adapter.close()
    logger.info("🏁 데일리 배치 스케줄러 모든 수집/동기화 작업을 정상 마쳤습니다.")


if __name__ == "__main__":
    main()
