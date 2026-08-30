"""데일리 배치 및 자동 스케줄링용 구동 스크립트.

DB SSOT는 Google Drive다 (db_ssot_guide.md §1, §6). 이 스크립트는 실행만
담당하고, 전체 흐름(DB 다운로드→수집→export→업로드)은
DailyRoutineService(orchestration_guide.md §1)가 소유한다.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from core.services.config_sync import (
    ARTIFACT_REMOTE_PATH,
    CONFIG_REMOTE_TO_LOCAL,
    DB_REMOTE_PATH,
    download_config_files,
)
from core.services.daily_collection_service import DailyCollectionService
from core.services.daily_routine_service import DailyRoutineService
from core.services.data_processing_service import DataProcessingService
from infra.adapters.corp_code_adapter import CorpCodeAdapter
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from infra.adapters.dart_xbrl_financial_adapter import DartXbrlFinancialAdapter
from infra.adapters.excel_export_adapter import ExcelExportAdapter
from infra.adapters.naver_ticker_name_adapter import NaverTickerNameAdapter
from infra.adapters.sqlite.sqlite_repository_adapter import SqliteRepositoryAdapter
from infra.adapters.sqlite.sqlite_ticker_name_cache_adapter import (
    SqliteTickerNameCacheAdapter,
)
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("DailyScheduler")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="DART 데일리 자동 증분 수집 및 엑셀 갱신 스크립트"
    )
    parser.add_argument(
        "--bgn-de", type=str, help="검색 시작일자 (YYYYMMDD, 기본값: 어제)"
    )
    parser.add_argument(
        "--end-de", type=str, help="검색 종료일자 (YYYYMMDD, 기본값: 오늘)"
    )
    parser.add_argument(
        "--companies",
        type=str,
        default="data/target_companies.csv",
        help="대상 기업 목록 파일 경로",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="output/financial_data_result.xlsx",
        help="최종 피벗 엑셀 저장 경로 (로컬 임시본)",
    )
    return parser.parse_args()


def load_target_companies(file_path: Path) -> list[str]:
    """대상 기업 목록 파일에서 유효 기업명들을 로드합니다."""
    if not file_path.exists():
        logger.error(f"대상 기업 목록 파일이 존재하지 않습니다: {file_path}")
        return []
    try:
        if file_path.suffix == ".csv":
            df = pd.read_csv(file_path)
        elif file_path.suffix == ".xlsx":
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

    drive_folder_id = os.getenv("GOOGLE_DRIVE_FINANCIAL_STATEMENTS_ID")
    if not drive_folder_id:
        logger.error(
            "GOOGLE_DRIVE_FINANCIAL_STATEMENTS_ID 환경 변수가 설정되지 않았습니다. "
            "DB SSOT가 Drive이므로 이 값 없이는 실행할 수 없습니다."
        )
        sys.exit(1)

    token_path = "secrets/token.json"
    if not os.path.exists(token_path):
        logger.error(f"구글 드라이브 토큰 파일이 존재하지 않습니다: {token_path}")
        sys.exit(1)

    args = parse_arguments()

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    bgn_de = args.bgn_de or yesterday.strftime("%Y%m%d")
    end_de = args.end_de or today.strftime("%Y%m%d")

    logger.info(f"🚀 데일리 수집 구동 시작 (스캔 범위: {bgn_de} ~ {end_de})")

    client_secret_path = "secrets/client_secret.json"
    storage_port = GoogleDriveAdapter(
        token_file=token_path,
        root_folder_id=drive_folder_id,
        client_secret_file=client_secret_path
        if os.path.exists(client_secret_path)
        else None,
    )

    # 설정 파일(target_companies.csv 등)은 로컬과 무관하게 항상 Drive의 db/ 폴더
    # 내용을 기준으로 동작해야 하므로, 대상 기업 목록을 읽기 전에 먼저 동기화한다.
    sync_results = download_config_files(storage_port, CONFIG_REMOTE_TO_LOCAL)
    for remote_path, ok in sync_results.items():
        if not ok:
            logger.warning(
                f"⚠️ 설정 파일을 Drive에서 받지 못했습니다(로컬 사본 유지 시도): {remote_path}"
            )

    company_names = load_target_companies(Path(args.companies))
    if not company_names:
        logger.error("수집할 대상 기업이 없습니다. 스케줄러를 종료합니다.")
        sys.exit(1)
    logger.info(f"로드된 수집 대상 기업: {len(company_names)}개")

    corp_code_adapter = CorpCodeAdapter()
    api_financial_adapter = DartFinancialAdapter(api_key=api_key, use_cache=True)
    xbrl_financial_adapter = DartXbrlFinancialAdapter(api_key=api_key)
    processing_service = DataProcessingService()
    ticker_name_port = NaverTickerNameAdapter()

    def collection_service_factory(repo):
        # repo와 동일한(Drive에서 받은 임시 작업 사본) DB 파일을 공유해야 티커명
        # 캐시도 SSOT 업로드에 함께 반영된다. 기본 경로(data/financial_data.db)로
        # 두면 SSOT 전환 전 남아있던 로컬 파일에 고립되어 저장된다.
        return DailyCollectionService(
            corp_code_port=corp_code_adapter,
            financial_port=api_financial_adapter,
            repository_port=repo,
            processing_service=processing_service,
            xbrl_collector_port=xbrl_financial_adapter,
            ticker_name_port=ticker_name_port,
            ticker_cache_port=SqliteTickerNameCacheAdapter(db_path=repo.db_path),
        )

    def export_service_factory(repo):
        # 지연 임포트: export_blacklist.csv를 config 동기화로 받은 *뒤에* 이 모듈이
        # 최초 임포트되어야 EXPORT_BLACKLIST가 최신 원격 내용을 반영한다.
        from core.services.financial_data_export_service import (
            FinancialDataExportService,
        )

        return FinancialDataExportService(
            repository_port=repo,
            export_port=ExcelExportAdapter(),
            processing_service=processing_service,
        )

    routine = DailyRoutineService(
        storage_port=storage_port,
        db_remote_path=DB_REMOTE_PATH,
        artifact_remote_path=ARTIFACT_REMOTE_PATH,
        local_artifact_path=args.output,
        repository_factory=lambda db_path: SqliteRepositoryAdapter(db_path=db_path),
        collection_service_factory=collection_service_factory,
        export_service_factory=export_service_factory,
        target_company_names=company_names,
        start_date=bgn_de,
        end_date=end_de,
    )

    result = routine.run()

    logger.info(
        f"🏁 데일리 배치 완료 — 수집 성공 {len(result.collected_success)}건, "
        f"실패 {len(result.collected_failed)}건 / export={'성공' if result.export_success else '실패'} "
        f"/ 산출물 업로드={'성공' if result.artifact_uploaded else '실패'} "
        f"/ DB 업로드={'성공' if result.db_uploaded else '실패'}"
    )
    if result.error:
        logger.warning(f"⚠️ 수집 단계 예외: {result.error}")

    if not result.artifact_uploaded:
        logger.error("❌ 산출물(엑셀) 업로드가 실패했습니다 — 다음 실행 전에 재시도 필요.")
        sys.exit(1)

    if not result.db_uploaded:
        logger.error("❌ DB SSOT 업로드가 실패했습니다 — 이번 실행분이 Drive에 반영되지 않았습니다.")
        sys.exit(1)


if __name__ == "__main__":
    main()
