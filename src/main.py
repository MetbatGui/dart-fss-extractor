"""메인 실행 스크립트 (수동 벌크 수집).

DB SSOT는 Google Drive다 (db_ssot_guide.md §1, §6). daily_scheduler.py처럼
매 실행마다 DB를 받아 작업하고 끝나면 다시 올린다. 다만 이 스크립트는
연/분기 벌크 수집이라 DailyRoutineService(일자 범위 기반 데일리 전용)를
억지로 맞추지 않고, 같은 재료(DbSyncSession, config_sync)를 직접 조합한다.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from core.services.config_sync import (
    CONFIG_REMOTE_TO_LOCAL,
    DB_REMOTE_PATH,
    download_config_files,
)
from core.services.data_processing_service import DataProcessingService
from core.services.db_sync_session import DbSyncSession
from core.services.financial_collection_service import FinancialCollectionService
from infra.adapters.corp_code_adapter import CorpCodeAdapter
from infra.adapters.dart_financial_adapter import DartFinancialAdapter
from infra.adapters.excel_export_adapter import ExcelExportAdapter
from infra.adapters.sqlite.sqlite_repository_adapter import SqliteRepositoryAdapter
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def load_company_names(target_file: Path) -> list[str] | None:
    """대상 기업 목록 파일에서 기업명 리스트를 로드한다. 실패 시 None."""
    if not target_file.exists():
        logger.error(f"기업 목록 파일을 찾을 수 없습니다: {target_file}")
        return None

    try:
        if target_file.suffix == ".csv":
            df = pd.read_csv(target_file)
        elif target_file.suffix == ".xlsx":
            df = pd.read_excel(target_file)
        else:
            logger.error(f"지원하지 않는 파일 형식입니다: {target_file.suffix}")
            return None

        # 1. 컬럼명 직접 매칭 시도
        target_col = None
        for col in df.columns:
            col_str = str(col).strip()
            if col_str in ["기업명", "종목명", "회사명", "corp_name"]:
                target_col = col
                break

        if target_col is not None:
            company_names = (
                df[target_col].dropna().astype(str).str.strip().unique().tolist()
            )
            logger.info(
                f"'{target_file}'의 '{target_col}' 컬럼을 사용하여 {len(company_names)}개 기업을 로드했습니다."
            )
            return company_names

        # 2. 컬럼명이 매칭되지 않을 경우 열 형태에 따른 추정
        if df.shape[1] >= 2:
            first_val = (
                str(df.iloc[:, 0].dropna().iloc[0]).strip()
                if not df.iloc[:, 0].dropna().empty
                else ""
            )
            if first_val.isdigit() and len(first_val) == 6:
                # 첫 번째 열이 코드이고 두 번째 열이 기업명일 확률이 매우 높음 (전종목리스트.xlsx 대응)
                company_names = (
                    df.iloc[:, 1].dropna().astype(str).str.strip().unique().tolist()
                )
                logger.info(
                    f"'{target_file}'의 첫 번째 열(코드 형태)을 건너뛰고, 두 번째 열을 사용하여 {len(company_names)}개 기업을 로드했습니다."
                )
                return company_names

        # 3. 차선책: 첫 번째 열 사용 (기존 호환성 유지)
        company_names = (
            df.iloc[:, 0].dropna().astype(str).str.strip().unique().tolist()
        )
        logger.info(
            f"'{target_file}'의 첫 번째 열을 사용하여 {len(company_names)}개 기업을 로드했습니다."
        )
        return company_names
    except Exception as e:
        logger.error(f"기업 목록 파일 읽기 실패: {e}")
        return None


def main():
    # .env 파일 로드
    load_dotenv()

    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        return

    drive_folder_id = os.getenv("GOOGLE_DRIVE_FINANCIAL_STATEMENTS_ID")
    if not drive_folder_id:
        logger.error(
            "GOOGLE_DRIVE_FINANCIAL_STATEMENTS_ID 환경 변수가 설정되지 않았습니다. "
            "DB SSOT가 Drive이므로 이 값 없이는 실행할 수 없습니다."
        )
        return

    token_path = "secrets/token.json"
    if not os.path.exists(token_path):
        logger.error(f"구글 드라이브 토큰 파일이 존재하지 않습니다: {token_path}")
        return

    # 인자 파싱
    parser = argparse.ArgumentParser(description="DART 재무 데이터 수집기")
    parser.add_argument(
        "--start-year", type=int, default=2015, help="수집 시작 연도 (기본값: 2015)"
    )
    parser.add_argument(
        "--end-year", type=int, default=2025, help="수집 종료 연도 (기본값: 2025)"
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
        help="결과 엑셀 파일 저장 경로",
    )
    parser.add_argument(
        "--force", action="store_true", help="기존 수집 데이터를 무시하고 다시 수집"
    )
    parser.add_argument(
        "--no-skip-failed", action="store_true", help="실패한 연도도 다시 시도"
    )

    args = parser.parse_args()

    logger.info("서비스 초기화 중...")

    client_secret_path = "secrets/client_secret.json"
    storage_port = GoogleDriveAdapter(
        token_file=token_path,
        root_folder_id=drive_folder_id,
        client_secret_file=client_secret_path
        if os.path.exists(client_secret_path)
        else None,
    )

    # 설정 파일(target_companies.csv 등)은 로컬과 무관하게 항상 Drive db/ 폴더
    # 내용을 기준으로 동작해야 하므로, 기업 목록을 읽기 전에 먼저 동기화한다.
    sync_results = download_config_files(storage_port, CONFIG_REMOTE_TO_LOCAL)
    for remote_path, ok in sync_results.items():
        if not ok:
            logger.warning(
                f"⚠️ 설정 파일을 Drive에서 받지 못했습니다(로컬 사본 유지 시도): {remote_path}"
            )

    # 기업 목록은 --companies로 임의 경로(스크래치용 부분 목록 등)를 줄 수 있으므로
    # 위 config 동기화(data/target_companies.csv 갱신)와 별개로 그 경로를 그대로 읽는다.
    company_names = load_company_names(Path(args.companies))
    if not company_names:
        logger.error("수집할 대상 기업이 없습니다.")
        return

    # 계정과목 키워드 설정 파일 직접 로드 (DI 적용)
    config_path = Path("config/account_keywords.toml")
    keywords_config = None
    if config_path.exists():
        try:
            import tomllib

            with open(config_path, "rb") as f:
                config = tomllib.load(f)
            keywords_config = config.get("account_keywords", {})
        except Exception as e:
            logger.error(f"설정 파일 읽기 실패: {e}")

    # DB SSOT(Drive)를 로컬 임시 작업 사본으로 받아온다.
    db_session = DbSyncSession(storage_port, DB_REMOTE_PATH)
    local_db_path = db_session.download()

    corp_code_adapter = CorpCodeAdapter()
    financial_adapter = DartFinancialAdapter(api_key=api_key, use_cache=True)
    repository_adapter = SqliteRepositoryAdapter(db_path=str(local_db_path))
    export_adapter = ExcelExportAdapter()
    processing_service = DataProcessingService(keywords_config=keywords_config)

    service = FinancialCollectionService(
        corp_code_port=corp_code_adapter,
        financial_port=financial_adapter,
        repository_port=repository_adapter,
        export_port=export_adapter,
        processing_service=processing_service,
    )

    logger.info(
        f"데이터 수집 시작: {len(company_names)}개 기업, {args.start_year}~{args.end_year}년"
    )

    try:
        service.collect_and_save(
            company_names=company_names,
            start_year=args.start_year,
            end_year=args.end_year,
            output_path=args.output,
            skip_failed=not args.no_skip_failed,
            force_recollect=args.force,
        )
        logger.info("모든 작업이 완료되었습니다.")
    except Exception as e:
        logger.exception(f"작업 중 치명적인 오류 발생: {e}")
    finally:
        repository_adapter.close()
        db_uploaded = db_session.upload()
        db_session.cleanup()
        if db_uploaded:
            logger.info("DB SSOT(Drive) 업로드 완료.")
        else:
            logger.error("DB SSOT(Drive) 업로드 실패 — 이번 실행분이 Drive에 반영되지 않았습니다.")


if __name__ == "__main__":
    main()
