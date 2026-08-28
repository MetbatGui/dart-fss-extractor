"""데일리 배치 전체 흐름(DB 연결→수집→export→업로드)을 소유하는 오케스트레이션 서비스.

orchestration_guide.md §1, §3 계약:
  DB 연결 -> 수집(fetch) -> upsert -> export(렌더링) -> 업로드(원격 동기화)
  업로드 순서: 산출물(사람이 바로 볼 결과) 먼저 -> SSOT DB 나중.
  산출물 업로드 실패와 DB 업로드 실패는 서로 독립적으로 시도되고 결과 값 객체에 기록된다.
  finally: repository connection close -> temporary working copy unlink.

CLI(daily_scheduler.py)는 이 서비스를 호출하고 결과를 로그로만 변환하는 역할만 맡는다.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from core.ports.storage_port import StoragePort
from core.services.db_sync_session import DbSyncSession

logger = logging.getLogger(__name__)


class _CollectionService(Protocol):
    def collect_daily_disclosures(
        self, target_company_names: list[str], start_date: str, end_date: str
    ) -> dict[str, list[str]]: ...


class _ExportService(Protocol):
    def export_integrated_financial_data(
        self, output_path: str, year_min: int | None = None, year_max: int | None = None
    ) -> bool: ...


@dataclass
class DailyRoutineResult:
    """한 번의 데일리 실행 결과 값 객체. CLI는 이 값을 로그/exit code로만 변환한다."""

    collected_success: list[str] = field(default_factory=list)
    collected_failed: list[str] = field(default_factory=list)
    export_success: bool = False
    artifact_uploaded: bool = False
    db_uploaded: bool = False
    error: str | None = None


class DailyRoutineService:
    """DB 다운로드부터 산출물/DB 업로드까지 데일리 배치 전체 흐름을 소유한다."""

    def __init__(
        self,
        storage_port: StoragePort,
        db_remote_path: str,
        artifact_remote_path: str,
        local_artifact_path: str,
        repository_factory: Callable[[str], Any],
        collection_service_factory: Callable[[Any], _CollectionService],
        export_service_factory: Callable[[Any], _ExportService],
        target_company_names: list[str],
        start_date: str,
        end_date: str,
    ):
        self._storage_port = storage_port
        self._db_remote_path = db_remote_path
        self._artifact_remote_path = artifact_remote_path
        self._local_artifact_path = local_artifact_path
        self._repository_factory = repository_factory
        self._collection_service_factory = collection_service_factory
        self._export_service_factory = export_service_factory
        self._target_company_names = target_company_names
        self._start_date = start_date
        self._end_date = end_date
        self._last_local_db_path: Path | None = None

    def run(self) -> DailyRoutineResult:
        session = DbSyncSession(self._storage_port, self._db_remote_path)
        local_db_path = session.download()
        self._last_local_db_path = local_db_path

        result = DailyRoutineResult()
        repo = None
        try:
            repo = self._repository_factory(str(local_db_path))

            try:
                collection_service = self._collection_service_factory(repo)
                collect_result = collection_service.collect_daily_disclosures(
                    target_company_names=self._target_company_names,
                    start_date=self._start_date,
                    end_date=self._end_date,
                )
                result.collected_success = collect_result.get("success", [])
                result.collected_failed = collect_result.get("failed", [])
            except Exception as e:
                logger.error(f"❌ 데일리 공시 수집 단계에서 예외 발생: {e}", exc_info=True)
                result.error = str(e)

            try:
                export_service = self._export_service_factory(repo)
                result.export_success = export_service.export_integrated_financial_data(
                    self._local_artifact_path
                )
            except Exception as e:
                logger.error(f"❌ 엑셀 export 단계에서 예외 발생: {e}", exc_info=True)
                result.export_success = False
        finally:
            if repo is not None and hasattr(repo, "close"):
                repo.close()

        # 업로드: 산출물(사람이 바로 볼 결과) 먼저, DB 나중. 서로 독립적으로 시도.
        if result.export_success:
            try:
                artifact_bytes = Path(self._local_artifact_path).read_bytes()
                result.artifact_uploaded = self._storage_port.put_file(
                    self._artifact_remote_path, artifact_bytes
                )
            except Exception as e:
                logger.error(f"❌ 산출물 업로드 중 예외 발생: {e}", exc_info=True)
                result.artifact_uploaded = False

        result.db_uploaded = session.upload()
        session.cleanup()

        return result
