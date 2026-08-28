"""DailyRoutineService 단위 테스트 (TDD로 구현보다 먼저 작성).

orchestration_guide.md §1, §3 계약을 검증한다:
  DB 연결 -> 수집 -> upsert -> export -> 업로드(산출물 먼저, DB 나중)
  finally: repository connection close -> temporary working copy unlink
  산출물 업로드 실패와 DB 업로드 실패는 서로 독립적으로 시도되고 결과 값 객체에 기록된다.
"""

from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from core.ports.storage_port import StoragePort
from core.services.daily_routine_service import DailyRoutineService


@pytest.fixture
def call_log():
    """여러 mock에 걸친 호출 순서를 하나의 리스트로 기록한다."""
    return []


@pytest.fixture
def mock_storage(call_log):
    storage = MagicMock(spec=StoragePort)
    storage.get_file.return_value = b"REMOTE_DB"
    storage.put_file.side_effect = lambda path, data: (
        call_log.append(f"put_file:{path}") or True
    )
    return storage


@pytest.fixture
def mock_repo(call_log):
    repo = MagicMock()
    repo.close.side_effect = lambda: call_log.append("repo.close")
    return repo


@pytest.fixture
def mock_collection_service(call_log):
    svc = MagicMock()
    svc.collect_daily_disclosures.side_effect = lambda **kwargs: (
        call_log.append("collect") or {"success": ["001"], "failed": []}
    )
    return svc


@pytest.fixture
def mock_export_service(call_log, tmp_path):
    svc = MagicMock()

    def _export(output_path, *args, **kwargs):
        call_log.append("export")
        Path(output_path).write_bytes(b"FAKE_XLSX")
        return True

    svc.export_integrated_financial_data.side_effect = _export
    return svc


@pytest.fixture
def service(mock_storage, mock_repo, mock_collection_service, mock_export_service, tmp_path):
    return DailyRoutineService(
        storage_port=mock_storage,
        db_remote_path="financial_data.db",
        artifact_remote_path="재무제표.xlsx",
        local_artifact_path=str(tmp_path / "result.xlsx"),
        repository_factory=lambda db_path: mock_repo,
        collection_service_factory=lambda repo: mock_collection_service,
        export_service_factory=lambda repo: mock_export_service,
        target_company_names=["A사"],
        start_date="20260101",
        end_date="20260101",
    )


def test_happy_path_runs_full_sequence_in_order(service, call_log, mock_storage):
    """다운로드 -> 수집 -> export -> 산출물 업로드 -> DB 업로드 -> close -> cleanup 순서를 지켜야 한다."""
    result = service.run()

    # close가 upload들보다 먼저(디스크 flush 보장), 산출물 업로드가 DB 업로드보다 먼저
    assert call_log.index("collect") < call_log.index("export")
    assert call_log.index("repo.close") < call_log.index("put_file:재무제표.xlsx")
    assert call_log.index("put_file:재무제표.xlsx") < call_log.index("put_file:financial_data.db")

    assert result.collected_success == ["001"]
    assert result.collected_failed == []
    assert result.export_success is True
    assert result.artifact_uploaded is True
    assert result.db_uploaded is True


def test_export_failure_skips_artifact_upload_but_still_uploads_db(
    mock_storage, mock_repo, mock_collection_service, tmp_path
):
    """export가 실패하면 산출물 업로드는 건너뛰되, DB 업로드는 그래도 시도해야 한다."""
    export_service = MagicMock()
    export_service.export_integrated_financial_data.return_value = False

    svc = DailyRoutineService(
        storage_port=mock_storage,
        db_remote_path="financial_data.db",
        artifact_remote_path="재무제표.xlsx",
        local_artifact_path=str(tmp_path / "result.xlsx"),
        repository_factory=lambda db_path: mock_repo,
        collection_service_factory=lambda repo: mock_collection_service,
        export_service_factory=lambda repo: export_service,
        target_company_names=["A사"],
        start_date="20260101",
        end_date="20260101",
    )

    result = svc.run()

    assert result.export_success is False
    assert result.artifact_uploaded is False
    assert result.db_uploaded is True
    # 산출물 경로로는 업로드가 시도되지 않아야 함
    mock_storage.put_file.assert_called_once_with("financial_data.db", b"REMOTE_DB")


def test_artifact_upload_failure_does_not_block_db_upload(
    mock_repo, mock_collection_service, mock_export_service, tmp_path
):
    """산출물 업로드가 실패해도 DB 업로드는 독립적으로 계속 시도해야 한다."""
    storage = MagicMock(spec=StoragePort)
    storage.get_file.return_value = b"REMOTE_DB"

    def put_file(path, data):
        return path != "재무제표.xlsx"  # 산출물 업로드만 실패시킴

    storage.put_file.side_effect = put_file

    svc = DailyRoutineService(
        storage_port=storage,
        db_remote_path="financial_data.db",
        artifact_remote_path="재무제표.xlsx",
        local_artifact_path=str(tmp_path / "result.xlsx"),
        repository_factory=lambda db_path: mock_repo,
        collection_service_factory=lambda repo: mock_collection_service,
        export_service_factory=lambda repo: mock_export_service,
        target_company_names=["A사"],
        start_date="20260101",
        end_date="20260101",
    )

    result = svc.run()

    assert result.artifact_uploaded is False
    assert result.db_uploaded is True


def test_local_working_copy_is_cleaned_up_after_run(service, tmp_path):
    """실행이 끝나면 로컬 임시 작업 사본이 디스크에 남지 않아야 한다."""
    service.run()

    assert service._last_local_db_path is not None
    assert not service._last_local_db_path.exists()


def test_cleanup_happens_even_when_collection_raises(
    mock_storage, mock_repo, mock_export_service, tmp_path
):
    """수집 단계에서 예외가 나도 repository는 닫히고 로컬 작업 사본은 정리되어야 한다
    (db_ssot_guide.md §6: 예외 경로에서도 finally 정리가 실행돼야 함)."""
    collection_service = MagicMock()
    collection_service.collect_daily_disclosures.side_effect = RuntimeError("네트워크 오류")

    svc = DailyRoutineService(
        storage_port=mock_storage,
        db_remote_path="financial_data.db",
        artifact_remote_path="재무제표.xlsx",
        local_artifact_path=str(tmp_path / "result.xlsx"),
        repository_factory=lambda db_path: mock_repo,
        collection_service_factory=lambda repo: collection_service,
        export_service_factory=lambda repo: mock_export_service,
        target_company_names=["A사"],
        start_date="20260101",
        end_date="20260101",
    )

    result = svc.run()

    mock_repo.close.assert_called_once()
    assert not svc._last_local_db_path.exists()
    assert result.collected_success == []
    assert result.collected_failed == []
    assert result.error is not None
    # 수집 단계와 export 단계는 서로 독립적: 수집이 실패해도 export/업로드는 계속 시도됨
    assert result.export_success is True
    assert result.db_uploaded is True
