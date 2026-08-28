"""DbSyncSession 단위 테스트 (TDD로 구현보다 먼저 작성).

db_ssot_guide.md §6의 세션 계약을 검증한다:
  StoragePort.get_file(db_path) -> tempfile에 씀 -> (사용) -> StoragePort.put_file(db_path)
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from core.ports.storage_port import StoragePort
from core.services.db_sync_session import DbSyncSession


@pytest.fixture
def mock_storage() -> MagicMock:
    return MagicMock(spec=StoragePort)


def test_download_writes_remote_bytes_to_local_temp_file(mock_storage):
    """원격에 DB가 있으면, 받은 바이트 그대로 로컬 임시 파일에 써야 한다."""
    mock_storage.get_file.return_value = b"REMOTE_DB_BYTES"
    session = DbSyncSession(mock_storage, remote_path="financial_data.db")

    local_path = session.download()

    mock_storage.get_file.assert_called_once_with("financial_data.db")
    assert local_path.is_file()
    assert local_path.read_bytes() == b"REMOTE_DB_BYTES"

    session.cleanup()


def test_download_creates_empty_local_file_when_remote_missing(mock_storage):
    """원격에 DB가 아직 없으면(최초 설치), 빈 로컬 파일로 시작해야 한다
    (SqliteRepositoryAdapter가 열면서 스키마를 새로 초기화할 수 있도록)."""
    mock_storage.get_file.return_value = None
    session = DbSyncSession(mock_storage, remote_path="financial_data.db")

    local_path = session.download()

    assert local_path.is_file()
    assert local_path.read_bytes() == b""

    session.cleanup()


def test_upload_sends_local_file_bytes_to_storage_port(mock_storage):
    """작업 사본에 쓰인 최신 바이트를 그대로 원격에 업로드해야 한다."""
    mock_storage.get_file.return_value = b"ORIGINAL"
    mock_storage.put_file.return_value = True
    session = DbSyncSession(mock_storage, remote_path="financial_data.db")

    local_path = session.download()
    local_path.write_bytes(b"MODIFIED_AFTER_WORK")

    ok = session.upload()

    assert ok is True
    mock_storage.put_file.assert_called_once_with(
        "financial_data.db", b"MODIFIED_AFTER_WORK"
    )

    session.cleanup()


def test_upload_returns_false_when_called_before_download(mock_storage):
    """download() 없이 upload()를 호출하면 원격에 아무것도 보내지 않고 False를 반환해야 한다."""
    session = DbSyncSession(mock_storage, remote_path="financial_data.db")

    ok = session.upload()

    assert ok is False
    mock_storage.put_file.assert_not_called()


def test_cleanup_removes_local_temp_file(mock_storage):
    """cleanup() 이후에는 로컬 임시 작업 사본이 디스크에서 사라져야 한다."""
    mock_storage.get_file.return_value = b"X"
    session = DbSyncSession(mock_storage, remote_path="financial_data.db")
    local_path = session.download()
    assert local_path.is_file()

    session.cleanup()

    assert not local_path.exists()


def test_cleanup_before_download_is_safe(mock_storage):
    """download() 전에 cleanup()을 호출해도 예외 없이 안전해야 한다."""
    session = DbSyncSession(mock_storage, remote_path="financial_data.db")
    session.cleanup()  # 예외가 나지 않아야 함


def test_cleanup_is_idempotent(mock_storage):
    """cleanup()을 두 번 호출해도 예외가 나지 않아야 한다."""
    mock_storage.get_file.return_value = b"X"
    session = DbSyncSession(mock_storage, remote_path="financial_data.db")
    session.download()

    session.cleanup()
    session.cleanup()  # 두 번째 호출도 안전해야 함


def test_download_uses_unique_temp_files_across_sessions(mock_storage):
    """서로 다른 세션은 서로 다른 임시 파일 경로를 써서 충돌하지 않아야 한다."""
    mock_storage.get_file.return_value = b"X"
    session_a = DbSyncSession(mock_storage, remote_path="financial_data.db")
    session_b = DbSyncSession(mock_storage, remote_path="financial_data.db")

    path_a = session_a.download()
    path_b = session_b.download()

    assert path_a != path_b

    session_a.cleanup()
    session_b.cleanup()
