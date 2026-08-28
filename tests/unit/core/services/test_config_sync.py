"""download_config_files() 단위 테스트 (TDD로 구현보다 먼저 작성).

target_companies.csv/corps.csv/export_blacklist.csv처럼 사람이 가끔
수정하는 설정 파일은 DB와 달리 "읽기 전용" 동기화다 (A안): 매 실행마다
Drive에서 받아 로컬 경로에 덮어쓰고, 원격에 없으면 로컬 파일을 그대로
둔 채 경고만 남긴다 (업로드는 하지 않음).
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from core.ports.storage_port import StoragePort
from core.services.config_sync import download_config_files


@pytest.fixture
def mock_storage():
    return MagicMock(spec=StoragePort)


def test_downloads_each_remote_file_to_its_local_path(mock_storage, tmp_path):
    """원격에 있는 파일은 지정된 로컬 경로에 그대로 써야 한다."""
    mock_storage.get_file.side_effect = lambda path: {
        "db/target_companies.csv": b"CSV_A",
        "db/corps.csv": b"CSV_B",
    }.get(path)

    mapping = {
        "db/target_companies.csv": str(tmp_path / "target_companies.csv"),
        "db/corps.csv": str(tmp_path / "corps.csv"),
    }

    results = download_config_files(mock_storage, mapping)

    assert results == {
        "db/target_companies.csv": True,
        "db/corps.csv": True,
    }
    assert (tmp_path / "target_companies.csv").read_bytes() == b"CSV_A"
    assert (tmp_path / "corps.csv").read_bytes() == b"CSV_B"


def test_missing_remote_file_keeps_existing_local_copy(mock_storage, tmp_path):
    """원격에 파일이 없으면 로컬 기존 파일을 건드리지 않고 실패로 표시해야 한다."""
    local_path = tmp_path / "export_blacklist.csv"
    local_path.write_bytes(b"OLD_LOCAL_CONTENT")
    mock_storage.get_file.return_value = None

    results = download_config_files(
        mock_storage, {"db/export_blacklist.csv": str(local_path)}
    )

    assert results == {"db/export_blacklist.csv": False}
    assert local_path.read_bytes() == b"OLD_LOCAL_CONTENT"


def test_missing_remote_file_with_no_local_copy_leaves_nothing(mock_storage, tmp_path):
    """원격에도 없고 로컬에도 없으면, 파일이 새로 생기지 않아야 한다."""
    local_path = tmp_path / "brand_new.csv"
    mock_storage.get_file.return_value = None

    results = download_config_files(mock_storage, {"db/brand_new.csv": str(local_path)})

    assert results == {"db/brand_new.csv": False}
    assert not local_path.exists()


def test_creates_parent_directory_if_missing(mock_storage, tmp_path):
    """로컬 경로의 상위 디렉터리가 없으면 새로 만들어서 써야 한다."""
    mock_storage.get_file.return_value = b"DATA"
    local_path = tmp_path / "nested" / "dir" / "file.csv"

    download_config_files(mock_storage, {"db/file.csv": str(local_path)})

    assert local_path.read_bytes() == b"DATA"
