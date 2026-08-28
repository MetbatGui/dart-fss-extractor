"""DB SSOT(Google Drive) 왕복 세션.

db_ssot_guide.md §6 계약: 호출 하나마다
  StoragePort.get_file(db_path) -> tempfile에 씀 -> (사용) -> StoragePort.put_file(db_path)
를 반복한다. 원격의 DB 파일이 유일한 원본이며, 로컬 작업 사본은 캐시일 뿐이다.
"""

import logging
import os
import tempfile
from pathlib import Path

from core.ports.storage_port import StoragePort

logger = logging.getLogger(__name__)


class DbSyncSession:
    """원격(Drive) SSOT DB를 로컬 임시 작업 사본으로 내려받고, 작업 후 다시 올리는 세션.

    사용 순서: download() -> (SqliteRepositoryAdapter로 작업 사본 열어서 작업) ->
    connection.close() -> upload() -> cleanup(). 정리(cleanup)는 예외 경로에서도
    호출되도록 오케스트레이터의 finally에서 보장해야 한다.
    """

    def __init__(self, storage_port: StoragePort, remote_path: str):
        self._storage_port = storage_port
        self._remote_path = remote_path
        self._local_path: Path | None = None

    def download(self) -> Path:
        """원격 DB를 받아 로컬 임시 작업 사본 경로를 반환한다.

        원격에 아직 DB가 없으면(최초 설치) 빈 파일로 시작한다 — SqliteRepositoryAdapter가
        이를 열면서 스키마를 새로 초기화하고, 첫 성공 업로드가 그 DB를 원격 원본으로 만든다.
        """
        fd, tmp_path_str = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        local_path = Path(tmp_path_str)

        data = self._storage_port.get_file(self._remote_path)
        if data:
            local_path.write_bytes(data)
        else:
            logger.info(
                f"원격에 DB가 없어(최초 설치로 추정) 빈 작업 사본으로 시작합니다: {self._remote_path}"
            )

        self._local_path = local_path
        return local_path

    def upload(self) -> bool:
        """로컬 작업 사본을 읽어 원격에 업로드한다.

        download()를 먼저 호출하지 않았거나 로컬 파일이 사라진 상태면 아무것도
        전송하지 않고 False를 반환한다. 호출 전 SQLite connection을 반드시 close해서
        모든 쓰기가 디스크에 flush된 상태여야 한다.
        """
        if self._local_path is None or not self._local_path.is_file():
            logger.error("download() 없이(또는 로컬 사본 소실 상태에서) upload()가 호출되었습니다.")
            return False
        data = self._local_path.read_bytes()
        return self._storage_port.put_file(self._remote_path, data)

    def cleanup(self) -> None:
        """로컬 임시 작업 사본을 삭제한다. 여러 번 호출해도 안전하다."""
        if self._local_path and self._local_path.is_file():
            self._local_path.unlink()
        self._local_path = None
