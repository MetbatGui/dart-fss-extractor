"""사람이 가끔 수정하는 설정 파일(target_companies.csv 등)의 Drive 동기화.

DB(financial_data.db)와 달리 이 파일들은 파이프라인이 쓰지 않고 사람이 편집하는
입력이라 "읽기 전용" 동기화(A안)를 쓴다: 매 실행마다 Drive에서 받아 로컬 경로에
덮어쓰고, 원격에 없으면 로컬 파일을 그대로 둔 채 실패로만 표시한다 (업로드 없음).
"""

import logging
from pathlib import Path

from core.ports.storage_port import StoragePort

logger = logging.getLogger(__name__)

# Drive SSOT 공통 경로 (daily_scheduler.py / main.py가 함께 참조).
DB_REMOTE_PATH = "db/financial_statements.db"
ARTIFACT_REMOTE_PATH = "재무제표.xlsx"
CONFIG_REMOTE_TO_LOCAL = {
    "db/target_companies.csv": "data/target_companies.csv",
    "db/corps.csv": "data/corps.csv",
    "db/export_blacklist.csv": "data/export_blacklist.csv",
}


def download_config_files(
    storage_port: StoragePort, remote_to_local: dict[str, str]
) -> dict[str, bool]:
    """{원격경로: 로컬경로} 매핑대로 각 설정 파일을 다운로드한다.

    Args:
        storage_port: Drive 등 원격 저장소 포트.
        remote_to_local: 원격 경로 -> 로컬 저장 경로 매핑.

    Returns:
        원격 경로별 다운로드 성공 여부. 원격에 파일이 없으면 False이고
        기존 로컬 파일(있었다면)은 건드리지 않는다.
    """
    results: dict[str, bool] = {}
    for remote_path, local_path_str in remote_to_local.items():
        data = storage_port.get_file(remote_path)
        if data is None:
            logger.warning(
                f"원격에 설정 파일이 없어 로컬 사본을 그대로 둡니다: {remote_path}"
            )
            results[remote_path] = False
            continue

        local_path = Path(local_path_str)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)
        logger.info(f"설정 파일 동기화 완료: {remote_path} -> {local_path}")
        results[remote_path] = True

    return results
