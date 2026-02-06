"""Parquet 저장소 어댑터."""

import os
from pathlib import Path
from typing import Optional
import pandas as pd

from core.ports.repository_port import RepositoryPort


class ParquetRepositoryAdapter(RepositoryPort):
    """Parquet 파일 시스템 저장소."""

    def __init__(self, base_dir: str = "data/repository"):
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def save_dataframe(self, key: str, df: pd.DataFrame) -> None:
        """DataFrame을 Parquet로 저장."""
        file_path = self._get_file_path(key)
        # engine='pyarrow' 권장 (설치 필요, 없으면 fastparquet 등 사용)
        # 여기서는 기본적으로 pandas가 설치된 엔진을 사용
        df.to_parquet(file_path, index=True)

    def load_dataframe(self, key: str) -> Optional[pd.DataFrame]:
        """Parquet 파일 로드."""
        file_path = self._get_file_path(key)
        if not file_path.exists():
            return None
        return pd.read_parquet(file_path)

    def _get_file_path(self, key: str) -> Path:
        """키에 해당하는 파일 경로 반환."""
        return self._base_dir / f"{key}.parquet"
