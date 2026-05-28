"""Parquet 저장소 어댑터."""

import json
from pathlib import Path
from typing import Optional
import pandas as pd

from core.ports.repository_port import RepositoryPort
from core.domain.models.company import Company


class ParquetRepositoryAdapter(RepositoryPort):
    """Parquet 파일 시스템 저장소."""

    def __init__(self, base_dir: str = "data/repository"):
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def save_dataframe(self, key: str, df: pd.DataFrame) -> None:
        """DataFrame을 Parquet로 저장."""
        file_path = self._get_file_path(key)
        temp_path = file_path.with_suffix(".tmp")
        try:
            df.to_parquet(temp_path, index=True)
            import os
            os.replace(temp_path, file_path)
        except Exception as e:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            raise e

    def load_dataframe(self, key: str) -> Optional[pd.DataFrame]:
        """Parquet 파일 로드."""
        file_path = self._get_file_path(key)
        if not file_path.exists():
            return None
        return pd.read_parquet(file_path)

    def save_partition(
        self, dataset_name: str, partition_name: str, df: pd.DataFrame
    ) -> None:
        """데이터셋의 파티션(부분 데이터)을 저장."""
        dataset_dir = self._base_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        file_path = dataset_dir / f"{partition_name}.parquet"
        temp_path = file_path.with_suffix(".tmp")
        try:
            df.to_parquet(temp_path, index=True)
            import os
            os.replace(temp_path, file_path)
        except Exception as e:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            raise e

    def exists(self, dataset_name: str, partition_name: str) -> bool:
        """파티션 존재 여부 확인."""
        file_path = self._base_dir / dataset_name / f"{partition_name}.parquet"
        return file_path.exists()

    def load_all(self, dataset_name: str) -> pd.DataFrame:
        """데이터셋의 모든 파티션을 읽어서 하나의 DataFrame으로 반환."""
        dataset_dir = self._base_dir / dataset_name
        if not dataset_dir.exists():
            return pd.DataFrame()

        # 1. 기본 방식 시도 (엔진이 지원하는 경우 디렉토리 전체 읽기)
        try:
            return pd.read_parquet(dataset_dir)
        except Exception:
            # 2. 실패 시 개별 파일을 하나씩 읽어서 병합 (더 안전한 방식)
            all_files = list(dataset_dir.glob("*.parquet"))
            if not all_files:
                return pd.DataFrame()

            dfs = []
            for f in all_files:
                try:
                    dfs.append(pd.read_parquet(f))
                except Exception:
                    continue

            if not dfs:
                return pd.DataFrame()

            return pd.concat(dfs, ignore_index=True)

    def save_company_metadata(self, company: Company) -> None:
        """기업 메타데이터(상태) 저장."""
        # dataset_name은 상수로 가정하거나, Company 객체나 Config에서 가져와야 함.
        # 여기서는 financial_data_raw와 같은 디렉토리에 저장한다고 가정.
        dataset_name = "financial_data_raw"
        dataset_dir = self._base_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        file_path = dataset_dir / f"{company.code}_meta.json"
        temp_path = file_path.with_suffix(".tmp")
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(company.to_dict(), f, ensure_ascii=False, indent=2)
            import os
            os.replace(temp_path, file_path)
        except Exception as e:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            raise e

    def load_company_metadata(self, code: str) -> Optional[Company]:
        """기업 메타데이터 로드."""
        dataset_name = "financial_data_raw"
        file_path = self._base_dir / dataset_name / f"{code}_meta.json"

        if not file_path.exists():
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return Company.from_dict(data)
        except Exception:
            return None

    def load_partition(self, dataset_name: str, partition_name: str) -> pd.DataFrame:
        """특정 파티션(기업) 데이터를 로드."""
        file_path = self._base_dir / dataset_name / f"{partition_name}.parquet"
        try:
            return pd.read_parquet(file_path)
        except Exception:
            return pd.DataFrame()

    def _get_file_path(self, key: str) -> Path:
        """키에 해당하는 파일 경로 반환."""
        return self._base_dir / f"{key}.parquet"
