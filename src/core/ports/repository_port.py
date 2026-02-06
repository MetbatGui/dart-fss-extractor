"""데이터 저장소 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd


class RepositoryPort(ABC):
    """데이터 영구 저장소 포트 (Source of Truth)."""

    @abstractmethod
    def save_dataframe(self, key: str, df: pd.DataFrame) -> None:
        """DataFrame을 저장소에 저장.
        
        Args:
            key: 저장할 데이터 식별 키 (예: 'financial_data_2023')
            df: 저장할 DataFrame
        """
        raise NotImplementedError

    @abstractmethod
    def load_dataframe(self, key: str) -> Optional[pd.DataFrame]:
        """저장소에서 DataFrame 로드.
        
        Args:
            key: 로드할 데이터 식별 키
            
        Returns:
            로드된 DataFrame, 없으면 None
        """
        raise NotImplementedError
