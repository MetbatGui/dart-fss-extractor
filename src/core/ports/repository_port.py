"""데이터 저장소 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
from core.domain.models.company import Company


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

    @abstractmethod
    def save_partition(self, dataset_name: str, partition_name: str, df: pd.DataFrame) -> None:
        """데이터셋의 파티션(부분 데이터)을 저장.
        
        Args:
            dataset_name: 데이터셋 이름 (예: 'financial_data_raw')
            partition_name: 파티션 이름 (예: '005930' - 기업코드)
            df: 저장할 DataFrame
        """
        raise NotImplementedError

    @abstractmethod
    def exists(self, dataset_name: str, partition_name: str) -> bool:
        """파티션 존재 여부 확인.
        
        Args:
            dataset_name: 데이터셋 이름
            partition_name: 파티션 이름
            
        Returns:
            True if exists, False otherwise
        """
        raise NotImplementedError

    @abstractmethod
    def load_all(self, dataset_name: str) -> pd.DataFrame:
        """데이터셋의 모든 파티션을 읽어서 하나의 DataFrame으로 반환.
        
        Args:
            dataset_name: 데이터셋 이름
            
        Returns:
            통합된 DataFrame
        """
        raise NotImplementedError

    @abstractmethod
    def save_company_metadata(self, company: Company) -> None:
        """기업 메타데이터(상태) 저장.
        
        Args:
            company: 기업 도메인 객체
        """
        raise NotImplementedError

    @abstractmethod
    def load_company_metadata(self, code: str) -> Optional[Company]:
        """기업 메타데이터 로드.
        
        Args:
            code: 기업 코드
            
        Returns:
            Company 객체 또는 None
        """
        raise NotImplementedError

    @abstractmethod
    def load_partition(self, dataset_name: str, partition_name: str) -> pd.DataFrame:
        """특정 파티션(기업) 데이터를 로드.
        
        Args:
            dataset_name: 데이터셋 이름
            partition_name: 파티션 이름 (기업 코드)
            
        Returns:
            DataFrame
        """
        raise NotImplementedError
