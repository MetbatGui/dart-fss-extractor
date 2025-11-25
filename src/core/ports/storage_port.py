"""데이터 저장을 위한 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd


class StoragePort(ABC):
    """데이터 저장 포트."""

    @abstractmethod
    def save_excel_with_sheets(
        self,
        dataframes: Dict[str, pd.DataFrame],
        file_path: str
    ) -> None:
        """여러 DataFrame을 단일 엑셀 파일에 다중 시트로 저장.
        
        Args:
            dataframes: {시트명: DataFrame} 딕셔너리
            file_path: 저장할 엑셀 파일 경로
        """
        raise NotImplementedError
