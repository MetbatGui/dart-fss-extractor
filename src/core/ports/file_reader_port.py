"""파일 읽기를 위한 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd


class FileReaderPort(ABC):
    """파일 읽기를 위한 포트."""
    
    @abstractmethod
    def read_excel_with_sheets(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """엑셀 파일의 모든 시트를 읽어옵니다.
        
        Args:
            file_path: 읽을 엑셀 파일 경로
            
        Returns:
            {시트명: DataFrame} 딕셔너리
        """
        raise NotImplementedError
