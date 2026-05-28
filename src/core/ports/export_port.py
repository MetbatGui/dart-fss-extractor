"""데이터 내보내기 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd


class ExportPort(ABC):
    """데이터 내보내기 포트 (Presentation Layer)."""

    @abstractmethod
    def export_excel(
        self,
        dataframes: Dict[str, pd.DataFrame],
        file_path: str
    ) -> None:
        """여러 DataFrame을 엑셀 파일로 내보내기.
        
        Args:
            dataframes: {시트명: DataFrame} 딕셔너리
            file_path: 저장할 파일 경로
        """
        raise NotImplementedError
