"""로컬 파일 시스템 파일 읽기 어댑터."""

from pathlib import Path
from typing import Dict
import pandas as pd

from core.ports.file_reader_port import FileReaderPort


class LocalFileReaderAdapter(FileReaderPort):
    """로컬 파일 시스템에서 파일을 읽는 어댑터."""
    
    def read_excel_with_sheets(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """엑셀 파일의 모든 시트를 읽어옵니다.
        
        Args:
            file_path: 읽을 엑셀 파일 경로
            
        Returns:
            {시트명: DataFrame} 딕셔너리
            
        Raises:
            FileNotFoundError: 파일이 존재하지 않을 경우
        """
        if not Path(file_path).exists():
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
        
        # 모든 시트를 딕셔너리로 읽기 (index_col=0: 첫 번째 컬럼을 인덱스로 사용 - 기업명)
        sheets_dict = pd.read_excel(file_path, sheet_name=None, engine='openpyxl', index_col=0)
        return sheets_dict
