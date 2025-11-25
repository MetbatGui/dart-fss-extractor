"""로컬 파일 시스템 저장 어댑터."""

import os
from pathlib import Path
from typing import Dict
import pandas as pd

from src.core.ports.storage_port import StoragePort


class LocalStorageAdapter(StoragePort):
    """로컬 파일 시스템에 데이터를 저장하는 어댑터.
    
    - pandas + openpyxl을 사용한 엑셀 파일 생성
    - 단일 파일에 다중 시트 저장 지원
    """

    def __init__(self, ensure_dir: bool = True):
        """초기화.
        
        Args:
            ensure_dir: True이면 저장 전 디렉터리 자동 생성
        """
        self._ensure_dir = ensure_dir

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
        # 디렉터리 생성
        if self._ensure_dir:
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        # ExcelWriter를 사용하여 다중 시트 저장
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=True)
