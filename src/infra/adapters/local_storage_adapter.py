"""로컬 파일 시스템 저장 어댑터."""

import os
from pathlib import Path
from typing import Dict
import pandas as pd

from core.ports.storage_port import StoragePort


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
            # 각 시트에 DataFrame 저장 (인덱스 포함)
            for sheet_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=True)

            # ----- bestfit 적용 시작 -----
            workbook = writer.book
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column_cells in worksheet.iter_cols():
                    max_length = max(
                        len(str(cell.value)) if cell.value is not None else 0
                        for cell in column_cells
                    )
                    # 약간 여유를 두어 열 너비 설정
                    adjusted_width = max_length + 2
                    column_letter = column_cells[0].column_letter
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            # ----- bestfit 적용 끝 -----
            # writer.save()는 context manager 종료 시 자동으로 호출됨
