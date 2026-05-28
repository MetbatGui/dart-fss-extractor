"""엑셀 내보내기 어댑터."""

from typing import Dict
import pandas as pd
from pathlib import Path

from core.ports.export_port import ExportPort


class ExcelExportAdapter(ExportPort):
    """Excel 내보내기 어댑터."""

    def export_excel(
        self,
        dataframes: Dict[str, pd.DataFrame],
        file_path: str
    ) -> None:
        """DataFrame 딕셔너리를 엑셀 파일로 저장."""
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            for sheet_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=sheet_name)
