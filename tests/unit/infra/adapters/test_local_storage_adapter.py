"""Local Storage Adapter 테스트."""

import os
import tempfile
from pathlib import Path
import pandas as pd
import pytest

from infra.adapters.local_storage_adapter import LocalStorageAdapter


@pytest.fixture
def adapter():
    """테스트용 어댑터 인스턴스."""
    return LocalStorageAdapter(ensure_dir=True)


@pytest.fixture
def temp_dir():
    """임시 디렉터리."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_save_excel_with_single_sheet(adapter, temp_dir):
    """단일 시트 저장 테스트."""
    # Arrange
    df = pd.DataFrame({
        '기업명': ['삼성전자', 'LG전자'],
        '2023': [100, 50],
        '2024': [120, 60]
    })
    dataframes = {'매출액_연간': df}
    file_path = Path(temp_dir) / 'test_single.xlsx'

    # Act
    adapter.save_excel_with_sheets(dataframes, str(file_path))

    # Assert
    assert file_path.exists(), "엑셀 파일이 생성되어야 합니다"
    
    # 파일 읽어서 검증
    loaded_df = pd.read_excel(file_path, sheet_name='매출액_연간', index_col=0)
    assert loaded_df.shape == df.shape, "데이터 크기가 일치해야 합니다"
    assert list(loaded_df.columns) == list(df.columns), "컬럼이 일치해야 합니다"


def test_save_excel_with_multiple_sheets(adapter, temp_dir):
    """다중 시트 저장 테스트 (6개 시트)."""
    # Arrange
    df1 = pd.DataFrame({'기업명': ['삼성전자'], '2023Q1': [25], '2023Q2': [30]})
    df2 = pd.DataFrame({'기업명': ['삼성전자'], '2023': [100]})
    df3 = pd.DataFrame({'기업명': ['삼성전자'], '2023Q1': [5], '2023Q2': [6]})
    df4 = pd.DataFrame({'기업명': ['삼성전자'], '2023': [20]})
    df5 = pd.DataFrame({'기업명': ['삼성전자'], '2023Q1': [10], '2023Q2': [12]})
    df6 = pd.DataFrame({'기업명': ['삼성전자'], '2023': [40]})
    
    dataframes = {
        '매출액_분기': df1,
        '매출액_연간': df2,
        '영업이익_분기': df3,
        '영업이익_연간': df4,
        '당기순이익_분기': df5,
        '당기순이익_연간': df6
    }
    file_path = Path(temp_dir) / 'test_multi.xlsx'

    # Act
    adapter.save_excel_with_sheets(dataframes, str(file_path))

    # Assert
    assert file_path.exists(), "엑셀 파일이 생성되어야 합니다"
    
    # 각 시트 확인 (context manager 사용)
    with pd.ExcelFile(file_path) as excel_file:
        assert len(excel_file.sheet_names) == 6, "6개 시트가 있어야 합니다"
        
        expected_sheets = [
            '매출액_분기', '매출액_연간', 
            '영업이익_분기', '영업이익_연간',
            '당기순이익_분기', '당기순이익_연간'
        ]
        for sheet_name in expected_sheets:
            assert sheet_name in excel_file.sheet_names, f"{sheet_name} 시트가 있어야 합니다"


def test_auto_create_directory(temp_dir):
    """디렉터리 자동 생성 테스트."""
    # Arrange
    adapter = LocalStorageAdapter(ensure_dir=True)
    df = pd.DataFrame({'A': [1, 2]})
    file_path = Path(temp_dir) / 'subdir' / 'nested' / 'test.xlsx'
    
    # Act
    adapter.save_excel_with_sheets({'Sheet1': df}, str(file_path))
    
    # Assert
    assert file_path.exists(), "중첩 디렉터리가 자동 생성되고 파일이 저장되어야 합니다"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
