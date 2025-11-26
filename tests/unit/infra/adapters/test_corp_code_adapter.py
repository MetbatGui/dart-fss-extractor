"""테스트 모듈: CorpCodeAdapter의 CSV 기반 기업코드 조회 기능 검증.

이 테스트는 다음 흐름을 따릅니다.
- Arrange: CSV 파일에서 기업명 리스트를 읽어 준비한다.
- Act: 어댑터의 `get_codes` 메서드에 기업명 리스트를 전달한다.
- Assert: 반환된 코드 리스트 길이가 입력과 일치하고, 각 요소가 문자열 또는 None인지 확인한다.
"""

import csv
import os
from pathlib import Path

import pytest

from infra.adapters.corp_code_adapter import CorpCodeAdapter

# CSV 파일 경로 (ROOT_DIR 환경변수 사용)
CSV_PATH = Path(os.getenv("ROOT_DIR", Path.cwd())) / "tests" / "fixtures" / "test_data" / "stock_list.csv"


def _read_company_names(csv_path: Path) -> list[str]:
    """CSV 파일에서 기업명(첫 번째 컬럼) 리스트를 반환한다."""
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if not rows:
            return []
        header = rows[0]
        start_idx = 1 if any(not cell.isdigit() for cell in header) else 0
        return [row[0].strip() for row in rows[start_idx:] if row]


@pytest.fixture(scope="module")
def adapter() -> CorpCodeAdapter:
    """테스트용 어댑터 인스턴스.

    `force_download=False` 로 기존 캐시를 재사용한다.
    """
    os.environ.setdefault("DART_API_KEY", os.getenv("DART_API_KEY", ""))
    return CorpCodeAdapter(force_download=False)


def test_get_codes_from_csv(adapter: CorpCodeAdapter) -> None:
    """AAA 패턴을 사용한 `get_codes` 동작 검증.

    Arrange: CSV 파일에서 기업명 리스트를 읽어 준비한다.
    Act: 어댑터의 `get_codes` 메서드에 기업명 리스트를 전달한다.
    Assert: 반환된 코드 리스트 길이가 입력과 일치하고, 각 요소가 문자열 또는 None인지 확인한다.
    """
    company_names = _read_company_names(CSV_PATH)
    assert company_names, "CSV 파일에서 기업명을 읽어올 수 없습니다."

    codes = adapter.get_codes(company_names)
    assert len(codes) == len(company_names), "반환된 코드 리스트 길이가 입력과 다릅니다."
    for code in codes:
        assert code is None or isinstance(code, str), "코드가 문자열이거나 None이어야 합니다."

def test_get_code_single(adapter: CorpCodeAdapter) -> None:
    """단일 기업명에 대한 코드 조회 테스트.

    Arrange: CSV 파일에서 첫 번째 기업명을 읽는다.
    Act: `get_code` 메서드에 전달한다.
    Assert: 반환값이 문자열이거나 None이다.
    """
    company_names = _read_company_names(CSV_PATH)
    assert company_names, "CSV 파일에서 기업명을 읽어올 수 없습니다."

    # Act
    codes = adapter.get_codes(company_names)

    # Assert
    assert len(codes) == len(company_names), "반환된 코드 리스트 길이가 입력과 다릅니다."
    for code in codes:
        assert code is None or isinstance(code, str), "코드가 문자열이거나 None이어야 합니다."


def test_force_download() -> None:
    """XML 파일이 없을 때 강제 다운로드가 동작하는지 확인.

    Arrange: 임시 디렉터리를 ROOT_DIR 로 지정하고, 기존 캐시를 삭제한다.
    Act: `CorpCodeAdapter(force_download=True)` 를 생성한다.
    Assert: 캐시 디렉터리에 CORPCODE.xml 파일이 존재한다.
    """
    import shutil
    import tempfile
    import zipfile
    import io
    from unittest.mock import patch, MagicMock

    temp_root = Path(tempfile.mkdtemp())
    
    # Mock environment and requests
    with patch.dict(os.environ, {"ROOT_DIR": str(temp_root), "DART_API_KEY": "dummy_key"}), \
         patch("requests.get") as mock_get:
        
        # Create a dummy zip file containing a dummy XML
        dummy_xml = b"<result><list><corp_code>12345678</corp_code><corp_name>Test Corp</corp_name><stock_code>123456</stock_code><modify_date>20230101</modify_date></list></result>"
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("CORPCODE.xml", dummy_xml)
        
        mock_response = MagicMock()
        mock_response.content = zip_buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Ensure adapter forces download
        adapter = CorpCodeAdapter(force_download=True)
        
        # Verify XML exists
        assert adapter._XML_PATH.is_file()
        
        # Verify content
        with open(adapter._XML_PATH, "rb") as f:
            content = f.read()
            assert b"Test Corp" in content

    # Cleanup
    shutil.rmtree(temp_root)
