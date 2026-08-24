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
from freezegun import freeze_time

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
    # CorpCodeAdapter uses OUTPUT_DIRECTORY for cache path
    with patch.dict(os.environ, {"OUTPUT_DIRECTORY": str(temp_root), "DART_API_KEY": "dummy_key"}), \
         patch("requests.get") as mock_get:
        
        # Create a dummy zip file containing a dummy XML
        # Create a dummy zip file containing a dummy XML
        dummy_xml = (
            b"<result><list><corp_code>12345678</corp_code><corp_name>Test Corp</corp_name>"
            b"<stock_code>123456</stock_code><modify_date>20230101</modify_date></list>"
            + b"<!-- " + b"x" * 2000 + b" --></result>"
        )
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zf:
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


def test_smart_cache_invalidation() -> None:
    """타겟 컴퍼니 파일의 수정 시각이 캐시보다 최신일 때 강제 다운로드가 유발되는지 확인."""
    import shutil
    import tempfile
    import zipfile
    import io
    import time
    from unittest.mock import patch, MagicMock

    temp_root = Path(tempfile.mkdtemp())
    
    # 1. 파일 경로 준비
    target_csv = temp_root / "target_companies.csv"
    
    # 타겟 컴퍼니 임시 파일 작성
    with open(target_csv, "w", encoding="utf-8") as f:
        f.write("기업명\nTest Corp\n")
        
    with patch.dict(os.environ, {"OUTPUT_DIRECTORY": str(temp_root), "DART_API_KEY": "dummy_key"}), \
         patch("requests.get") as mock_get:
        
        # 2. 첫 다운로드를 위한 더미 ZIP 세팅
        dummy_xml = (
            b"<result><list><corp_code>12345678</corp_code><corp_name>Test Corp</corp_name>"
            b"<stock_code>123456</stock_code><modify_date>20230101</modify_date></list>"
            + b"<!-- " + b"x" * 2000 + b" --></result>"
        )
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zf:
            zf.writestr("CORPCODE.xml", dummy_xml)
        
        mock_response = MagicMock()
        mock_response.content = zip_buffer.getvalue()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # 3. 최초 어댑터 초기화 (최초 다운로드 수행됨)
        adapter = CorpCodeAdapter(force_download=False, target_companies_path=str(target_csv))
        assert mock_get.call_count == 1
        
        # 4. 다운로드 횟수 초기화 및 캐시 파일이 더 최신인 상태 테스트
        mock_get.reset_mock()
        # 캐시의 수정 시각을 더 뒤로 조정
        os.utime(adapter._XML_PATH, (time.time() + 10, time.time() + 10))
        # 다시 어댑터 생성 (mtime이 캐시가 더 최신이므로 다운로드 미호출해야 함)
        _ = CorpCodeAdapter(force_download=False, target_companies_path=str(target_csv))
        assert mock_get.call_count == 0

        # 5. 타겟 컴퍼니 파일의 수정 시각을 캐시보다 더 미래(최신)로 조정
        os.utime(target_csv, (time.time() + 20, time.time() + 20))
        # 다시 어댑터 생성 (target_mtime > cache_mtime이므로 자동 강제 다운로드 유발)
        _ = CorpCodeAdapter(force_download=False, target_companies_path=str(target_csv))
        assert mock_get.call_count == 1

    # Cleanup
    shutil.rmtree(temp_root)


def test_name_collision_prefers_listed_company(monkeypatch) -> None:
    """동명이인(같은 이름의 비상장 기타법인 + 상장사)이 있을 때, XML 등장 순서와
    무관하게 상장사(stock_code 보유)가 우선 선택되어야 한다.

    과거엔 XML에서 나중에 나오는 쪽이 무조건 덮어써서, 비상장 동명이인이
    실제 상장사 매핑을 가리는 데이터 오염(예: 태광, 대웅) 버그가 있었음.
    (data/corps.csv가 있으면 그쪽이 우선 로드되어 XML 경로를 안 타므로,
    실제 저장소의 corps.csv를 우회하기 위해 임시 디렉터리로 cwd를 옮긴다.)
    """
    import shutil
    import tempfile
    import zipfile
    import io
    from unittest.mock import patch, MagicMock

    original_cwd = Path.cwd()
    temp_root = Path(tempfile.mkdtemp())
    monkeypatch.chdir(temp_root)

    # 비상장 동명이인이 상장사보다 XML에서 먼저 나오는 케이스
    dummy_xml = (
        b"<result>"
        b"<list><corp_code>99999999</corp_code><corp_name>\xed\x83\x9c\xea\xb4\x91</corp_name>"
        b"<stock_code></stock_code><modify_date>20230101</modify_date></list>"
        b"<list><corp_code>00153375</corp_code><corp_name>\xed\x83\x9c\xea\xb4\x91</corp_name>"
        b"<stock_code>023160</stock_code><modify_date>20230101</modify_date></list>"
        b"</result>"
        + b"<!-- " + b"x" * 2000 + b" -->"
    )
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("CORPCODE.xml", dummy_xml)

    mock_response = MagicMock()
    mock_response.content = zip_buffer.getvalue()
    mock_response.raise_for_status.return_value = None

    with patch.dict(os.environ, {"OUTPUT_DIRECTORY": str(temp_root), "DART_API_KEY": "dummy_key"}), \
         patch("requests.get", return_value=mock_response):
        adapter = CorpCodeAdapter(force_download=True)
        assert adapter.get_code("태광") == "00153375"

    monkeypatch.chdir(original_cwd)
    shutil.rmtree(temp_root)


def test_cache_expires_after_max_age_days() -> None:
    """캐시 파일이 max_age_days보다 오래되면 자동 재다운로드되는지 확인 (freezegun으로 시간 경과 시뮬레이션)."""
    import shutil
    import tempfile
    import zipfile
    import io
    from datetime import datetime, timedelta
    from unittest.mock import patch, MagicMock

    temp_root = Path(tempfile.mkdtemp())

    dummy_xml = (
        b"<result><list><corp_code>12345678</corp_code><corp_name>Test Corp</corp_name>"
        b"<stock_code>123456</stock_code><modify_date>20230101</modify_date></list>"
        + b"<!-- " + b"x" * 2000 + b" --></result>"
    )
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("CORPCODE.xml", dummy_xml)

    mock_response = MagicMock()
    mock_response.content = zip_buffer.getvalue()
    mock_response.raise_for_status.return_value = None

    with patch.dict(os.environ, {"OUTPUT_DIRECTORY": str(temp_root), "DART_API_KEY": "dummy_key"}), \
         patch("requests.get", return_value=mock_response) as mock_get:

        now = datetime.now()

        # 1. 최초 다운로드 (기준 시각 고정)
        with freeze_time(now):
            adapter = CorpCodeAdapter(force_download=False, max_age_days=30)
            assert mock_get.call_count == 1

        mock_get.reset_mock()

        # 2. 29일 경과: max_age_days(30일) 미만이므로 재다운로드 없어야 함
        with freeze_time(now + timedelta(days=29)):
            _ = CorpCodeAdapter(
                force_download=False,
                target_companies_path=str(adapter._target_companies_path),
                max_age_days=30,
            )
            assert mock_get.call_count == 0

        # 3. 31일 경과: max_age_days 초과이므로 자동 재다운로드되어야 함
        with freeze_time(now + timedelta(days=31)):
            _ = CorpCodeAdapter(
                force_download=False,
                target_companies_path=str(adapter._target_companies_path),
                max_age_days=30,
            )
            assert mock_get.call_count == 1

    # Cleanup
    shutil.rmtree(temp_root)
