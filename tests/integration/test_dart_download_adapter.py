import os
import pytest
from unittest.mock import patch
from pathlib import Path
from dotenv import load_dotenv

# .env 로드
project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(project_root / ".env")

from core.ports.download_port import DownloadPort
from infra.adapters.dart_download_adapter import DartDownloadAdapter


def test_download_xbrl_zip_success():
    """유효한 접수번호로 호출 시, 올바른 ZIP 매직 넘버(PK\x03\x04)를 가진 바이트 스트림이 정상 수신되는지 테스트."""
    api_key = os.getenv("DART_API_KEY")
    assert api_key is not None, "DART_API_KEY 환경변수가 설정되어 있어야 합니다."
    
    adapter = DartDownloadAdapter(api_key=api_key)
    valid_rcept_no = "20250514000801" # 테스트용 유효 접수번호
    
    zip_bytes = adapter.download_xbrl_zip(valid_rcept_no)
    
    assert zip_bytes is not None, "성공적인 다운로드는 None이 아니어야 합니다."
    assert zip_bytes.startswith(b'PK\x03\x04'), "반환 데이터는 ZIP 포맷의 매직 넘버(PK\x03\x04)로 시작해야 합니다."


def test_download_xbrl_zip_failure():
    """잘못된 접수번호로 호출 시, 어댑터가 None을 반환하는지 테스트."""
    api_key = os.getenv("DART_API_KEY")
    adapter = DartDownloadAdapter(api_key=api_key)
    invalid_rcept_no = "99999999999999" # 무효 접수번호
    
    zip_bytes = adapter.download_xbrl_zip(invalid_rcept_no)
    
    assert zip_bytes is None, "유효하지 않은 접수번호 다운로드는 None을 반환해야 합니다."

