import os
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Mapping, Optional, Sequence, List
import requests

from src.core.ports.corp_code_port import CorpCodePort


class CorpCodeAdapter(CorpCodePort):
    """기업명 ↔ 기업코드 매핑 어댑터.

    - 최초 호출 시 DART에서 제공하는 ``corpCode.zip`` 파일을 다운로드하고
      ``CORPCODE.xml`` 을 파싱한다.
    - 파일이 이미 존재하면 재다운로드하지 않는다. ``force_download``
      플래그를 통해 강제 업데이트 가능.
    - 어댑터는 포트 인터페이스만 구현하므로 서비스 레이어는
      구체적인 구현 세부사항을 알 필요가 없다.
    """

    _CACHE_DIR = Path(os.getenv("OUTPUT_DIRECTORY", "./data")).resolve() / "corp_code"
    _ZIP_PATH = _CACHE_DIR / "corpCode.zip"
    _XML_PATH = _CACHE_DIR / "CORPCODE.xml"

    def __init__(self, force_download: bool = False) -> None:
        """생성자.

        Args:
            force_download: ``True``이면 매 호출 시 최신 XML을 다운로드한다.
        """
        self._force_download = force_download
        self._ensure_data()

    # ---------------------------------------------------------------------
    # 내부 헬퍼
    # ---------------------------------------------------------------------
    def _ensure_data(self) -> None:
        """XML 데이터가 없으면 다운로드하고 압축을 푼다.

        다운로드는 환경 변수 ``DART_API_KEY`` 가 필요하다.
        """
        self._CACHE_DIR.mkdir(parents=True, exist_ok=True)
        if self._force_download or not self._XML_PATH.is_file():
            self._download_and_extract()

    def _download_and_extract(self) -> None:
        """DART API 로부터 ``corpCode.zip`` 을 받아 압축을 푼다."""
        api_key = os.getenv("DART_API_KEY")
        if not api_key:
            raise EnvironmentError("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        self._ZIP_PATH.write_bytes(response.content)
        with zipfile.ZipFile(self._ZIP_PATH, "r") as z:
            # zip 안에 CORPCODE.xml 이 하나만 존재한다.
            z.extractall(self._CACHE_DIR)
        if not self._XML_PATH.is_file():
            raise FileNotFoundError("압축 해제 후 CORPCODE.xml 파일을 찾을 수 없습니다.")

    def _load_mapping(self) -> Mapping[str, str]:
        """XML 파일을 파싱해 ``{기업명: 기업코드}`` 사전을 만든다."""
        tree = ET.parse(self._XML_PATH)
        root = tree.getroot()
        mapping: dict[str, str] = {}
        for corp in root.findall("./list"):
            name = corp.findtext("corp_name")
            code = corp.findtext("corp_code")
            if name and code:
                mapping[name] = code
        return mapping

    # ---------------------------------------------------------------------
    # CorpCodePort 구현
    # ---------------------------------------------------------------------
    def get_all_mapping(self) -> Mapping[str, str]:
        """전체 기업명‑코드 매핑을 반환한다."""
        return self._load_mapping()

    def get_code(self, company_name: str) -> Optional[str]:
        """단일 기업명의 코드를 조회한다.

        Args:
            company_name: 조회 대상 기업명.

        Returns:
            기업코드 문자열 혹은 매핑이 없을 경우 ``None``.
        """
        return self._load_mapping().get(company_name)

    def get_codes(self, company_names: Sequence[str]) -> List[Optional[str]]:
        """기업명 리스트에 대한 코드 리스트를 반환한다.

        Args:
            company_names: 기업명 시퀀스.

        Returns:
            각 기업명에 대응하는 코드 리스트. 매칭되지 않으면 ``None``.
        """
        mapping = self._load_mapping()
        return [mapping.get(name) for name in company_names]
