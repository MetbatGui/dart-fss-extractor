import os
import time
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import requests

from core.ports.corp_code_port import CorpCodePort


class CorpCodeAdapter(CorpCodePort):
    """기업명 ↔ 기업코드 매핑 어댑터.

    - 최초 호출 시 DART에서 제공하는 ``corpCode.zip`` 파일을 다운로드하고
      ``CORPCODE.xml`` 을 파싱한다.
    - 파일이 이미 존재하면 재다운로드하지 않는다. ``force_download``
      플래그를 통해 강제 업데이트 가능.
    - 어댑터는 포트 인터페이스만 구현하므로 서비스 레이어는
      구체적인 구현 세부사항을 알 필요가 없다.
    """

    def __init__(
        self,
        force_download: bool = False,
        target_companies_path: str = "data/target_companies.csv",
        cache_dir: str | None = None,
        max_age_days: int = 30,
    ) -> None:
        """생성자.

        Args:
            force_download: ``True``이면 매 호출 시 최신 XML을 다운로드한다.
            target_companies_path: 타겟 기업 목록 파일 경로 (캐시 비교용)
            cache_dir: 캐시 저장 경로 (None이면 OUTPUT_DIRECTORY 환경변수 기반 기본 경로 사용)
            max_age_days: 캐시 파일이 이보다 오래되면 재다운로드한다 (기본 30일, 월 1회 주기 갱신)
        """
        if cache_dir:
            self._CACHE_DIR = Path(cache_dir).resolve()
        else:
            self._CACHE_DIR = (
                Path(os.getenv("OUTPUT_DIRECTORY", "./data")).resolve() / "corp_code"
            )
        self._ZIP_PATH = self._CACHE_DIR / "corpCode.zip"
        self._XML_PATH = self._CACHE_DIR / "CORPCODE.xml"

        self._force_download = force_download
        self._target_companies_path = target_companies_path
        self._max_age_days = max_age_days
        self._ensure_data()

    # ---------------------------------------------------------------------
    # 내부 헬퍼
    # ---------------------------------------------------------------------
    def _ensure_data(self) -> None:
        """XML 데이터가 없으면 다운로드하고 압축을 푼다.

        다운로드는 환경 변수 ``DART_API_KEY`` 가 필요하다.
        파일이 존재하더라도 크기가 너무 작으면(1KB 미만) 손상된 것으로 간주하고 재다운로드한다.
        타겟 컴퍼니 파일의 수정 시각이 캐시보다 더 최신이면 강제로 재다운로드한다.
        """
        self._CACHE_DIR.mkdir(parents=True, exist_ok=True)

        should_download = self._force_download

        if not self._XML_PATH.is_file() or self._XML_PATH.stat().st_size < 1024:
            should_download = True

        # 타겟 컴퍼니 파일의 수정 시각이 캐시보다 더 최신이면 재다운로드 트리거
        if not should_download and self._XML_PATH.is_file():
            target_path = Path(self._target_companies_path)
            if target_path.is_file():
                target_mtime = target_path.stat().st_mtime
                cache_mtime = self._XML_PATH.stat().st_mtime
                if target_mtime > cache_mtime:
                    import logging

                    logger = logging.getLogger(__name__)
                    logger.info(
                        f"[캐시 무효화] 타겟 파일({target_path.name})의 변경 시각이 "
                        f"캐시({self._XML_PATH.name})보다 최신입니다. 고유번호를 재다운로드합니다."
                    )
                    should_download = True

        # 캐시가 max_age_days보다 오래되면 재다운로드 (사명변경/신규상장 등 주기적 반영)
        if not should_download and self._XML_PATH.is_file():
            age_days = (time.time() - self._XML_PATH.stat().st_mtime) / 86400
            if age_days > self._max_age_days:
                import logging

                logger = logging.getLogger(__name__)
                logger.info(
                    f"[캐시 무효화] 고유번호 캐시가 {age_days:.1f}일 경과하여 "
                    f"({self._max_age_days}일 초과) 재다운로드합니다."
                )
                should_download = True

        if should_download:
            self._download_and_extract()

    def _download_and_extract(self) -> None:
        """DART API 로부터 ``corpCode.zip`` 을 받아 압축을 푼다."""
        api_key = os.getenv("DART_API_KEY")
        if not api_key:
            raise OSError("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # 응답 크기 검증 (너무 작으면 에러)
        if len(response.content) < 1024:
            raise ValueError(
                f"다운로드된 파일 크기가 너무 작습니다 ({len(response.content)} bytes). API 키나 요청을 확인하세요."
            )

        self._ZIP_PATH.write_bytes(response.content)
        with zipfile.ZipFile(self._ZIP_PATH, "r") as z:
            # zip 안에 CORPCODE.xml 이 하나만 존재한다.
            z.extractall(self._CACHE_DIR)

        if not self._XML_PATH.is_file():
            raise FileNotFoundError(
                "압축 해제 후 CORPCODE.xml 파일을 찾을 수 없습니다."
            )

        # 압축 해제 후 크기 재검증
        if self._XML_PATH.stat().st_size < 1024:
            raise ValueError(
                f"압축 해제된 XML 파일 크기가 너무 작습니다 ({self._XML_PATH.stat().st_size} bytes)."
            )

    def _load_mapping(self, only_listed: bool = False) -> Mapping[str, str]:
        """XML 파일을 파싱해 ``{기업명: 기업코드}`` 사전을 만든다.

        단, 로컬 매핑 데이터인 'data/corps.csv'가 존재하는 경우 정합성을 위해
        이를 우선적으로 로드하여 사명 매칭 불일치를 원천 방어합니다.
        """
        csv_path = Path("data/corps.csv")
        if csv_path.is_file():
            mapping: dict[str, str] = {}
            try:
                with csv_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        parts = line.strip().split(",")
                        if len(parts) == 2:
                            name, code = parts
                            mapping[name.strip()] = code.strip()
                if mapping:
                    return mapping
            except Exception:
                # 에러 시 원래 DART XML 파싱으로 Fallback
                pass

        tree = ET.parse(self._XML_PATH)
        root = tree.getroot()
        mapping: dict[str, str] = {}
        listed_names: set[str] = set()
        for corp in root.findall("./list"):
            name = corp.findtext("corp_name")
            code = corp.findtext("corp_code")
            stock_code = corp.findtext("stock_code")

            if not (name and code):
                continue

            if only_listed:
                if stock_code and stock_code.strip():
                    mapping[name] = code
                continue

            # 동명이인(같은 이름의 비상장 기타법인과 상장사)이 있을 경우,
            # 상장사(stock_code 보유)를 우선한다. 한 번 상장사로 확정된 이름은
            # 이후 비상장 동명이인이 나와도 덮어쓰지 않는다.
            is_listed = bool(stock_code and stock_code.strip())
            if name not in mapping or (is_listed and name not in listed_names):
                mapping[name] = code
                if is_listed:
                    listed_names.add(name)
        return mapping

    def get_code(self, company_name: str) -> str | None:
        """단일 기업명의 코드를 조회한다.

        Args:
            company_name: 조회 대상 기업명.

        Returns:
            기업코드 문자열 혹은 매핑이 없을 경우 ``None``.
        """
        return self._load_mapping().get(company_name)

    def get_codes(self, company_names: Sequence[str]) -> list[str | None]:
        """기업명 리스트에 대한 코드 리스트를 반환한다.

        Args:
            company_names: 기업명 시퀀스.

        Returns:
            각 기업명에 대응하는 코드 리스트. 매칭되지 않으면 ``None``.
        """
        mapping = self._load_mapping()
        return [mapping.get(name) for name in company_names]
