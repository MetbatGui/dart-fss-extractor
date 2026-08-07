"""DART 공시 원본 다운로드 어댑터."""

import logging
import os
import re
import time

import requests
from requests.exceptions import RequestException

from core.ports.download_port import DownloadPort

logger = logging.getLogger(__name__)


class DartDownloadAdapter(DownloadPort):
    """DART 공시 다운로드 어댑터.

    - HTML 스크래핑을 통해 dcmNo를 획득하고 ifrs.do에서 XBRL ZIP 파일을 직접 다운로드합니다.
    """

    def __init__(
        self,
        api_key: str | None = None,
        timeout: int = 25,
        max_retries: int = 3,
        retry_backoff: float = 2.0,
        request_delay: float = 0.1,
    ):
        """초기화합니다.

        Args:
            api_key: DART API 키 (현재 사용되지 않음, 이전 시그니처 호환용)
            timeout: HTTP 요청 타임아웃 초 (기본값: 25)
            max_retries: 커넥션 오류 시 재시도 최대 횟수 (기본값: 3)
            retry_backoff: 재시도 간 대기 시간(초) 배수, 시도 횟수에 비례해 증가 (기본값: 2.0)
            request_delay: 연속 요청 사이 최소 대기 시간(초). 대량 배치 시 서버의 커넥션 차단을 예방한다 (기본값: 0.3)
        """
        self._api_key = api_key or os.getenv("DART_API_KEY")
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._request_delay = request_delay
        self._session = requests.Session()

    def _get_with_retry(self, url: str, **kwargs) -> requests.Response:
        """커넥션 레벨 오류(RemoteDisconnected 등) 발생 시 지수 백오프로 재시도하는 GET 요청."""
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            if self._request_delay:
                time.sleep(self._request_delay)
            try:
                return self._session.get(url, timeout=self._timeout, **kwargs)
            except RequestException as e:
                last_exc = e
                if attempt < self._max_retries:
                    wait = self._retry_backoff * attempt
                    logger.warning(
                        f"  ⚠️ [재시도 {attempt}/{self._max_retries}] 커넥션 오류로 {wait}초 대기 후 재시도: {e}"
                    )
                    time.sleep(wait)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"요청 실패 (max_retries={self._max_retries}): {url}")

    def _get_dcm_no(self, rcept_no: str) -> str | None:
        """DART 공시 메인 페이지 HTML을 파싱하여 dcmNo를 추출합니다."""
        url = "https://dart.fss.or.kr/dsaf001/main.do"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        try:
            r = self._get_with_retry(url, params={"rcpNo": rcept_no}, headers=headers)
            r.raise_for_status()

            matches = re.findall(r"'dcmNo'\]\s*=\s*\"(\d+)\"", r.text)
            if matches:
                return matches[0]

            logger.warning(f"[DART] dcmNo를 찾을 수 없습니다. (접수번호: {rcept_no})")
            return None
        except Exception as e:
            logger.error(f"[DART] dcmNo 파싱 중 오류 (접수번호: {rcept_no}): {e}")
            return None

    def download_xbrl_zip(self, rcept_no: str) -> bytes | None:
        """HTML 스크래핑 및 ifrs.do를 통해 원본 ZIP 바이너리를 획득합니다."""
        logger.info(
            f"[다운로드] HTML 스크래핑 및 ifrs.do 호출 시도 (접수번호: {rcept_no})"
        )
        try:
            dcm_no = self._get_dcm_no(rcept_no)
            if dcm_no:
                download_url = "https://dart.fss.or.kr/pdf/download/ifrs.do"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Referer": f"https://dart.fss.or.kr/pdf/download/main.do?rcp_no={rcept_no}&dcm_no={dcm_no}",
                }
                params = {"rcp_no": rcept_no, "dcm_no": dcm_no, "lang": "ko"}
                r = self._get_with_retry(download_url, params=params, headers=headers)

                is_zip = r.content.startswith(b"PK\x03\x04")
                if r.status_code == 200 and is_zip:
                    logger.info(
                        f"  -> [다운로드 성공] XBRL ZIP 다운로드 완료 ({len(r.content):,} bytes)"
                    )
                    return r.content
                else:
                    logger.warning(
                        f"  -> [다운로드 실패] ZIP 응답이 아니거나 HTTP 상태코드가 이상함 (코드: {r.status_code})"
                    )
        except Exception as e:
            logger.error(f"  -> [다운로드 예외 발생]: {e}")

        logger.error(f"[다운로드 최종 실패] (접수번호: {rcept_no})")
        return None
