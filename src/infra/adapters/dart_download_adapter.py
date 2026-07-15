"""DART 공시 원본 다운로드 어댑터."""

import re
import os
import logging
from typing import Optional
import requests

from core.ports.download_port import DownloadPort

logger = logging.getLogger(__name__)


class DartDownloadAdapter(DownloadPort):
    """DART 공시 다운로드 어댑터.
    
    - HTML 스크래핑을 통해 dcmNo를 획득하고 ifrs.do에서 XBRL ZIP 파일을 직접 다운로드합니다.
    """

    def __init__(self, api_key: Optional[str] = None, timeout: int = 25):
        """초기화합니다.
        
        Args:
            api_key: DART API 키 (현재 사용되지 않음, 이전 시그니처 호환용)
            timeout: HTTP 요청 타임아웃 초 (기본값: 25)
        """
        self._api_key = api_key or os.getenv("DART_API_KEY")
        self._timeout = timeout
        self._session = requests.Session()

    def _get_dcm_no(self, rcept_no: str) -> Optional[str]:
        """DART 공시 메인 페이지 HTML을 파싱하여 dcmNo를 추출합니다."""
        url = "https://dart.fss.or.kr/dsaf001/main.do"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        try:
            r = self._session.get(url, params={"rcpNo": rcept_no}, headers=headers, timeout=self._timeout)
            r.raise_for_status()
            
            matches = re.findall(r"'dcmNo'\]\s*=\s*\"(\d+)\"", r.text)
            if matches:
                return matches[0]
                
            logger.warning(f"[DART] dcmNo를 찾을 수 없습니다. (접수번호: {rcept_no})")
            return None
        except Exception as e:
            logger.error(f"[DART] dcmNo 파싱 중 오류 (접수번호: {rcept_no}): {e}")
            return None

    def download_xbrl_zip(self, rcept_no: str) -> Optional[bytes]:
        """HTML 스크래핑 및 ifrs.do를 통해 원본 ZIP 바이너리를 획득합니다."""
        logger.info(f"[다운로드] HTML 스크래핑 및 ifrs.do 호출 시도 (접수번호: {rcept_no})")
        try:
            dcm_no = self._get_dcm_no(rcept_no)
            if dcm_no:
                download_url = "https://dart.fss.or.kr/pdf/download/ifrs.do"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Referer": f"https://dart.fss.or.kr/pdf/download/main.do?rcp_no={rcept_no}&dcm_no={dcm_no}"
                }
                params = {"rcp_no": rcept_no, "dcm_no": dcm_no, "lang": "ko"}
                r = self._session.get(download_url, params=params, headers=headers, timeout=self._timeout)
                
                is_zip = r.content.startswith(b'PK\x03\x04')
                if r.status_code == 200 and is_zip:
                    logger.info(f"  -> [다운로드 성공] XBRL ZIP 다운로드 완료 ({len(r.content):,} bytes)")
                    return r.content
                else:
                    logger.warning(f"  -> [다운로드 실패] ZIP 응답이 아니거나 HTTP 상태코드가 이상함 (코드: {r.status_code})")
        except Exception as e:
            logger.error(f"  -> [다운로드 예외 발생]: {e}")

        logger.error(f"[다운로드 최종 실패] (접수번호: {rcept_no})")
        return None
