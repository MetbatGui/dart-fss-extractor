"""공시 파일 다운로드 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Optional


class DownloadPort(ABC):
    """공시 파일 다운로드 포트."""

    @abstractmethod
    def download_xbrl_zip(self, rcept_no: str) -> Optional[bytes]:
        """접수번호에 매칭되는 공시 원본(XBRL ZIP) 바이너리를 다운로드합니다.
        
        Args:
            rcept_no: 공시 접수번호 (rcpno)
            
        Returns:
            ZIP 파일 bytes 또는 다운로드 실패 시 None
        """
        raise NotImplementedError
