"""공시 캐싱 포트 인터페이스."""

from abc import ABC, abstractmethod
from typing import Dict


class CachePort(ABC):
    """공시 수집 캐시 포트 인터페이스."""

    @abstractmethod
    def load_all(self) -> Dict[str, dict]:
        """전체 캐시 데이터를 메모리에 로드하여 Dict 형태로 반환합니다.
        
        Returns:
            Dict[str, dict]: 접수번호(rcept_no)를 키로 하고 캐시 메타데이터 dict를 값으로 하는 딕셔너리
        """
        raise NotImplementedError

    @abstractmethod
    def save_all(self, cache_data: Dict[str, dict]) -> None:
        """메모리 상의 전체 캐시 Dict 데이터를 파일에 일괄 영속화합니다.
        
        Args:
            cache_data: 일괄 영속화할 캐시 데이터 딕셔너리
        """
        raise NotImplementedError
