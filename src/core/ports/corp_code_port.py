from abc import ABC, abstractmethod
from typing import Mapping, Optional, Sequence


class CorpCodePort(ABC):
    """기업명 ↔ 기업코드 조회를 위한 포트 인터페이스.

    서비스 레이어는 이 인터페이스에만 의존하고, 실제 구현(어댑터)은
    이 인터페이스를 구현한다. 이를 통해 도메인(서비스)은 어댑터의
    구체적인 동작을 알 필요가 없으며, 의존성 역전 원칙을 만족한다.
    """

    @abstractmethod
    def get_all_mapping(self) -> Mapping[str, str]:
        """전체 기업명‑코드 매핑을 반환한다.

        Returns:
            Mapping[str, str]: {기업명: 기업코드} 형태의 사전.
        """
        raise NotImplementedError

    @abstractmethod
    def get_code(self, company_name: str) -> Optional[str]:
        """단일 기업명의 코드를 조회한다.

        Args:
            company_name: 조회하고자 하는 기업명.

        Returns:
            기업코드 문자열 혹은 존재하지 않을 경우 ``None``.
        """
        raise NotImplementedError

    @abstractmethod
    def get_codes(self, company_names: Sequence[str]) -> list[Optional[str]]:
        """기업명 리스트에 대한 코드 리스트를 반환한다.

        Args:
            company_names: 기업명 시퀀스.

        Returns:
            기업코드 리스트. 매칭되지 않으면 해당 위치에 ``None``.
        """
        raise NotImplementedError
