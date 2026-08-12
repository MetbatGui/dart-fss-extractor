"""티커 기반 최신 종목명 조회 포트 인터페이스."""

from abc import ABC, abstractmethod


class TickerNamePort(ABC):
    """종목코드(ticker)로 현재 유효한 종목명을 조회하는 포트 인터페이스."""

    @abstractmethod
    def get_name_by_ticker(self, ticker: str) -> str | None:
        """티커 코드를 기반으로 현재 종목명을 조회한다.

        Args:
            ticker: 6자리 KRX 종목코드.

        Returns:
            종목명 문자열 혹은 조회 실패 시 ``None``.
        """
        raise NotImplementedError
