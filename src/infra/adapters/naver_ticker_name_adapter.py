"""네이버 모바일 주식 검색 API 기반 티커→종목명 조회 어댑터."""

import logging
import re

import requests

from core.ports.ticker_name_port import TickerNamePort

logger = logging.getLogger(__name__)


class NaverTickerNameAdapter(TickerNamePort):
    """네이버 모바일 주식 검색 자동완성 API로 티커의 현재 종목명을 조회한다."""

    BASE_URL = "https://m.stock.naver.com/front-api/search/autoComplete"

    def get_name_by_ticker(self, ticker: str) -> str | None:
        """티커로 네이버 검색을 호출해 현재 종목명을 조회한다."""
        params = {"query": ticker, "target": "stock"}
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            "Referer": "https://m.stock.naver.com/search",
        }
        try:
            response = requests.get(
                self.BASE_URL, params=params, headers=headers, timeout=5
            )
            response.raise_for_status()
            data = response.json()

            items = data.get("result", {}).get("items", [])
            for item in items:
                if item.get("code") != ticker:
                    continue
                name = item.get("name")
                if name:
                    return re.sub(r"<[^>]*>", "", name)
            return None
        except Exception as e:
            logger.warning(f"[NaverTickerName] 조회 실패 (ticker={ticker}): {e}")
            return None
