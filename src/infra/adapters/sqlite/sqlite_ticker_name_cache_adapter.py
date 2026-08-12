"""SQLite 기반 티커별 종목명 캐시 어댑터 (ticker_names 테이블)."""

import sqlite3

from core.ports.cache_port import CachePort


class SqliteTickerNameCacheAdapter(CachePort):
    """DailyCollectionService의 티커명 캐시를 ticker_names 테이블에 영속화한다.

    financial_data.db와 동일한 DB 파일을 공유해 별도 파일 관리가 필요 없다.
    """

    def __init__(self, db_path: str = "data/financial_data.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticker_names (
                corp_code TEXT PRIMARY KEY,
                ticker TEXT NOT NULL,
                name TEXT NOT NULL,
                updated_at TEXT
            )
            """
        )
        self._conn.commit()

    def load_all(self) -> dict[str, dict]:
        """corp_code를 키로 하는 {ticker, name, updated_at} 딕셔너리를 반환."""
        cursor = self._conn.execute(
            "SELECT corp_code, ticker, name, updated_at FROM ticker_names"
        )
        return {
            row["corp_code"]: {
                "ticker": row["ticker"],
                "name": row["name"],
                "updated_at": row["updated_at"],
            }
            for row in cursor.fetchall()
        }

    def save_all(self, cache_data: dict[str, dict]) -> None:
        """전체 딕셔너리를 ticker_names 테이블에 일괄 upsert."""
        with self._conn:
            for corp_code, entry in cache_data.items():
                self._conn.execute(
                    """
                    INSERT INTO ticker_names (corp_code, ticker, name, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(corp_code) DO UPDATE SET
                        ticker = excluded.ticker,
                        name = excluded.name,
                        updated_at = excluded.updated_at
                    """,
                    (
                        corp_code,
                        entry.get("ticker"),
                        entry.get("name"),
                        entry.get("updated_at"),
                    ),
                )
