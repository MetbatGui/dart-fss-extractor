"""SQLite 데이터베이스 스키마 및 DDL 정의."""

CREATE_COMPANIES_TABLE = """
CREATE TABLE IF NOT EXISTS companies (
    corp_code TEXT PRIMARY KEY,
    corp_name TEXT UNIQUE NOT NULL,
    success_years TEXT,   -- 콤마 분리된 연도 리스트 (예: "2024,2025")
    failed_years TEXT,    -- 콤마 분리된 연도 리스트 (예: "2023")
    last_updated TEXT,
    settlement_month INTEGER DEFAULT 12
);
"""

CREATE_FINANCIALS_TABLE = """
CREATE TABLE IF NOT EXISTS financials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corp_code TEXT NOT NULL,
    corp_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    division TEXT NOT NULL,       -- "분기", "연간"
    quarter TEXT NOT NULL,        -- "1Q", "2Q", "3Q", "4Q", "연간"
    detail_type TEXT NOT NULL,    -- "연결", "개별"
    revenue REAL,                 -- 매출액 (원 단위 또는 정밀도 보존용 실수)
    operating_profit REAL,        -- 영업이익
    net_income REAL,              -- 당기순이익
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(corp_code) REFERENCES companies(corp_code)
);
"""

CREATE_FINANCIALS_UNIQUE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS uidx_financials 
ON financials (corp_code, year, division, quarter, detail_type);
"""


def initialize_db(conn) -> None:
    """데이터베이스 커넥션을 받아 스키마 및 인덱스를 안전하게 초기화합니다."""
    with conn:
        conn.execute(CREATE_COMPANIES_TABLE)
        conn.execute(CREATE_FINANCIALS_TABLE)
        conn.execute(CREATE_FINANCIALS_UNIQUE_INDEX)
