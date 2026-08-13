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
    rcept_no TEXT,                -- 이 값의 출처 공시 접수번호 (원본 추적/충돌 판별용)
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(corp_code) REFERENCES companies(corp_code)
);
"""

CREATE_FINANCIALS_UNIQUE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS uidx_financials
ON financials (corp_code, year, division, quarter, detail_type);
"""

# financials는 기간별 "최신값 1건"만 유지(덮어쓰기)하므로, 정정공시로 값이 바뀌기 전
# 이전 공시(A)의 값이 사라진다. 정정 이력을 보존하기 위한 append-only 로그 테이블.
CREATE_FINANCIALS_HISTORY_TABLE = """
CREATE TABLE IF NOT EXISTS financials_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corp_code TEXT NOT NULL,
    corp_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    division TEXT NOT NULL,
    quarter TEXT NOT NULL,
    detail_type TEXT NOT NULL,
    revenue REAL,
    operating_profit REAL,
    net_income REAL,
    rcept_no TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

# 같은 공시(rcept_no)를 재수집(cache_only 재처리 등)해도 값이 동일하면 중복 기록하지 않기 위한 dedup 인덱스.
CREATE_FINANCIALS_HISTORY_UNIQUE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS uidx_financials_history
ON financials_history (corp_code, year, division, quarter, detail_type, rcept_no, revenue, operating_profit, net_income);
"""

CREATE_TICKER_NAMES_TABLE = """
CREATE TABLE IF NOT EXISTS ticker_names (
    corp_code TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,      -- KRX 종목코드
    name TEXT NOT NULL,        -- 네이버 기준 현재 종목명
    updated_at TEXT
);
"""


def initialize_db(conn) -> None:
    """데이터베이스 커넥션을 받아 스키마 및 인덱스를 안전하게 초기화합니다."""
    with conn:
        conn.execute(CREATE_COMPANIES_TABLE)
        conn.execute(CREATE_FINANCIALS_TABLE)
        conn.execute(CREATE_FINANCIALS_UNIQUE_INDEX)
        conn.execute(CREATE_FINANCIALS_HISTORY_TABLE)
        conn.execute(CREATE_FINANCIALS_HISTORY_UNIQUE_INDEX)
        conn.execute(CREATE_TICKER_NAMES_TABLE)
