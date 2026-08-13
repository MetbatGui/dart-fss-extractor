"""SQLite 영속성 데이터 저장소 어댑터."""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from core.domain.models.company import Company
from core.ports.repository_port import RepositoryPort
from infra.adapters.sqlite.schema import initialize_db

logger = logging.getLogger(__name__)


class SqliteRepositoryAdapter(RepositoryPort):
    """SQLite 데이터베이스 영속성 저장소 어댑터 (LSP 준수).

    - 기존 Parquet 파티션 입출력 구조와 100% 동일하게 DataFrame 형태로 데이터를 호환시킵니다.
    - 트랜잭션 ACID를 완벽히 보증합니다.
    """

    def __init__(self, db_path: str = "data/financial_data.db"):
        self.db_path = db_path

        # 인메모리가 아닐 경우 디렉터리 자동 생성
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

        # 테이블 및 인덱스 초기화
        initialize_db(self._conn)
        self._migrate_schema_if_needed()

    def _migrate_schema_if_needed(self) -> None:
        """기존 DB에 settlement_month 컬럼이 없을 경우 자동으로 추가해주는 마이그레이션 방어 로직입니다."""
        cursor = self._conn.cursor()
        try:
            # PRAGMA table_info를 사용하여 컬럼 존재 여부 체크
            cursor.execute("PRAGMA table_info(companies)")
            columns = [row["name"] for row in cursor.fetchall()]
            if "settlement_month" not in columns:
                logger.info(
                    "companies 테이블에 settlement_month 컬럼이 존재하지 않아 추가 마이그레이션을 시작합니다."
                )
                with self._conn:
                    self._conn.execute(
                        "ALTER TABLE companies ADD COLUMN settlement_month INTEGER DEFAULT 12"
                    )
                logger.info(
                    "companies 테이블에 settlement_month 컬럼을 성공적으로 추가했습니다."
                )

            cursor.execute("PRAGMA table_info(financials)")
            fin_columns = [row["name"] for row in cursor.fetchall()]
            if "rcept_no" not in fin_columns:
                logger.info(
                    "financials 테이블에 rcept_no 컬럼이 존재하지 않아 추가 마이그레이션을 시작합니다."
                )
                with self._conn:
                    self._conn.execute(
                        "ALTER TABLE financials ADD COLUMN rcept_no TEXT"
                    )
                logger.info(
                    "financials 테이블에 rcept_no 컬럼을 성공적으로 추가했습니다."
                )
        except Exception as e:
            logger.error(f"스키마 마이그레이션 검사 중 실패: {e}")

    def close(self) -> None:
        """커넥션을 안전하게 닫습니다."""
        if self._conn:
            self._conn.close()

    def save_partition(
        self, dataset_name: str, partition_name: str, df: pd.DataFrame
    ) -> None:
        """특정 기업의 실적 DataFrame 데이터를 SQLite에 적재 (INSERT OR REPLACE).

        df에 'rcept_no'/'is_amendment' 컬럼이 있으면, 정정공시가 아닌데 기존에 저장된 값과
        다른 접수번호가 같은 (연도, 구분, 분기, 구분_상세) 키를 덮어쓰려는 충돌을 감지해
        기존 값을 보존하고 경고 로그만 남긴다 (서로 다른 두 공시가 같은 분기 키로 잘못
        매핑되어 데이터가 조용히 사라지는 것을 방지).
        """
        if df.empty:
            return

        detail_type = "연결"
        if "cfs" in dataset_name.lower():
            detail_type = "연결"
        elif "ofs" in dataset_name.lower():
            detail_type = "개별"

        query = """
        INSERT OR REPLACE INTO financials (
            corp_code, corp_name, year, division, quarter, detail_type, revenue, operating_profit, net_income, rcept_no
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        existing_query = """
        SELECT rcept_no FROM financials
        WHERE corp_code = ? AND year = ? AND division = ? AND quarter = ? AND detail_type = ?
        """

        with self._conn:
            for _, row in df.iterrows():
                # 데이터프레임 내에 '구분_상세' 열이 존재하면 이를 우선 사용
                row_detail = row.get("구분_상세", detail_type)
                year = int(row.get("연도"))
                division = str(row.get("구분"))
                quarter = str(row.get("분기"))
                rcept_no = row.get("rcept_no")
                rcept_no = (
                    str(rcept_no)
                    if rcept_no is not None and not pd.isna(rcept_no)
                    else None
                )
                is_amendment = bool(row.get("is_amendment", False))

                if rcept_no and not is_amendment:
                    cursor = self._conn.execute(
                        existing_query,
                        (partition_name, year, division, quarter, row_detail),
                    )
                    existing = cursor.fetchone()
                    existing_rcept_no = existing["rcept_no"] if existing else None
                    if existing_rcept_no and existing_rcept_no != rcept_no:
                        logger.error(
                            f"[충돌 감지] {partition_name} {year}년 {quarter}({row_detail})에 "
                            f"이미 다른 공시(rcept_no={existing_rcept_no})의 값이 저장되어 있는데, "
                            f"정정이 아닌 별개 공시(rcept_no={rcept_no})가 같은 기간 키로 덮어쓰려 했습니다. "
                            f"기존 값을 보존하고 이번 값은 건너뜁니다."
                        )
                        continue

                # float 결측치 처리 (NaN -> None)
                def clean_val(v):
                    return None if pd.isna(v) else float(v)

                rev_val = clean_val(row.get("매출액"))
                op_val = clean_val(row.get("영업이익"))
                ni_val = clean_val(row.get("당기순이익"))

                self._conn.execute(
                    query,
                    (
                        partition_name,  # corp_code
                        str(row.get("기업명")),
                        year,
                        division,
                        quarter,
                        row_detail,
                        rev_val,
                        op_val,
                        ni_val,
                        rcept_no,
                    ),
                )

                # 실제 공시(rcept_no)에서 온 값만 정정 이력으로 남긴다.
                # (JSON 캐시 재처리 등 rcept_no 없는 배치성 재적재는 실제 정정이 아니므로 제외)
                if rcept_no:
                    self._conn.execute(
                        """
                        INSERT OR IGNORE INTO financials_history (
                            corp_code, corp_name, year, division, quarter, detail_type,
                            revenue, operating_profit, net_income, rcept_no
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            partition_name,
                            str(row.get("기업명")),
                            year,
                            division,
                            quarter,
                            row_detail,
                            rev_val,
                            op_val,
                            ni_val,
                            rcept_no,
                        ),
                    )

    def load_partition(self, dataset_name: str, partition_name: str) -> pd.DataFrame:
        """특정 기업의 적재된 실적 데이터를 판다스 DataFrame으로 가져옵니다."""
        detail_type = "연결"
        if "ofs" in dataset_name.lower():
            detail_type = "개별"

        query = """
        SELECT corp_name AS 기업명, year AS 연도, division AS 구분,
               quarter AS 분기, detail_type AS 구분_상세,
               revenue AS 매출액, operating_profit AS 영업이익, net_income AS 당기순이익,
               rcept_no
        FROM financials
        WHERE corp_code = ? AND detail_type = ?
        ORDER BY year ASC, quarter ASC
        """

        df = pd.read_sql_query(query, self._conn, params=[partition_name, detail_type])
        return df

    def load_history(
        self, corp_code: str, year: int, quarter: str, detail_type: str = "연결"
    ) -> pd.DataFrame:
        """특정 기업/기간에 대해 그동안 수집된 모든 정정 이력(각 rcept_no별 값)을 시간순으로 반환합니다.

        가장 마지막 행(created_at 최댓값)이 현재 financials 테이블의 값과 일치하는 최신본입니다.
        """
        query = """
        SELECT corp_name AS 기업명, year AS 연도, division AS 구분, quarter AS 분기,
               detail_type AS 구분_상세, revenue AS 매출액, operating_profit AS 영업이익,
               net_income AS 당기순이익, rcept_no, created_at
        FROM financials_history
        WHERE corp_code = ? AND year = ? AND quarter = ? AND detail_type = ?
        ORDER BY created_at ASC
        """
        return pd.read_sql_query(
            query, self._conn, params=[corp_code, year, quarter, detail_type]
        )

    def exists(self, dataset_name: str, partition_name: str) -> bool:
        """특정 기업의 실적이 DB 내에 존재하는지 신속 스캔."""
        detail_type = "연결"
        if "ofs" in dataset_name.lower():
            detail_type = "개별"

        query = (
            "SELECT 1 FROM financials WHERE corp_code = ? AND detail_type = ? LIMIT 1"
        )
        cursor = self._conn.cursor()
        cursor.execute(query, (partition_name, detail_type))
        return cursor.fetchone() is not None

    def load_all(self, dataset_name: str) -> pd.DataFrame:
        """데이터셋에 매칭되는 전체 데이터를 통합하여 1개의 DataFrame으로 로드합니다."""
        detail_type = "연결"
        if "ofs" in dataset_name.lower():
            detail_type = "개별"

        query = """
        SELECT corp_code AS 종목코드, corp_name AS 기업명, year AS 연도, division AS 구분,
               quarter AS 분기, detail_type AS 구분_상세,
               revenue AS 매출액, operating_profit AS 영업이익, net_income AS 당기순이익,
               rcept_no
        FROM financials
        WHERE detail_type = ?
        ORDER BY corp_name ASC, year ASC, quarter ASC
        """
        df = pd.read_sql_query(query, self._conn, params=[detail_type])
        return df

    def save_company_metadata(self, company: Company) -> None:
        """기업 상태 및 수집 메타데이터를 저장합니다."""
        success_str = ",".join(map(str, company.success_years))
        failed_str = ",".join(map(str, company.failed_years))

        query = """
        INSERT OR REPLACE INTO companies (corp_code, corp_name, success_years, failed_years, last_updated, settlement_month)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._conn:
            self._conn.execute(
                query,
                (
                    company.code,
                    company.name,
                    success_str if success_str else None,
                    failed_str if failed_str else None,
                    company.last_updated,
                    company.settlement_month,
                ),
            )

    def load_company_metadata(self, code: str) -> Company | None:
        """기업 상태 및 수집 메타데이터를 DB로부터 조회해 복원합니다."""
        query = "SELECT * FROM companies WHERE corp_code = ?"
        cursor = self._conn.cursor()
        cursor.execute(query, (code,))
        row = cursor.fetchone()

        if not row:
            return None

        success_years = []
        if row["success_years"]:
            success_years = [
                int(y) for y in row["success_years"].split(",") if y.strip()
            ]

        failed_years = []
        if row["failed_years"]:
            failed_years = [int(y) for y in row["failed_years"].split(",") if y.strip()]

        # 기존 DB 호환성 보장: settlement_month가 없을 경우 기본값 12
        settlement_month = 12
        try:
            if "settlement_month" in row.keys() and row["settlement_month"] is not None:
                settlement_month = int(row["settlement_month"])
        except Exception:
            pass

        return Company(
            code=row["corp_code"],
            name=row["corp_name"],
            success_years=success_years,
            failed_years=failed_years,
            last_updated=row["last_updated"],
            settlement_month=settlement_month,
        )

    # --- SQLite 고속 확장 기능 ---
    def find_missing_companies(
        self,
        company_codes: list[str],
        year: int,
        quarter: str,
        detail_type: str = "연결",
    ) -> list[str]:
        """특정 분기의 실적 수치(매출액 등)가 누락되어(NaN/Null) 수집이 필요한 기업들의 코드 목록을 스캔합니다."""
        if not company_codes:
            return []

        placeholders = ",".join("?" for _ in company_codes)
        # json_each 호환성 대안을 위한 간결한 SQL IN 절 사용
        query_in = f"""
        SELECT corp_code FROM companies 
        WHERE corp_code IN ({placeholders})
        EXCEPT
        SELECT corp_code FROM financials
        WHERE year = ? AND quarter = ? AND detail_type = ? AND revenue IS NOT NULL
        """

        params = list(company_codes) + [year, quarter, detail_type]
        cursor = self._conn.cursor()
        cursor.execute(query_in, params)
        return [r["corp_code"] for r in cursor.fetchall()]
