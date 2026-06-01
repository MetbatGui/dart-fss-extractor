"""SQLite 영속성 데이터 저장소 어댑터."""

import sqlite3
import logging
from pathlib import Path
from typing import Optional, List
import pandas as pd

from core.ports.repository_port import RepositoryPort
from core.domain.models.company import Company
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

    def close(self) -> None:
        """커넥션을 안전하게 닫습니다."""
        if self._conn:
            self._conn.close()

    def save_partition(self, dataset_name: str, partition_name: str, df: pd.DataFrame) -> None:
        """특정 기업의 실적 DataFrame 데이터를 SQLite에 적재 (INSERT OR REPLACE)."""
        if df.empty:
            return

        detail_type = "연결"
        if "cfs" in dataset_name.lower():
            detail_type = "연결"
        elif "ofs" in dataset_name.lower():
            detail_type = "개별"

        query = """
        INSERT OR REPLACE INTO financials (
            corp_code, corp_name, year, division, quarter, detail_type, revenue, operating_profit, net_income
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with self._conn:
            for _, row in df.iterrows():
                # 데이터프레임 내에 '구분_상세' 열이 존재하면 이를 우선 사용
                row_detail = row.get("구분_상세", detail_type)
                
                # float 결측치 처리 (NaN -> None)
                def clean_val(v):
                    return None if pd.isna(v) else float(v)

                self._conn.execute(query, (
                    partition_name,                    # corp_code
                    str(row.get("기업명")),
                    int(row.get("연도")),
                    str(row.get("구분")),
                    str(row.get("분기")),
                    row_detail,
                    clean_val(row.get("매출액")),
                    clean_val(row.get("영업이익")),
                    clean_val(row.get("당기순이익"))
                ))

    def load_partition(self, dataset_name: str, partition_name: str) -> pd.DataFrame:
        """특정 기업의 적재된 실적 데이터를 판다스 DataFrame으로 가져옵니다."""
        detail_type = "연결"
        if "ofs" in dataset_name.lower():
            detail_type = "개별"

        query = """
        SELECT corp_name AS 기업명, year AS 연도, division AS 구분, 
               quarter AS 분기, detail_type AS 구분_상세, 
               revenue AS 매출액, operating_profit AS 영업이익, net_income AS 당기순이익
        FROM financials
        WHERE corp_code = ? AND detail_type = ?
        ORDER BY year ASC, quarter ASC
        """
        
        df = pd.read_sql_query(query, self._conn, params=[partition_name, detail_type])
        return df

    def exists(self, dataset_name: str, partition_name: str) -> bool:
        """특정 기업의 실적이 DB 내에 존재하는지 신속 스캔."""
        detail_type = "연결"
        if "ofs" in dataset_name.lower():
            detail_type = "개별"

        query = "SELECT 1 FROM financials WHERE corp_code = ? AND detail_type = ? LIMIT 1"
        cursor = self._conn.cursor()
        cursor.execute(query, (partition_name, detail_type))
        return cursor.fetchone() is not None

    def load_all(self, dataset_name: str) -> pd.DataFrame:
        """데이터셋에 매칭되는 전체 데이터를 통합하여 1개의 DataFrame으로 로드합니다."""
        detail_type = "연결"
        if "ofs" in dataset_name.lower():
            detail_type = "개별"

        query = """
        SELECT corp_name AS 기업명, year AS 연도, division AS 구분, 
               quarter AS 분기, detail_type AS 구분_상세, 
               revenue AS 매출액, operating_profit AS 영업이익, net_income AS 당기순이익
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
        INSERT OR REPLACE INTO companies (corp_code, corp_name, success_years, failed_years, last_updated)
        VALUES (?, ?, ?, ?, ?)
        """
        with self._conn:
            self._conn.execute(query, (
                company.code,
                company.name,
                success_str if success_str else None,
                failed_str if failed_str else None,
                company.last_updated
            ))

    def load_company_metadata(self, code: str) -> Optional[Company]:
        """기업 상태 및 수집 메타데이터를 DB로부터 조회해 복원합니다."""
        query = "SELECT * FROM companies WHERE corp_code = ?"
        cursor = self._conn.cursor()
        cursor.execute(query, (code,))
        row = cursor.fetchone()
        
        if not row:
            return None
            
        success_years = []
        if row["success_years"]:
            success_years = [int(y) for y in row["success_years"].split(",") if y.strip()]

        failed_years = []
        if row["failed_years"]:
            failed_years = [int(y) for y in row["failed_years"].split(",") if y.strip()]

        return Company(
            code=row["corp_code"],
            name=row["corp_name"],
            success_years=success_years,
            failed_years=failed_years,
            last_updated=row["last_updated"]
        )

    # --- SQLite 고속 확장 기능 ---
    def find_missing_companies(self, company_codes: List[str], year: int, quarter: str, detail_type: str = "연결") -> List[str]:
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
