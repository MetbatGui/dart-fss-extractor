"""비12월 결산법인 분기 데이터 캘린더 기준 정정 마이그레이션 스크립트.

- 안전 장치: 자동 DB 백업, Dry-Run 모드(기본 활성화), 트랜잭션 롤백 보장, 중복 키 해결
"""

import os
import sys
import shutil
import sqlite3
import argparse
import logging
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DB_Migration")


def get_acc_mt_from_dart(api_key: str, corp_code: str) -> int:
    """DART 기업개황 API를 통해 결산월을 조회합니다."""
    url = "https://opendart.fss.or.kr/api/company.json"
    params = {"crtfc_key": api_key, "corp_code": corp_code}
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data.get("status") == "000":
            acc_mt = data.get("acc_mt", "12")
            return int(acc_mt)
    except Exception as e:
        logger.error(f"DART API 호출 실패 ({corp_code}): {e}")
    return 12


def backup_database(db_path: str) -> str:
    """데이터베이스 파일을 백업합니다."""
    db_file = Path(db_path)
    if not db_file.exists():
        logger.error(f"백업할 데이터베이스 파일이 없습니다: {db_path}")
        sys.exit(1)
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = db_file.with_name(f"{db_file.stem}_backup_{timestamp}.db")
    shutil.copy2(db_file, backup_file)
    logger.info(f"📦 DB 백업 완료: {backup_file}")
    return str(backup_file)


def get_correct_calendar_period(fiscal_year: int, fiscal_quarter: str, settlement_month: int) -> tuple[int, str]:
    """DART 회기 분기를 캘린더 기준의 연도/분기로 변환합니다.
    
    공식:
    - calendar_month = (settlement_month + quarter_num * 3) % 12 (0이면 12)
    - calendar_year = fiscal_year - 1 (calendar_month > settlement_month 일 때) else fiscal_year
    """
    if settlement_month == 12:
        return fiscal_year, fiscal_quarter

    try:
        quarter_num = int(fiscal_quarter[0])  # "1Q" -> 1
    except Exception:
        return fiscal_year, fiscal_quarter

    calendar_month = (settlement_month + quarter_num * 3) % 12
    if calendar_month == 0:
        calendar_month = 12

    calendar_quarter = f"{calendar_month // 3}Q"
    
    if calendar_month > settlement_month:
        calendar_year = fiscal_year - 1
    else:
        calendar_year = fiscal_year

    return calendar_year, calendar_quarter


def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
        
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="비12월 결산법인 캘린더 분기 데이터 정정 마이그레이션")
    parser.add_argument("--commit", action="store_true", help="실제 변경사항을 데이터베이스에 반영하고 커밋합니다.")
    parser.add_argument("--db", type=str, default="data/financial_data.db", help="SQLite DB 파일 경로")
    args = parser.parse_args()

    db_path = args.db
    is_dry_run = not args.commit

    if is_dry_run:
        logger.info("🛡️  [DRY-RUN MODE] 모의 실행 중입니다. 데이터베이스가 수정되지 않습니다.")
    else:
        logger.warning("⚠️  [LIVE MODE] 실제 데이터 정정을 수행합니다!")
        # 백업 수행
        backup_database(db_path)

    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 환경 변수가 없습니다.")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # 1. DB의 전체 financials에서 분기 구분 데이터를 조회하여 의심 기업 색별
    df_db = pd.read_sql_query(
        "SELECT id, corp_code, corp_name, year, quarter, division, detail_type, revenue, operating_profit, net_income FROM financials", 
        conn
    )
    
    if df_db.empty:
        logger.info("수정할 실적 데이터가 없습니다.")
        conn.close()
        return

    # 의심 기업 색별 (2026년 2Q/3Q가 존재하거나 비연속 분기 구조를 갖는 기업)
    df_quarter_only = df_db[df_db["division"] == "분기"]
    suspicious_codes = set()
    
    df_2026_anomalies = df_quarter_only[(df_quarter_only["year"] == 2026) & (df_quarter_only["quarter"].isin(["2Q", "3Q"]))]
    suspicious_codes.update(df_2026_anomalies["corp_code"].unique())
    
    for (corp_code, corp_name, year), grp in df_quarter_only.groupby(["corp_code", "corp_name", "year"]):
        quarters = set(grp["quarter"].tolist())
        if ("2Q" in quarters and "1Q" not in quarters) or \
           ("3Q" in quarters and "2Q" not in quarters) or \
           ("4Q" in quarters and "3Q" not in quarters):
            suspicious_codes.add(corp_code)

    logger.info(f"의심 기업 {len(suspicious_codes)}개 식별 완료. 결산월 DART 조회 시작...")

    # 2. 의심 기업의 DART 개황 API 조회를 통해 실제 결산월 정보 파악 및 companies 테이블 메타데이터 로드
    settlement_months = {}
    
    # DB에 기존 등록된 결산월이 있는지 확인
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(companies)")
        columns = [row["name"] for row in cursor.fetchall()]
        has_settlement_column = "settlement_month" in columns
    except Exception:
        has_settlement_column = False

    for i, code in enumerate(suspicious_codes, 1):
        # DB에 이미 저장된 유효한 결산월이 있는지 먼저 체크
        acc_mt = None
        if has_settlement_column:
            cursor.execute("SELECT settlement_month FROM companies WHERE corp_code = ?", (code,))
            row = cursor.fetchone()
            if row and row["settlement_month"] is not None and row["settlement_month"] != 12:
                acc_mt = int(row["settlement_month"])
        
        if acc_mt is None:
            acc_mt = get_acc_mt_from_dart(api_key, code)
            
        settlement_months[code] = acc_mt
        if i % 20 == 0 or i == len(suspicious_codes):
            logger.info(f"결산월 스캔: {i}/{len(suspicious_codes)} 완료...")

    # 3. 비12월 결산법인 필터링
    non_12_corps = {code: m for code, m in settlement_months.items() if m != 12}
    logger.info(f"🎯 최종 비12월 결산 법인 {len(non_12_corps)}개 발견!")
    for code, m in non_12_corps.items():
        corp_name = df_db[df_db["corp_code"] == code]["corp_name"].iloc[0]
        logger.info(f"  - [{corp_name}] 코드: {code}, 결산월: {m}월")

    if not non_12_corps:
        logger.info("정정이 필요한 비12월 결산 법인이 발견되지 않았습니다. 마이그레이션을 조기 종료합니다.")
        conn.close()
        return

    # 4. 정정 데이터 셋 생성
    mismatch_records = []
    
    for code, m in non_12_corps.items():
        # 해당 기업의 분기(division='분기') 실적만 필터링
        df_corp = df_quarter_only[df_quarter_only["corp_code"] == code]
        for _, row in df_corp.iterrows():
            f_year = int(row["year"])
            f_quarter = str(row["quarter"])
            
            c_year, c_quarter = get_correct_calendar_period(f_year, f_quarter, m)
            
            if f_year != c_year or f_quarter != c_quarter:
                mismatch_records.append({
                    "id": row["id"],
                    "corp_code": code,
                    "corp_name": row["corp_name"],
                    "old_year": f_year,
                    "old_quarter": f_quarter,
                    "new_year": c_year,
                    "new_quarter": c_quarter,
                    "detail_type": row["detail_type"],
                    "revenue": row["revenue"],
                    "operating_profit": row["operating_profit"],
                    "net_income": row["net_income"]
                })

    logger.info(f"정정 대상 분기 레코드 수: {len(mismatch_records)}개")

    if not mismatch_records:
        logger.info("정정이 필요한 불일치 데이터가 없습니다.")
        conn.close()
        return

    # 5. 모의 실행 출력 (Dry-Run)
    if is_dry_run:
        logger.info("\n=== [MIGRATION DRY-RUN REPORT] ===")
        df_mismatch = pd.DataFrame(mismatch_records)
        sample = df_mismatch[["corp_name", "old_year", "old_quarter", "new_year", "new_quarter", "detail_type"]].head(30)
        logger.info("보정될 데이터 샘플 (최대 30개):")
        logger.info("\n" + sample.to_string(index=False))
        logger.info(f"\n총 {len(mismatch_records)}건의 데이터가 정정될 예정입니다.")
        logger.info("데이터베이스를 실제로 변경하려면 '--commit' 옵션을 지정하여 실행하십시오.")
        conn.close()
        return

    # 6. 실제 데이터베이스 마이그레이션 실행 (트랜잭션 수호)
    logger.info("실제 데이터베이스 정정 작업 시작...")
    
    try:
        with conn:
            # 6-1. companies 테이블 결산월 정보 업데이트
            if has_settlement_column:
                logger.info("companies 테이블에 기업별 결산월 메타데이터 일괄 업데이트 중...")
                for code, m in settlement_months.items():
                    conn.execute(
                        "UPDATE companies SET settlement_month = ? WHERE corp_code = ?", 
                        (m, code)
                    )

            # 6-2. financials 테이블 내 27개 비12월 결산 법인의 분기 실적 정정
            # 중복 키(Unique Index) 제약 충돌을 안전하게 방지하기 위해:
            # 1) 대상 기업의 '분기' 실적을 임시 백업/로드 후 삭제
            # 2) 캘린더 기준으로 보정된 데이터로 INSERT OR REPLACE 수행
            for code, m in non_12_corps.items():
                corp_name = df_db[df_db["corp_code"] == code]["corp_name"].iloc[0]
                
                # 기존 해당 기업의 분기 실적을 전부 로드
                cursor.execute(
                    "SELECT corp_name, division, detail_type, revenue, operating_profit, net_income, year, quarter "
                    "FROM financials WHERE corp_code = ? AND division = '분기'", 
                    (code,)
                )
                rows = cursor.fetchall()
                
                if not rows:
                    continue

                # 캘린더 분기로 변환된 행 리스트 생성
                new_rows = []
                for r in rows:
                    f_year = r["year"]
                    f_quarter = r["quarter"]
                    c_year, c_quarter = get_correct_calendar_period(f_year, f_quarter, m)
                    
                    new_rows.append((
                        code,
                        r["corp_name"],
                        c_year,
                        r["division"],
                        c_quarter,
                        r["detail_type"],
                        r["revenue"],
                        r["operating_profit"],
                        r["net_income"]
                    ))

                # 기존 분기 실적 데이터 삭제 (충돌 우려 제거)
                conn.execute("DELETE FROM financials WHERE corp_code = ? AND division = '분기'", (code,))
                
                # 정정된 데이터 삽입 (INSERT OR REPLACE로 UNIQUE 제약 조건 수호)
                insert_query = """
                INSERT OR REPLACE INTO financials (
                    corp_code, corp_name, year, division, quarter, detail_type, revenue, operating_profit, net_income
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                conn.executemany(insert_query, new_rows)
                logger.info(f"  - [{corp_name}] 실적 정정 완료 (처리 레코드: {len(new_rows)}건)")

        logger.info("🎉 데이터베이스 마이그레이션이 최종 성공적으로 반영 및 커밋되었습니다!")
        
    except Exception as err:
        logger.error(f"❌ 마이그레이션 도중 치명적인 오류가 발생하여 전체 작업이 롤백(Rollback)되었습니다: {err}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
