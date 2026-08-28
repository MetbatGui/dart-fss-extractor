# 📊 DART FSS Extractor

DART(전자공시시스템)에서 상장사의 **재무제표**(XBRL/API)를 수집해 SQLite DB(SSOT)에
쌓고, 기업×연도×계정과목 피벗 형태의 엑셀로 내보내는 배치 프로그램입니다. 평일 정기공시를
스캔해 대상 기업만 자동 증분 수집하는 데일리 스케줄러도 포함합니다.

이 문서는 다음 개발자 또는 시스템 관리자가 프로젝트를 빠르고 정확하게 파악하고 인수인계받을
수 있도록 작성되었습니다.

---

## ✨ 주요 기능

- **연/분기 벌크 수집** (`main.py`): 대상 기업 목록 파일(csv/xlsx)을 읽어, 지정한 연도 범위의
  재무제표를 DART API로 수집해 DB에 저장하고 피벗 엑셀로 내보냅니다.
- **데일리 자동 증분 수집** (`daily_scheduler.py`): 당일 정기공시를 스캔해, 대상 기업의 새 공시가
  있으면 XBRL을 파싱해 증분 수집합니다. 컨테이너 내장 cron이 평일 15:50 KST에 자동 실행합니다.
- **누락분 재보강** (`update_missing_data.py`): 이미 만들어진 엑셀에서 값이 비어있는(수집
  실패했던) 연도/기업만 골라 다시 수집합니다.
- **엑셀 재생성** (`export_v2_excel.py`): DB 내용을 다시 읽어 피벗 엑셀만 재렌더링합니다(재수집 없음).
- **구글 드라이브 SSOT 동기화**: 매 실행마다 Drive에서 최신 DB/설정 파일을 받아 작업하고, 끝나면
  변경분을 다시 업로드합니다.

---

## 🏗 아키텍처

포트-어댑터(Hexagonal Architecture) 구조로, 비즈니스 로직이 외부 인프라(DART API/XBRL, SQLite,
구글 드라이브, 네이버 종목 조회)에 직접 의존하지 않습니다.

```
dart-fss-extractor/
├── docker/              # Docker 환경 구축 파일 (Dockerfile, docker-compose, cron 스크립트)
├── config/              # account_keywords.toml (계정과목 매핑 설정)
├── secrets/             # 인증 자격 증명 키 저장소 (Git 제외 대상)
│   ├── client_secret.json     # Google OAuth 클라이언트 보안 비밀
│   └── token.json              # 최초 실행 시 생성되는 OAuth 토큰
├── data/                # SQLite SSOT DB 및 대상 기업 목록 작업 사본 (Git 제외 대상)
├── output/              # 피벗 엑셀 산출물 (Git 제외 대상)
├── src/
│   ├── core/
│   │   ├── domain/models/   # 순수 도메인 모델
│   │   ├── ports/            # 어댑터용 인터페이스 (수집/저장/캐시/내보내기 등)
│   │   └── services/         # FinancialCollectionService, DailyRoutineService,
│   │                         # IncrementalUpdateService, DbSyncSession, config_sync 등
│   ├── infra/adapters/
│   │   ├── sqlite/            # SqliteRepositoryAdapter (SSOT), 티커명 캐시
│   │   ├── storage/           # GoogleDriveAdapter, 로컬 JSON 캐시
│   │   └── ...                # DART API/XBRL 수집, 엑셀 export, 네이버 종목명 조회
│   ├── main.py               # 수동 벌크 수집 진입점
│   ├── daily_scheduler.py    # 데일리 자동 증분 수집 진입점 (cron이 호출)
│   ├── export_v2_excel.py    # DB → 엑셀 재렌더링 진입점
│   └── update_missing_data.py # 누락분 재보강 진입점
└── tests/               # unit / integration / e2e
```

- `main.py`/`daily_scheduler.py` 모두 실행 시작 시 Google Drive에서 DB(SSOT)를 받아 로컬
  작업 사본으로 열고, 종료 시(성공/실패 무관, `finally`) 변경분을 다시 업로드합니다
  (`DbSyncSession`) — DB SSOT + 세션형 왕복 패턴은 `db_ssot_guide.md` §6 참고.
- `DailyRoutineService`가 데일리 흐름(DB 다운로드 → 증분 수집 → export → 업로드) 전체를
  오케스트레이션하며, CLI(`daily_scheduler.py`)는 실행만 담당합니다(`orchestration_guide.md` §1).
- DB 업로드 실패나 치명적 예외는 exit code 1로 끝나 cron 파이프라인이 성공으로 오판하지
  않도록 합니다(`docker_guide.md` §10).

---

## 🚀 환경 설정 및 설치

### 1. 사전 요구 사항
- **Python 3.12** 이상 및 **`uv`** 패키지 관리자
- DART Open API 인증키
- **Docker 및 Docker Compose** (컨테이너 실행 시)

### 2. 패키지 설치
```bash
uv sync
```

### 3. 환경 변수 설정 (`.env`)
```env
DART_API_KEY=your_dart_open_api_key
GOOGLE_DRIVE_FINANCIAL_STATEMENTS_ID=your_google_drive_folder_id
OUTPUT_DIRECTORY=output
ROOT_DIR=.
```

### 4. 시크릿 설정
`secrets/client_secret.json`(Google Cloud Console에서 발급받은 OAuth 2.0 Desktop app 클라이언트)을
넣어두면, 최초 실행 시 브라우저 인증을 거쳐 `secrets/token.json`이 자동 생성됩니다.

---

## 💻 사용법

```bash
# 연/분기 벌크 수집 (기본 2015~2025년)
uv run dart-extractor --start-year 2020 --end-year 2026 --companies data/target_companies.csv

# 데일리 증분 수집 (기본: 어제~오늘 정기공시 스캔)
uv run python src/daily_scheduler.py

# 특정 기간 정기공시만 스캔
uv run python src/daily_scheduler.py --bgn-de 20260801 --end-de 20260828

# DB를 다시 읽어 엑셀만 재렌더링 (재수집 없음)
uv run dart-export-excel

# 이미 만든 엑셀에서 값이 빈 연도/기업만 재보강
uv run dart-update-missing --file output/financial_data_2015_2025.xlsx
```

---

## 🐳 Docker로 실행

```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml run --rm dart-fss-extractor python src/main.py --start-year 2026 --end-year 2026
docker compose -f docker/docker-compose.yml up -d dart-fss-extractor-cron
```

컨테이너 내장 cron이 스케줄에 따라 `daily_scheduler.py`를 자동 실행합니다. 스케줄은
`docker/crontab`을 참고하세요(기본: 평일 15:50 KST — 당일 정기공시 확정 이후 스캔).

---

## 🧪 테스트

```bash
uv run pytest
```

`tests/unit`, `tests/integration`, `tests/e2e`로 나뉘어 있습니다.

---

## 💡 인수인계 시 주의 사항 (개발 팁)

1. **SQLite가 SSOT, `data/`는 작업 사본일 뿐**: `main.py`/`daily_scheduler.py` 둘 다 실행
   시작 시 Google Drive에서 DB를 받아와 로컬에 작업 사본을 두고, 종료 시(`finally`, 성공/실패
   무관) 다시 업로드합니다. 로컬 DB를 직접 손으로 고치지 마세요 — 다음 실행에서 원격이 다시
   덮어씁니다.
2. **설정 파일도 Drive가 원본**: 대상 기업 목록(`data/target_companies.csv`)이나 계정과목
   매핑 설정도 로컬 사본과 무관하게 매 실행 시작 시 Drive `db/` 폴더 내용으로 갱신됩니다
   (`config_sync.download_config_files`).
3. **exit code로 실패를 숨기지 말 것**: `main.py`/`daily_scheduler.py`는 치명적 오류나
   DB 업로드 실패 시 `sys.exit(1)`로 끝납니다 — cron이 `set -e` 등으로 성공/실패를 판단하므로,
   새 진입점을 추가할 때도 예외를 삼키고 조용히 0으로 끝나지 않도록 주의하세요.
4. **`main.py`(수동 벌크)와 `daily_scheduler.py`(자동 증분)는 의도적으로 별도 오케스트레이션**:
   `daily_scheduler.py`는 날짜 범위 기반 데일리 전용 `DailyRoutineService`를 쓰고, `main.py`는
   연/분기 벌크 수집이라 억지로 그 서비스에 맞추지 않고 같은 재료(`DbSyncSession`,
   `config_sync`)를 직접 조합합니다.
5. **의존성 패키지 관리 (`uv`)**: `pip install` 대신 `uv add <패키지명>`을 사용해
   `pyproject.toml`/`uv.lock`을 자동 최신화하세요.
