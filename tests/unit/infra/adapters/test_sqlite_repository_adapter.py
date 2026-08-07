"""SqliteRepositoryAdapter 단위 테스트."""

import pytest
import pandas as pd
from core.domain.models.company import Company
from infra.adapters.sqlite.sqlite_repository_adapter import SqliteRepositoryAdapter


@pytest.fixture
def adapter():
    """인메모리 데이터베이스를 사용하는 격리된 어댑터 피스처."""
    ad = SqliteRepositoryAdapter(db_path=":memory:")
    yield ad
    ad.close()


def test_sqlite_company_metadata_crud(adapter):
    """기업 메타데이터 등록, 조회 및 덮어쓰기 검증."""
    comp = Company(
        code="005930",
        name="삼성전자",
        success_years=[2024, 2025],
        failed_years=[2023],
        last_updated="2026-05-29T12:00:00"
    )
    
    # 저장
    adapter.save_company_metadata(comp)
    
    # 조회 및 복원 상태 검증
    loaded = adapter.load_company_metadata("005930")
    assert loaded is not None
    assert loaded.code == "005930"
    assert loaded.name == "삼성전자"
    assert loaded.success_years == [2024, 2025]
    assert loaded.failed_years == [2023]
    
    # 실패 리스트의 성공 전환 갱신 검증
    loaded.mark_success(2023)
    adapter.save_company_metadata(loaded)
    
    updated = adapter.load_company_metadata("005930")
    assert updated.success_years == [2023, 2024, 2025]
    assert updated.failed_years == []


def test_sqlite_partition_dataframe_ops(adapter):
    """DataFrame 파티션 저장, 병합 덮어쓰기 및 전체 로드 검증."""
    dataset = "financial_data_cfs"
    
    df_raw = pd.DataFrame([
        {"기업명": "삼성전자", "연도": 2026, "구분": "분기", "분기": "1Q", "매출액": 1000, "영업이익": 100, "당기순이익": 80},
        {"기업명": "삼성전자", "연도": 2026, "구분": "분기", "분기": "2Q", "매출액": 2000, "영업이익": 200, "당기순이익": 160},
    ])
    
    # 파티션 저장
    adapter.save_partition(dataset, "005930", df_raw)
    
    # 존재 유무 확인
    assert adapter.exists(dataset, "005930") is True
    assert adapter.exists(dataset, "000000") is False
    
    # 덮어쓰기(Insert or Replace) 및 정정공시 상황 검증 (매출액 2000 -> 2500으로 정정)
    df_rectified = pd.DataFrame([
        {"기업명": "삼성전자", "연도": 2026, "구분": "분기", "분기": "2Q", "매출액": 2500, "영업이익": 250, "당기순이익": 200},
    ])
    adapter.save_partition(dataset, "005930", df_rectified)
    
    # 조회 후 보정 확인
    loaded_df = adapter.load_partition(dataset, "005930")
    assert len(loaded_df) == 2
    # 2Q 매출액이 2500으로 업데이트 되었는지 확인
    row_2q = loaded_df[loaded_df["분기"] == "2Q"].iloc[0]
    assert row_2q["매출액"] == 2500
    assert row_2q["영업이익"] == 250


def test_sqlite_partition_conflict_different_disclosure_not_amendment_preserves_existing(adapter):
    """서로 다른 rcept_no(공시)가 같은 분기 키를 가리키는데 정정이 아니면, 기존 값을 보존해야 한다.

    시나리오: 3월 공시(rcept_no=A)가 이미 저장돼 있는데, 파싱 오류 등으로 6월 공시(rcept_no=B,
    정정 아님)가 같은 (연도, 분기) 키로 잘못 매핑되어 들어오는 경우 - 기존 3월 데이터가
    조용히 사라지면 안 된다.
    """
    dataset = "financial_data_cfs"

    df_original = pd.DataFrame([
        {"기업명": "테스트기업", "연도": 2026, "구분": "분기", "분기": "1Q",
         "매출액": 1000, "영업이익": 100, "당기순이익": 80,
         "rcept_no": "20260101000001", "is_amendment": False},
    ])
    adapter.save_partition(dataset, "999999", df_original)

    df_conflicting = pd.DataFrame([
        {"기업명": "테스트기업", "연도": 2026, "구분": "분기", "분기": "1Q",
         "매출액": 9999, "영업이익": 999, "당기순이익": 999,
         "rcept_no": "20260601000002", "is_amendment": False},
    ])
    adapter.save_partition(dataset, "999999", df_conflicting)

    loaded_df = adapter.load_partition(dataset, "999999")
    row = loaded_df[loaded_df["분기"] == "1Q"].iloc[0]
    assert row["매출액"] == 1000  # 원본(A) 값이 그대로 보존되어야 함
    assert row["rcept_no"] == "20260101000001"


def test_sqlite_partition_amendment_overwrites_despite_different_rcept_no(adapter):
    """진짜 정정공시(is_amendment=True)는 rcept_no가 달라도 정상적으로 덮어써야 한다."""
    dataset = "financial_data_cfs"

    df_original = pd.DataFrame([
        {"기업명": "테스트기업", "연도": 2026, "구분": "분기", "분기": "1Q",
         "매출액": 1000, "영업이익": 100, "당기순이익": 80,
         "rcept_no": "20260101000001", "is_amendment": False},
    ])
    adapter.save_partition(dataset, "999999", df_original)

    df_amendment = pd.DataFrame([
        {"기업명": "테스트기업", "연도": 2026, "구분": "분기", "분기": "1Q",
         "매출액": 1500, "영업이익": 150, "당기순이익": 120,
         "rcept_no": "20260315000003", "is_amendment": True},
    ])
    adapter.save_partition(dataset, "999999", df_amendment)

    loaded_df = adapter.load_partition(dataset, "999999")
    row = loaded_df[loaded_df["분기"] == "1Q"].iloc[0]
    assert row["매출액"] == 1500
    assert row["rcept_no"] == "20260315000003"


def test_sqlite_partition_same_rcept_no_reprocess_overwrites(adapter):
    """같은 공시(rcept_no 동일)를 재처리하는 경우는 충돌이 아니라 정상 재저장이어야 한다."""
    dataset = "financial_data_cfs"

    df_first = pd.DataFrame([
        {"기업명": "테스트기업", "연도": 2026, "구분": "분기", "분기": "1Q",
         "매출액": 1000, "영업이익": 100, "당기순이익": 80,
         "rcept_no": "20260101000001", "is_amendment": False},
    ])
    adapter.save_partition(dataset, "999999", df_first)

    df_rerun = pd.DataFrame([
        {"기업명": "테스트기업", "연도": 2026, "구분": "분기", "분기": "1Q",
         "매출액": 1000, "영업이익": 100, "당기순이익": 80,
         "rcept_no": "20260101000001", "is_amendment": False},
    ])
    adapter.save_partition(dataset, "999999", df_rerun)

    loaded_df = adapter.load_partition(dataset, "999999")
    assert len(loaded_df) == 1
    assert loaded_df.iloc[0]["매출액"] == 1000


def test_sqlite_find_missing_companies(adapter):
    """특정 연도/분기 실적이 누락된 기업 색출 고속 쿼리 기능 검증."""
    # 1. 기업 메타데이터 3개 입력
    adapter.save_company_metadata(Company("001", "A사"))
    adapter.save_company_metadata(Company("002", "B사"))
    adapter.save_company_metadata(Company("003", "C사"))
    
    # 2. A사만 2026.1Q 실적이 있고, B사와 C사는 누락인 상태 모사
    df_a = pd.DataFrame([
        {"기업명": "A사", "연도": 2026, "구분": "분기", "분기": "1Q", "매출액": 100, "영업이익": 10, "당기순이익": 8}
    ])
    adapter.save_partition("financial_data_cfs", "001", df_a)
    
    # 색출 스캔
    target_codes = ["001", "002", "003"]
    missing = adapter.find_missing_companies(target_codes, 2026, "1Q", "연결")
    
    # A사(001)는 있고 B사, C사(002, 003)는 누락이므로 이 둘만 검출되어야 함
    assert len(missing) == 2
    assert "002" in missing
    assert "003" in missing
    assert "001" not in missing
