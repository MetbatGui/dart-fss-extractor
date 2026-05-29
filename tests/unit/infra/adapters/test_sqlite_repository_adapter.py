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
