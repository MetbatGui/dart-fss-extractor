"""FinancialDataExportService의 연결/개별 병합 로직 검증."""

import pandas as pd
import pytest

from core.services.data_processing_service import DataProcessingService
from core.services.financial_data_export_service import FinancialDataExportService


class _FakeRepo:
    """RepositoryPort를 흉내내는 인메모리 더미 (dataset_name별 DataFrame 반환)."""

    def __init__(self, datasets: dict[str, pd.DataFrame]):
        self._datasets = datasets

    def load_all(self, dataset_name: str) -> pd.DataFrame:
        return self._datasets.get(dataset_name, pd.DataFrame())


class _FakeExporter:
    """ExportPort를 흉내내는 더미 - export된 DataFrame을 그대로 저장."""

    def __init__(self):
        self.exported: dict[str, pd.DataFrame] | None = None

    def export_excel(self, dataframes: dict[str, pd.DataFrame], file_path: str) -> None:
        self.exported = dataframes


def test_ofs_value_used_when_cfs_row_exists_but_is_null():
    """CFS 행이 존재하지만 값이 전부 NULL이고, 같은 기간 OFS에 실값이 있으면
    기업 단위로 통째로 CFS를 선택해 OFS 값을 버리지 않고, 그 값을 채워야 한다."""
    cfs_df = pd.DataFrame(
        [
            {
                "종목코드": "000001",
                "기업명": "테스트기업",
                "연도": 2020,
                "구분": "분기",
                "분기": "1Q",
                "매출액": None,
                "영업이익": None,
                "당기순이익": None,
                "rcept_no": None,
            }
        ]
    )
    ofs_df = pd.DataFrame(
        [
            {
                "종목코드": "000001",
                "기업명": "테스트기업",
                "연도": 2020,
                "구분": "분기",
                "분기": "1Q",
                "매출액": 1000.0,
                "영업이익": 100.0,
                "당기순이익": 50.0,
                "rcept_no": "R001",
            }
        ]
    )

    repo = _FakeRepo(
        {"financial_data_cfs": cfs_df, "financial_data_ofs": ofs_df}
    )
    exporter = _FakeExporter()
    svc = FinancialDataExportService(repo, exporter, DataProcessingService())

    ok = svc.export_integrated_financial_data("dummy.xlsx")

    assert ok
    rev_sheet = exporter.exported["매출액_분기"]
    # 백만원 단위 반올림: 1000 / 1_000_000 -> 0.0
    assert rev_sheet.loc[("테스트기업", "000001"), "2020.1Q"] == 0.0

    op_sheet = exporter.exported["영업이익_분기"]
    assert op_sheet.loc[("테스트기업", "000001"), "2020.1Q"] == 0.0


def test_year_range_filter_excludes_out_of_range_rows():
    """year_min/year_max를 지정하면 해당 범위 밖 연도는 최종 결과에서 제외되어야 한다."""
    cfs_df = pd.DataFrame(
        [
            {
                "종목코드": "000001",
                "기업명": "테스트기업",
                "연도": 2014,
                "구분": "연간",
                "분기": "연간",
                "매출액": 500.0,
                "영업이익": 50.0,
                "당기순이익": 10.0,
                "rcept_no": "R000",
            },
            {
                "종목코드": "000001",
                "기업명": "테스트기업",
                "연도": 2020,
                "구분": "연간",
                "분기": "연간",
                "매출액": 1000.0,
                "영업이익": 100.0,
                "당기순이익": 50.0,
                "rcept_no": "R001",
            },
        ]
    )
    repo = _FakeRepo({"financial_data_cfs": cfs_df})
    exporter = _FakeExporter()
    svc = FinancialDataExportService(repo, exporter, DataProcessingService())

    ok = svc.export_integrated_financial_data(
        "dummy.xlsx", year_min=2015, year_max=2025
    )

    assert ok
    rev_sheet = exporter.exported["매출액_연간"]
    assert 2014 not in rev_sheet.columns
    assert 2020 in rev_sheet.columns
