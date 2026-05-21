"""TOML 설정 로딩 테스트."""

import pytest
from core.services.data_processing_service import DataProcessingService

def test_data_processing_config_loading():
    """DataProcessingService의 TOML 설정 로딩 테스트."""
    # DataProcessingService 초기화
    service = DataProcessingService()
    
    # 키워드 로드 여부 확인 (비어있지 않아야 함)
    assert len(service.REVENUE_KEYWORDS) > 0
    assert len(service.OP_PROFIT_KEYWORDS) > 0
    assert len(service.NET_INCOME_KEYWORDS) > 0
    
    # 기본값 이상의 키워드가 로드되었는지 확인 (설정 파일 존재 가정)
    # 실제 설정 파일 내용에 따라 수치는 변경될 수 있으므로 최소 1개 이상으로 검증
    assert "매출액" in service.REVENUE_KEYWORDS
    assert "영업이익" in service.OP_PROFIT_KEYWORDS
    assert "당기순이익" in service.NET_INCOME_KEYWORDS
