"""TOML 설정 로딩 테스트 스크립트."""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from core.services.data_processing_service import DataProcessingService

def test_toml_loading():
    """TOML 설정 파일 로딩 테스트."""
    print("=" * 60)
    print("TOML 설정 로딩 테스트")
    print("=" * 60)
    
    # DataProcessingService 초기화
    service = DataProcessingService()
    
    # 키워드 확인
    print(f"\n[OK] REVENUE_KEYWORDS: {service.REVENUE_KEYWORDS}")
    print(f"[OK] OP_PROFIT_KEYWORDS: {service.OP_PROFIT_KEYWORDS}")
    print(f"[OK] NET_INCOME_KEYWORDS: {service.NET_INCOME_KEYWORDS}")
    
    # 검증
    assert len(service.REVENUE_KEYWORDS) == 4, "매출액 키워드는 4개여야 합니다"
    assert len(service.OP_PROFIT_KEYWORDS) == 2, "영업이익 키워드는 2개여야 합니다"
    assert len(service.NET_INCOME_KEYWORDS) == 6, "당기순이익 키워드는 6개여야 합니다"
    
    print("\n" + "=" * 60)
    print("[SUCCESS] 모든 테스트 통과!")
    print("=" * 60)

if __name__ == "__main__":
    test_toml_loading()
