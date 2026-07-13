"""JSON 파일 기반 캐시 저장소 어댑터."""

import json
import os
from pathlib import Path
from typing import Dict

from core.ports.cache_port import CachePort


class JsonCacheAdapter(CachePort):
    """JSON 파일을 사용한 일괄 로드/저장 방식의 공시 캐시 어댑터.
    
    경로: cache/disclosure_cache.json
    """

    def __init__(self, cache_file_path: str = "cache/disclosure_cache.json"):
        """JsonCacheAdapter 초기화 및 부모 디렉터리 동적 생성."""
        self.cache_file_path = Path(cache_file_path)
        # cache/ 폴더가 없을 경우 자동 생성
        self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)

    def load_all(self) -> Dict[str, dict]:
        """전체 캐시 데이터를 메모리에 로드하여 Dict 형태로 반환합니다."""
        if not self.cache_file_path.exists():
            return {}

        try:
            with open(self.cache_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
                return {}
        except Exception:
            # 파일이 비어있거나 올바른 형식이 아닐 경우 빈 Dict 반환
            return {}

    def save_all(self, cache_data: Dict[str, dict]) -> None:
        """메모리 상의 전체 캐시 Dict 데이터를 파일에 일괄 영속화합니다."""
        try:
            with open(self.cache_file_path, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise RuntimeError(f"캐시 파일 저장 실패 ({self.cache_file_path}): {e}")
