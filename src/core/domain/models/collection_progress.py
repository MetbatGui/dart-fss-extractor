from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime

@dataclass
class CollectionProgress:
    """전체 수집 진행 상태를 관리하는 모델."""
    total_companies: int
    completed_codes: List[str] = field(default_factory=list)
    last_processed_idx: int = -1
    last_processed_name: Optional[str] = None
    last_processed_code: Optional[str] = None
    api_call_count: int = 0
    start_time: str = field(default_factory=lambda: datetime.now().isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())

    def mark_company_completed(self, code: str, name: str, idx: int):
        if code not in self.completed_codes:
            self.completed_codes.append(code)
        self.last_processed_idx = idx
        self.last_processed_name = name
        self.last_processed_code = code
        self.last_updated = datetime.now().isoformat()

    def increment_api_call(self, count: int = 1):
        self.api_call_count += count
        self.last_updated = datetime.now().isoformat()
