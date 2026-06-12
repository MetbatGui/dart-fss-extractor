"""기업 도메인 모델."""

from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional
from datetime import datetime

@dataclass
class Company:
    """기업 정보 및 수집 상태를 관리하는 도메인 객체."""
    
    code: str
    name: str
    failed_years: List[int] = field(default_factory=list)
    success_years: List[int] = field(default_factory=list)
    last_updated: Optional[str] = None
    settlement_month: int = 12
    
    def mark_success(self, year: int) -> None:
        """연도별 수집 성공 표시."""
        if year not in self.success_years:
            self.success_years.append(year)
            self.success_years.sort()
        
        # 실패 목록에 있다면 제거 (재시도 성공 시)
        if year in self.failed_years:
            self.failed_years.remove(year)
            
        self._update_timestamp()

    def mark_failure(self, year: int) -> None:
        """연도별 수집 실패 표시."""
        if year not in self.failed_years:
            self.failed_years.append(year)
            self.failed_years.sort()
            
        self._update_timestamp()

    def _update_timestamp(self) -> None:
        """최종 수정 시간 업데이트."""
        self.last_updated = datetime.now().isoformat()

    def to_dict(self) -> Dict:
        """딕셔너리 변환."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Company':
        """딕셔너리에서 객체 생성."""
        return cls(**data)

