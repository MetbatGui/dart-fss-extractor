"""금액 처리를 전담하는 Value Object (값 객체)."""

import re
from decimal import Decimal
from typing import Optional, Any, Union


class Amount:
    """재무 금액을 안전하게 캡슐화하는 불변 Value Object (값 객체).

    - 문자열 결측치('-', '', None)를 안전하게 None으로 처리합니다.
    - 덧셈, 뺄셈 등 사칙연산에서 결측치를 방어적으로 처리합니다.
    - 하위 호환성을 위해 int, float, str 변환 및 연산자 오버로딩을 제공합니다.
    """

    def __init__(self, value: Optional[Union[int, float, Decimal, str, 'Amount']] = None):
        self._value: Optional[Decimal] = self._parse_value(value)

    @property
    def value(self) -> Optional[Decimal]:
        """내부 Decimal 값 반환 (결측 시 None)."""
        return self._value

    @property
    def is_none(self) -> bool:
        """결측치 여부 확인."""
        return self._value is None

    def _parse_value(self, val: Any) -> Optional[Decimal]:
        if val is None:
            return None
        if isinstance(val, Amount):
            return val.value
        if isinstance(val, (int, float, Decimal)):
            return Decimal(str(val))
        
        # 문자열 파싱
        if isinstance(val, str):
            clean_str = val.strip()
            if clean_str in ("", "-", "None", "NaN"):
                return None
            
            # 숫자, 소수점, 음수 기호만 추출
            clean_str = re.sub(r"[^\d.-]", "", clean_str)
            if not clean_str or clean_str == "." or clean_str == "-":
                return None
            try:
                return Decimal(clean_str)
            except Exception:
                return None
        return None

    def scale(self, factor: Union[int, float, Decimal]) -> 'Amount':
        """스케일을 조정하여 새로운 Amount 객체를 반환합니다."""
        if self.is_none:
            return Amount(None)
        return Amount(self._value * Decimal(str(factor)))

    def __add__(self, other: Any) -> 'Amount':
        other_val = Amount(other)
        if self.is_none:
            return other_val
        if other_val.is_none:
            return self
        return Amount(self._value + other_val.value)

    def __sub__(self, other: Any) -> 'Amount':
        other_val = Amount(other)
        if self.is_none and other_val.is_none:
            return Amount(None)
        if self.is_none:
            return Amount(-other_val.value)
        if other_val.is_none:
            return self
        return Amount(self._value - other_val.value)

    def __mul__(self, other: Any) -> 'Amount':
        if self.is_none:
            return Amount(None)
        if isinstance(other, Amount):
            if other.is_none:
                return Amount(None)
            return Amount(self._value * other.value)
        try:
            return Amount(self._value * Decimal(str(other)))
        except Exception:
            return Amount(None)

    def __truediv__(self, other: Any) -> 'Amount':
        if self.is_none:
            return Amount(None)
        if isinstance(other, Amount):
            if other.is_none or other.value == 0:
                return Amount(None)
            return Amount(self._value / other.value)
        try:
            divisor = Decimal(str(other))
            if divisor == 0:
                return Amount(None)
            return Amount(self._value / divisor)
        except Exception:
            return Amount(None)

    def __eq__(self, other: Any) -> bool:
        other_val = Amount(other)
        return self._value == other_val.value

    def __lt__(self, other: Any) -> bool:
        other_val = Amount(other)
        if self.is_none or other_val.is_none:
            return False
        return self._value < other_val.value

    def __le__(self, other: Any) -> bool:
        other_val = Amount(other)
        if self.is_none or other_val.is_none:
            return False
        return self._value <= other_val.value

    def __gt__(self, other: Any) -> bool:
        other_val = Amount(other)
        if self.is_none or other_val.is_none:
            return False
        return self._value > other_val.value

    def __ge__(self, other: Any) -> bool:
        other_val = Amount(other)
        if self.is_none or other_val.is_none:
            return False
        return self._value >= other_val.value

    def __neg__(self) -> 'Amount':
        if self.is_none:
            return Amount(None)
        return Amount(-self._value)

    def __abs__(self) -> 'Amount':
        if self.is_none:
            return Amount(None)
        return Amount(abs(self._value))

    def __int__(self) -> int:
        if self.is_none:
            raise ValueError("결측치(None)는 int로 변환할 수 없습니다.")
        return int(self._value)

    def __float__(self) -> float:
        if self.is_none:
            raise ValueError("결측치(None)는 float로 변환할 수 없습니다.")
        return float(self._value)

    def __str__(self) -> str:
        if self.is_none:
            return ""
        # 소수점 이하가 없으면 정수 문자열로, 있으면 실수 문자열로 반환
        if self._value == self._value.to_integral_value():
            return str(int(self._value))
        return str(self._value)

    def __repr__(self) -> str:
        return f"Amount({str(self)})"
