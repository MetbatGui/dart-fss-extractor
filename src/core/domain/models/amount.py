"""금액 처리를 전담하는 Value Object (값 객체)."""

import re
from decimal import Decimal
from typing import Any


class Amount:
    """재무 금액을 안전하게 캡슐화하는 불변 Value Object (값 객체).

    - 문자열 결측치('-', '', None)를 안전하게 None으로 처리합니다.
    - 덧셈, 뺄셈 등 사칙연산에서 결측치를 방어적으로 처리합니다.
    - 하위 호환성을 위해 int, float, str 변환 및 연산자 오버로딩을 제공합니다.
    """

    def __init__(self, value: Any = None):
        self._value: Decimal | None = self._parse_value(value)

    @property
    def value(self) -> Decimal | None:
        """내부 Decimal 값 반환 (결측 시 None)."""
        return self._value

    @property
    def is_none(self) -> bool:
        """결측치 여부 확인."""
        return self._value is None

    def _parse_value(self, val: Any) -> Decimal | None:
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

    def scale(self, factor: float | Decimal) -> "Amount":
        """스케일을 조정하여 새로운 Amount 객체를 반환합니다."""
        value = self._value
        if value is None:
            return Amount(None)
        return Amount(value * Decimal(str(factor)))

    def __add__(self, other: Any) -> "Amount":
        value = self._value
        other_value = Amount(other).value
        if value is None:
            return Amount(other_value)
        if other_value is None:
            return Amount(value)
        return Amount(value + other_value)

    def __sub__(self, other: Any) -> "Amount":
        value = self._value
        other_value = Amount(other).value
        if value is None:
            return Amount(None) if other_value is None else Amount(-other_value)
        if other_value is None:
            return Amount(value)
        return Amount(value - other_value)

    def __mul__(self, other: Any) -> "Amount":
        value = self._value
        if value is None:
            return Amount(None)
        if isinstance(other, Amount):
            if other.value is None:
                return Amount(None)
            return Amount(value * other.value)
        try:
            return Amount(value * Decimal(str(other)))
        except Exception:
            return Amount(None)

    def __truediv__(self, other: Any) -> "Amount":
        value = self._value
        if value is None:
            return Amount(None)
        if isinstance(other, Amount):
            if other.value is None or other.value == 0:
                return Amount(None)
            return Amount(value / other.value)
        try:
            divisor = Decimal(str(other))
            if divisor == 0:
                return Amount(None)
            return Amount(value / divisor)
        except Exception:
            return Amount(None)

    def __eq__(self, other: object) -> bool:
        return self._value == Amount(other).value

    def __lt__(self, other: Any) -> bool:
        value = self._value
        other_value = Amount(other).value
        if value is None or other_value is None:
            return False
        return value < other_value

    def __le__(self, other: Any) -> bool:
        value = self._value
        other_value = Amount(other).value
        if value is None or other_value is None:
            return False
        return value <= other_value

    def __gt__(self, other: Any) -> bool:
        value = self._value
        other_value = Amount(other).value
        if value is None or other_value is None:
            return False
        return value > other_value

    def __ge__(self, other: Any) -> bool:
        value = self._value
        other_value = Amount(other).value
        if value is None or other_value is None:
            return False
        return value >= other_value

    def __neg__(self) -> "Amount":
        value = self._value
        if value is None:
            return Amount(None)
        return Amount(-value)

    def __abs__(self) -> "Amount":
        value = self._value
        if value is None:
            return Amount(None)
        return Amount(abs(value))

    def __int__(self) -> int:
        value = self._value
        if value is None:
            raise ValueError("결측치(None)는 int로 변환할 수 없습니다.")
        return int(value)

    def __float__(self) -> float:
        value = self._value
        if value is None:
            raise ValueError("결측치(None)는 float로 변환할 수 없습니다.")
        return float(value)

    def __str__(self) -> str:
        value = self._value
        if value is None:
            return ""
        # 소수점 이하가 없으면 정수 문자열로, 있으면 실수 문자열로 반환
        if value == value.to_integral_value():
            return str(int(value))
        return str(value)

    def __repr__(self) -> str:
        return f"Amount({self!s})"
