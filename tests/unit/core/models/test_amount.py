"""Amount Value Object 단위 테스트."""

import pytest
from decimal import Decimal
from core.domain.models.amount import Amount


def test_amount_parsing():
    """Amount 객체의 다양한 타입 및 문자열 파싱 검증."""
    assert Amount(12345).value == Decimal("12345")
    assert Amount(123.45).value == Decimal("123.45")
    assert Amount("12,345,678").value == Decimal("12345678")
    assert Amount("-1,234.5").value == Decimal("-1234.5")
    
    # 결측치 파싱 검증
    assert Amount("-").is_none is True
    assert Amount("").is_none is True
    assert Amount(None).is_none is True
    assert Amount("None").is_none is True
    assert Amount("NaN").is_none is True


def test_amount_math_operations():
    """Amount 객체 간 사칙 연산 검증."""
    a = Amount(1000)
    b = Amount(500)
    
    # 덧셈 및 뺄셈
    assert a + b == Amount(1500)
    assert a - b == Amount(500)
    
    # 곱셈 및 나눗셈
    assert a * 2 == Amount(2000)
    assert a / 2 == Amount(500)
    assert a * b == Amount(500000)
    assert a / b == Amount(2)


def test_amount_null_safety():
    """결측치가 섞인 상태에서의 안전 연산 검증."""
    a = Amount(1000)
    none_amount = Amount(None)
    
    # 결측치와의 연산
    assert a + none_amount == Amount(1000)
    assert none_amount + a == Amount(1000)
    assert a - none_amount == Amount(1000)
    assert none_amount - a == Amount(-1000)
    assert a * none_amount == Amount(None)
    assert a / none_amount == Amount(None)
    assert none_amount / a == Amount(None)


def test_amount_comparison():
    """Amount 객체 간의 크기 비교 연산 검증."""
    a = Amount(100)
    b = Amount(200)
    c = Amount(100)
    
    assert a < b
    assert b > a
    assert a <= b
    assert a == c
    assert a != b
    
    # 결측치와의 비교 방어 (비교 대상에 None이 있으면 False 반환 검증)
    none_amount = Amount(None)
    assert (a < none_amount) is False
    assert (a > none_amount) is False
    assert (a == none_amount) is False


def test_amount_casting():
    """형변환 동작 검증."""
    a = Amount("1,000.5")
    assert str(a) == "1000.5"
    assert float(a) == 1000.5
    assert int(a) == 1000  # 표준 절사(truncation) 방식 검증
    
    b = Amount("500")
    assert str(b) == "500"  # 정수 표기
    
    none_amount = Amount(None)
    assert str(none_amount) == ""
    with pytest.raises(ValueError):
        int(none_amount)
    with pytest.raises(ValueError):
        float(none_amount)


def test_amount_math_operations_edge_cases():
    """Amount 객체의 나눗셈 0 오류(Zero Division) 및 예외 인자 처리 방어력 검증."""
    a = Amount(1000)
    
    # 1. 0으로 나누기 연산 방어 (None 반환 검증)
    assert (a / 0) == Amount(None)
    assert (a / Amount(0)) == Amount(None)
    
    # 2. 부적합한 인자와의 연산 방어 (None 반환 검증)
    assert (a * "invalid_text") == Amount(None)
    assert (a / "invalid_text") == Amount(None)
    
    # 3. 비정상 문자열 파싱 방어 (None 반환 검증)
    assert Amount("1.2.3.4").is_none is True
    assert Amount("--500").is_none is True
    assert Amount("abc").is_none is True
