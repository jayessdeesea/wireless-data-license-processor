"""Tests for schema validation"""
import pytest
from datetime import date
from pydantic import ValidationError
from wdlp.schema import AMRecord, ENRecord

def test_am_record_valid():
    """Test valid Amateur License record creation"""
    record = AMRecord(
        record_type="AM",
        system_id=123456789,
        call_sign="W1AW",
        operator_class="A",
        group_code="C",
        region_code=1,
        trustee_call_sign="K1ABC",
        trustee_indicator="Y"
    )
    assert record.record_type == "AM"
    assert record.system_id == 123456789
    assert record.call_sign == "W1AW"
    assert record.operator_class == "A"

def test_am_record_invalid_type():
    """Test invalid record type validation"""
    with pytest.raises(ValidationError) as exc:
        AMRecord(record_type="XX")
    assert "record_type" in str(exc.value)
    assert "pattern" in str(exc.value)

def test_am_record_invalid_system_id():
    """Test system_id range validation"""
    with pytest.raises(ValidationError) as exc:
        AMRecord(record_type="AM", system_id=1000000000)  # Too large
    assert "system_id" in str(exc.value)
    assert "less than or equal to" in str(exc.value)

def test_am_record_optional_fields():
    """Test optional fields handling"""
    record = AMRecord(record_type="AM")
    assert record.system_id is None
    assert record.call_sign is None
    assert record.operator_class is None

def test_en_record_valid():
    """Test valid Entity record creation"""
    record = ENRecord(
        record_type="EN",
        system_id=123456789,
        entity_type="I",
        entity_name="Test Entity",
        first_name="John",
        last_name="Doe",
        status_date=date(2024, 1, 1)
    )
    assert record.record_type == "EN"
    assert record.system_id == 123456789
    assert record.entity_type == "I"
    assert record.entity_name == "Test Entity"

def test_en_record_invalid_entity_type():
    """Test invalid entity type validation"""
    with pytest.raises(ValidationError) as exc:
        ENRecord(record_type="EN", entity_type="X")
    assert "entity_type" in str(exc.value)

def test_en_record_invalid_date():
    """Test date validation"""
    # Test future date
    future = date(2025, 12, 31)
    with pytest.raises(ValidationError) as exc:
        ENRecord(record_type="EN", status_date=future)
    assert "status_date" in str(exc.value)

def test_en_record_phone_format():
    """Test phone number format validation"""
    # Valid phone number
    record = ENRecord(record_type="EN", phone="1234567890")
    assert record.phone == "1234567890"

    # Invalid phone number
    with pytest.raises(ValidationError) as exc:
        ENRecord(record_type="EN", phone="123")  # Too short
    assert "phone" in str(exc.value)

def test_en_record_email_format():
    """Test email format validation"""
    # Valid email
    record = ENRecord(record_type="EN", email="test@example.com")
    assert record.email == "test@example.com"

    # Invalid email
    with pytest.raises(ValidationError) as exc:
        ENRecord(record_type="EN", email="invalid-email")
    assert "email" in str(exc.value)

def test_model_serialization():
    """Test model serialization to dict/json"""
    record = AMRecord(
        record_type="AM",
        system_id=123456789,
        call_sign="W1AW"
    )
    data = record.model_dump()
    assert data["record_type"] == "AM"
    assert data["system_id"] == 123456789
    assert data["call_sign"] == "W1AW"
    assert "operator_class" in data  # Optional field included as None
