"""Tests for record type mapping"""
import pytest
from datetime import date
from wdlp.mapper import MapperFactory, MapperError
from wdlp.reader import Record

def test_am_mapper_valid():
    """Test valid Amateur License record mapping"""
    record = Record(
        line=1,
        fields=[
            "AM",           # record_type
            "123456789",    # system_id
            "ULS12345",     # uls_file_number
            "EBF98765",     # ebf_number
            "W1AW",         # call_sign
            "A",           # operator_class
            "C",           # group_code
            "1",           # region_code
            "K1ABC",       # trustee_call_sign
            "Y",           # trustee_indicator
            "N",           # physician_certification
            "Y",           # ve_signature
            "N",           # systematic_call_sign_change
            "N",           # vanity_call_sign_change
            "",            # vanity_relationship
            "",            # previous_call_sign
            "",            # previous_operator_class
            "John Doe"     # trustee_name
        ]
    )
    
    mapper = MapperFactory.create_mapper("AM")
    result = mapper(record)
    
    assert result.record_type == "AM"
    assert result.system_id == 123456789
    assert result.call_sign == "W1AW"
    assert result.operator_class == "A"
    assert result.trustee_name == "John Doe"

def test_am_mapper_invalid_type():
    """Test invalid record type handling"""
    record = Record(line=1, fields=["XX"])  # Wrong type
    
    with pytest.raises(MapperError) as exc:
        mapper = MapperFactory.create_mapper("AM")
        mapper(record)
    assert "record_type" in str(exc.value)

def test_am_mapper_invalid_system_id():
    """Test invalid system_id conversion"""
    record = Record(
        line=1,
        fields=["AM", "invalid", "", "", "", ""]  # Invalid system_id
    )
    
    with pytest.raises(MapperError) as exc:
        mapper = MapperFactory.create_mapper("AM")
        mapper(record)
    assert "system_id" in str(exc.value)

def test_en_mapper_valid():
    """Test valid Entity record mapping"""
    record = Record(
        line=1,
        fields=[
            "EN",           # record_type
            "123456789",    # system_id
            "ULS12345",     # uls_file_number
            "EBF98765",     # ebf_number
            "W1AW",         # call_sign
            "I",            # entity_type
            "LIC123",       # licensee_id
            "Test Entity",  # entity_name
            "John",         # first_name
            "M",            # mi
            "Doe",          # last_name
            "Jr",           # suffix
            "1234567890",   # phone
            "9876543210",   # fax
            "test@example.com",  # email
            "123 Main St",  # street_address
            "Anytown",      # city
            "CA",           # state
            "12345",        # zip_code
            "",            # po_box
            "",            # attention_line
            "ABC",         # sgin
            "1234567890",  # frn
            "I",           # applicant_type_code
            "",            # applicant_type_code_other
            "A",           # status_code
            "01/15/2024",  # status_date
            "B",           # license_type
            "987654321",   # linked_system_id
            "K1XYZ"        # linked_call_sign
        ]
    )
    
    mapper = MapperFactory.create_mapper("EN")
    result = mapper(record)
    
    assert result.record_type == "EN"
    assert result.system_id == 123456789
    assert result.entity_type == "I"
    assert result.entity_name == "Test Entity"
    assert result.status_date == date(2024, 1, 15)

def test_en_mapper_invalid_date():
    """Test invalid date handling"""
    record = Record(
        line=1,
        fields=["EN", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "13/99/2024"]  # Invalid date
    )
    
    with pytest.raises(MapperError) as exc:
        mapper = MapperFactory.create_mapper("EN")
        mapper(record)
    assert "status_date" in str(exc.value)

def test_mapper_factory():
    """Test mapper factory functionality"""
    # Valid mapper types
    assert MapperFactory.create_mapper("AM") is not None
    assert MapperFactory.create_mapper("EN") is not None
    
    # Invalid mapper type
    assert MapperFactory.create_mapper("XX") is None

def test_field_count_validation():
    """Test field count validation"""
    # Too few fields for AM record
    record = Record(line=1, fields=["AM"])
    
    with pytest.raises(MapperError) as exc:
        mapper = MapperFactory.create_mapper("AM")
        mapper(record)
    assert "fields" in str(exc.value)

def test_optional_fields():
    """Test handling of optional fields"""
    record = Record(
        line=1,
        fields=["AM", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    )
    
    mapper = MapperFactory.create_mapper("AM")
    result = mapper(record)
    
    assert result.system_id is None
    assert result.call_sign is None
    assert result.operator_class is None

def test_error_context():
    """Test error message context"""
    record = Record(line=42, fields=["AM", "invalid"])
    
    with pytest.raises(MapperError) as exc:
        mapper = MapperFactory.create_mapper("AM")
        mapper(record)
    
    error = str(exc.value)
    assert "line 42" in error
    assert "invalid" in error
