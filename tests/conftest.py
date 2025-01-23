"""Shared test fixtures and configuration"""
import pytest
from pathlib import Path
from datetime import date
from wdlp.schema import AMRecord, ENRecord

@pytest.fixture
def sample_am_record():
    """Create a sample Amateur License record"""
    return AMRecord(
        record_type="AM",
        system_id=123456789,
        uls_file_number="ULS123456789",
        ebf_number="EBF987654321",
        call_sign="W1AW",
        operator_class="A",
        group_code="C",
        region_code=1,
        trustee_call_sign="K1ABC",
        trustee_indicator="Y",
        physician_certification="N",
        ve_signature="Y",
        systematic_call_sign_change="N",
        vanity_call_sign_change="N",
        trustee_name="John Doe"
    )

@pytest.fixture
def sample_en_record():
    """Create a sample Entity record"""
    return ENRecord(
        record_type="EN",
        system_id=987654321,
        uls_file_number="ULS987654321",
        ebf_number="EBF123456789",
        call_sign="K1ABC",
        entity_type="I",
        licensee_id="LIC123456",
        entity_name="Test Entity",
        first_name="John",
        mi="M",
        last_name="Doe",
        suffix="Jr",
        phone="1234567890",
        email="test@example.com",
        street_address="123 Main St",
        city="Anytown",
        state="CA",
        zip_code="12345",
        status_code="A",
        status_date=date(2024, 1, 15)
    )

@pytest.fixture
def data_dir(tmp_path):
    """Create a temporary data directory with test files"""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
    # Create test .dat files
    (data_dir / "AM.dat").write_text(
        "AM|123456789|ULS123|EBF456|W1AW|A|\n"
        "AM|987654321|ULS789|EBF012|K1ABC|E|\n"
    )
    
    (data_dir / "EN.dat").write_text(
        "EN|123456789|ULS123|EBF456|W1AW|I|LIC123|Test Entity|\n"
        "EN|987654321|ULS789|EBF012|K1ABC|B|LIC456|Another Entity|\n"
    )
    
    return data_dir

@pytest.fixture
def output_dir(tmp_path):
    """Create a temporary output directory"""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir
