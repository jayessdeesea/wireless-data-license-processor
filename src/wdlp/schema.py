from datetime import date
from typing import Optional
from pydantic import BaseModel, Field

class AMRecord(BaseModel):
    """Amateur License record schema"""
    record_type: Optional[str] = Field(
        None,
        min_length=2,
        max_length=2,
        pattern="^AM$",
        description="Record type identifier, must be 'AM'"
    )
    system_id: Optional[int] = Field(
        None,
        ge=0,
        le=999999999,
        description="Unique system identifier (0-999999999)"
    )
    uls_file_number: Optional[str] = Field(None, min_length=14, max_length=14)
    ebf_number: Optional[str] = Field(None, max_length=30)
    call_sign: Optional[str] = Field(None, min_length=10, max_length=10)
    operator_class: Optional[str] = Field(None, min_length=1, max_length=1)
    group_code: Optional[str] = Field(None, min_length=1, max_length=1)
    region_code: Optional[int] = Field(None, ge=0, le=255)
    trustee_call_sign: Optional[str] = Field(None, min_length=10, max_length=10)
    trustee_indicator: Optional[str] = Field(None, min_length=1, max_length=1)
    physician_certification: Optional[str] = Field(None, min_length=1, max_length=1)
    ve_signature: Optional[str] = Field(None, min_length=1, max_length=1)
    systematic_call_sign_change: Optional[str] = Field(None, min_length=1, max_length=1)
    vanity_call_sign_change: Optional[str] = Field(None, min_length=1, max_length=1)
    vanity_relationship: Optional[str] = Field(None, min_length=12, max_length=12)
    previous_call_sign: Optional[str] = Field(None, min_length=10, max_length=10)
    previous_operator_class: Optional[str] = Field(None, min_length=1, max_length=1)
    trustee_name: Optional[str] = Field(None, max_length=50)

class ENRecord(BaseModel):
    """Entity record schema"""
    record_type: Optional[str] = Field(
        None,
        min_length=2,
        max_length=2,
        pattern="^EN$",
        description="Record type identifier, must be 'EN'"
    )
    system_id: Optional[int] = Field(
        None,
        ge=0,
        le=999999999,
        description="Unique system identifier (0-999999999)"
    )
    uls_file_number: Optional[str] = Field(None, min_length=14, max_length=14)
    ebf_number: Optional[str] = Field(None, max_length=30)
    call_sign: Optional[str] = Field(None, min_length=10, max_length=10)
    entity_type: Optional[str] = Field(None, min_length=2, max_length=2)
    licensee_id: Optional[str] = Field(None, min_length=9, max_length=9)
    entity_name: Optional[str] = Field(None, max_length=200)
    first_name: Optional[str] = Field(None, max_length=20)
    mi: Optional[str] = Field(None, min_length=1, max_length=1)
    last_name: Optional[str] = Field(None, max_length=20)
    suffix: Optional[str] = Field(None, min_length=3, max_length=3)
    phone: Optional[str] = Field(None, min_length=10, max_length=10)
    fax: Optional[str] = Field(None, min_length=10, max_length=10)
    email: Optional[str] = Field(None, max_length=50)
    street_address: Optional[str] = Field(None, max_length=60)
    city: Optional[str] = Field(None, max_length=20)
    state: Optional[str] = Field(None, min_length=2, max_length=2)
    zip_code: Optional[str] = Field(None, min_length=9, max_length=9)
    po_box: Optional[str] = Field(None, max_length=20)
    attention_line: Optional[str] = Field(None, max_length=35)
    sgin: Optional[str] = Field(None, min_length=3, max_length=3)
    frn: Optional[str] = Field(None, min_length=10, max_length=10)
    applicant_type_code: Optional[str] = Field(None, min_length=1, max_length=1)
    applicant_type_code_other: Optional[str] = Field(None, max_length=40)
    status_code: Optional[str] = Field(None, min_length=1, max_length=1)
    status_date: Optional[date] = Field(None)
    license_type: Optional[str] = Field(None, min_length=1, max_length=1)
    linked_system_id: Optional[int] = Field(None, ge=0, le=999999999)
    linked_call_sign: Optional[str] = Field(None, min_length=10, max_length=10)
