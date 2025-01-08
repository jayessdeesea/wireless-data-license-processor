from pydantic import BaseModel, Field
from typing import Optional

# Schema for EN record type
class ENSchema(BaseModel):
    record_type: Optional[str] = Field(default=None, max_length=2)
    unique_system_identifier: Optional[int]
    uls_file_number: Optional[str] = Field(default=None, max_length=14)
    ebf_number: Optional[str] = Field(default=None, max_length=30)
    call_sign: Optional[str] = Field(default=None, max_length=10)
    entity_type: Optional[str] = Field(default=None, max_length=2)
    licensee_id: Optional[str] = Field(default=None, max_length=9)
    entity_name: Optional[str] = Field(default=None, max_length=200)
    first_name: Optional[str] = Field(default=None, max_length=20)
    mi: Optional[str] = Field(default=None, max_length=1)
    last_name: Optional[str] = Field(default=None, max_length=20)
    suffix: Optional[str] = Field(default=None, max_length=3)
    phone: Optional[str] = Field(default=None, max_length=10)
    fax: Optional[str] = Field(default=None, max_length=10)
    email: Optional[str] = Field(default=None, max_length=50)
    street_address: Optional[str] = Field(default=None, max_length=60)
    city: Optional[str] = Field(default=None, max_length=20)
    state: Optional[str] = Field(default=None, max_length=2)
    zip_code: Optional[str] = Field(default=None, max_length=9)
    po_box: Optional[str] = Field(default=None, max_length=20)
    attention_line: Optional[str] = Field(default=None, max_length=35)
    sgin: Optional[str] = Field(default=None, max_length=3)
    fcc_registration_number: Optional[str] = Field(default=None, max_length=10)
    applicant_type_code: Optional[str] = Field(default=None, max_length=1)
    applicant_type_code_other: Optional[str] = Field(default=None, max_length=40)
    status_code: Optional[str] = Field(default=None, max_length=1)
    status_date: Optional[str]
    linked_unique_system_identifier: Optional[int]
    linked_call_sign: Optional[str] = Field(default=None, max_length=10)

# Schema for AM record type
class AMSchema(BaseModel):
    record_type: Optional[str] = Field(default=None, max_length=2)
    unique_system_identifier: Optional[int]
    uls_file_number: Optional[str] = Field(default=None, max_length=14)
    ebf_number: Optional[str] = Field(default=None, max_length=30)
    call_sign: Optional[str] = Field(default=None, max_length=10)
    operator_class: Optional[str] = Field(default=None, max_length=1)
    group_code: Optional[str] = Field(default=None, max_length=1)
    region_code: Optional[int]
    trustee_call_sign: Optional[str] = Field(default=None, max_length=10)
    trustee_indicator: Optional[str] = Field(default=None, max_length=1)
    physician_certification: Optional[str] = Field(default=None, max_length=1)
    ve_signature: Optional[str] = Field(default=None, max_length=1)
    systematic_call_sign_change: Optional[str] = Field(default=None, max_length=1)
    vanity_call_sign_change: Optional[str] = Field(default=None, max_length=1)
    vanity_relationship: Optional[str] = Field(default=None, max_length=12)
    previous_call_sign: Optional[str] = Field(default=None, max_length=10)
    previous_operator_class: Optional[str] = Field(default=None, max_length=1)
    trustee_name: Optional[str] = Field(default=None, max_length=50)
