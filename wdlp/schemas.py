from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class ENSchema(BaseModel):
    record_type: str = Field(..., min_length=2, max_length=2, alias="Record Type")
    unique_system_identifier: Optional[int] = Field(None, alias="Unique System Identifier")
    uls_file_number: Optional[str] = Field(None, max_length=14, alias="ULS File Number")
    ebf_number: Optional[str] = Field(None, max_length=30, alias="EBF Number")
    call_sign: Optional[str] = Field(None, max_length=10, alias="Call Sign")
    entity_type: Optional[str] = Field(None, max_length=2, alias="Entity Type")
    licensee_id: Optional[str] = Field(None, max_length=9, alias="Licensee ID")
    entity_name: Optional[str] = Field(None, max_length=200, alias="Entity Name")
    first_name: Optional[str] = Field(None, max_length=20, alias="First Name")
    mi: Optional[str] = Field(None, max_length=1, alias="MI")
    last_name: Optional[str] = Field(None, max_length=20, alias="Last Name")
    suffix: Optional[str] = Field(None, max_length=3, alias="Suffix")
    phone: Optional[str] = Field(None, max_length=10, alias="Phone")
    fax: Optional[str] = Field(None, max_length=10, alias="Fax")
    email: Optional[str] = Field(None, max_length=50, alias="Email")
    street_address: Optional[str] = Field(None, max_length=60, alias="Street Address")
    city: Optional[str] = Field(None, max_length=20, alias="City")
    state: Optional[str] = Field(None, max_length=2, alias="State")
    zip_code: Optional[str] = Field(None, max_length=9, alias="Zip Code")
    po_box: Optional[str] = Field(None, max_length=20, alias="PO Box")
    attention_line: Optional[str] = Field(None, max_length=35, alias="Attention Line")
    sgin: Optional[str] = Field(None, max_length=3, alias="SGIN")
    fcc_registration_number: Optional[str] = Field(None, max_length=10, alias="FCC Registration Number (FRN)")
    applicant_type_code: Optional[str] = Field(None, max_length=1, alias="Applicant Type Code")
    applicant_type_code_other: Optional[str] = Field(None, max_length=40, alias="Applicant Type Code Other")
    status_code: Optional[str] = Field(None, max_length=1, alias="Status Code")
    status_date: Optional[datetime] = Field(None, alias="Status Date")

    @staticmethod
    def validate_date(value, line_number):
        if value:
            try:
                return datetime.strptime(value, "%m/%d/%Y")
            except ValueError:
                raise ValueError(f"Invalid date format at line {line_number}. Expected MM/DD/YYYY, not {value}.")
        return value

    @staticmethod
    def combine_multiline(records: List[str]) -> str:
        return "\n".join([line.rstrip() for line in records]).strip()

class FASchema(BaseModel):
    record_type: str = Field(..., min_length=2, max_length=2, alias="Record Type")
    unique_system_identifier: Optional[int] = Field(None, alias="Unique System Identifier")
    uls_file_number: Optional[str] = Field(None, max_length=14, alias="ULS File Number")
    ebf_number: Optional[str] = Field(None, max_length=30, alias="EBF Number")
    call_sign: Optional[str] = Field(None, max_length=10, alias="Call Sign")
    operator_class_code: Optional[str] = Field(None, max_length=2, alias="Operator Class Code")
    ship_radar_endorsement: Optional[str] = Field(None, max_length=1, alias="Ship Radar Endorsement")
    six_month_endorsement: Optional[str] = Field(None, max_length=1, alias="Six Month Endorsement")
    date_of_birth: Optional[datetime] = Field(None, alias="Date of Birth")
    certification_not_restricted: Optional[str] = Field(None, max_length=1, alias="Certification Not Restricted")
    certification_restricted_permit: Optional[str] = Field(None, max_length=1, alias="Certification Restricted Permit")
    certification_restricted_permit_limited_use: Optional[str] = Field(None, max_length=1, alias="Certification Restricted Permit Limited Use")
    cole_manager_code: Optional[str] = Field(None, max_length=5, alias="Cole Manager Code")
    dm_call_sign: Optional[str] = Field(None, max_length=10, alias="DM Call Sign")
    valid_proof_of_passing: Optional[str] = Field(None, max_length=1, alias="Valid Proof of Passing")
    description: Optional[str] = Field(None, alias="Description")
    status_date: Optional[datetime] = Field(None, alias="Status Date")

    @staticmethod
    def validate_date(value, line_number):
        if value:
            try:
                return datetime.strptime(value, "%m/%d/%Y")
            except ValueError:
                raise ValueError(f"Invalid date format at line {line_number}. Expected MM/DD/YYYY, not {value}.")
        return value

    @staticmethod
    def combine_multiline(records: List[str]) -> str:
        return "\n".join([line.rstrip() for line in records]).strip()
