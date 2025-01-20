import pytest
from pydantic import ValidationError
from wdlp.schema.schemas import AMRecord, ENRecord

def test_am_record_valid():
    record = AMRecord(
        Record_Type="AM",
        Unique_System_Identifier=123456789,
        ULS_File_Number="12345678901234",
        EBF_Number="EBF1234567890123456789012345",
        Call_Sign="CALLSIGN",
        Operator_Class="A",
        Group_Code="B",
        Region_Code=10,
        Trustee_Call_Sign="TRUSTCALL",
        Trustee_Indicator="Y",
        Physician_Certification="N",
        VE_Signature="Y",
        Systematic_Call_Sign_Change="Y",
        Vanity_Call_Sign_Change="N",
        Vanity_Relationship="Vanity",
        Previous_Call_Sign="PREVCALL",
        Previous_Operator_Class="B",
        Trustee_Name="Trustee Name"
    )
    assert record.Record_Type == "AM"

def test_am_record_invalid_field():
    with pytest.raises(ValidationError):
        AMRecord(
            Record_Type="A",  # Invalid length
            Unique_System_Identifier=-1  # Out of range
        )

def test_en_record_valid():
    record = ENRecord(
        Record_Type="EN",
        Unique_System_Identifier=987654321,
        ULS_File_Number="98765432101234",
        EBF_Number="EBF9876543210987654321098765",
        Call_Sign="CALLSIGN",
        Entity_Type="ET",
        Licensee_ID="LIC123456",
        Entity_Name="Entity Name",
        First_Name="First",
        MI="M",
        Last_Name="Last",
        Suffix="Sr",
        Phone="1234567890",
        Fax="0987654321",
        Email="example@example.com",
        Street_Address="123 Main St",
        City="Cityname",
        State="ST",
        Zip_Code="123456789",
        PO_Box="PO123",
        Attention_Line="Attention",
        SGIN="SG1",
        FCC_Registration_Number="FCC1234567",
        Applicant_Type_Code="A",
        Applicant_Type_Code_Other="Other Code",
        Status_Code="S",
        Status_Date="01/01/2025",
        GHz_License_Type="G",
        Linked_Unique_System_Identifier=123456789,
        Linked_Call_Sign="LINKCALL"
    )
    assert record.Record_Type == "EN"

def test_en_record_invalid_field():
    with pytest.raises(ValidationError):
        ENRecord(
            Record_Type="E",  # Invalid length
            Phone="123"  # Invalid length
        )
