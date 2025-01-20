import pytest
from wdlp.schema.untyped_utils import UntypedField, UntypedRecord, UntypedRecordToTypedRecordMapper
from wdlp.schema.schemas import AMRecord

def test_untyped_field():
    field = UntypedField("TestValue")
    assert field.get_value() == "TestValue"

def test_untyped_record():
    fields = [UntypedField("Field1"), UntypedField("Field2")]
    record = UntypedRecord(fields)

    assert record.get_field(0) == "Field1"
    assert record.get_field(1) == "Field2"

    with pytest.raises(ValueError):
        record.get_field(2)  # Index out of range

def test_untyped_to_typed_mapping():
    fields = [
        UntypedField("AM"),
        UntypedField("123456789"),
        UntypedField("12345678901234"),
        UntypedField("EBF1234567890123456789012345"),
        UntypedField("CALLSIGN"),
        UntypedField("A"),
        UntypedField("B"),
        UntypedField("10"),
        UntypedField("TRUSTCALL"),
        UntypedField("Y"),
        UntypedField("N"),
        UntypedField("Y"),
        UntypedField("Y"),
        UntypedField("N"),
        UntypedField("Vanity"),
        UntypedField("PREVCALL"),
        UntypedField("B"),
        UntypedField("Trustee Name")
    ]

    record = UntypedRecord(fields)
    mapper = UntypedRecordToTypedRecordMapper(AMRecord)

    field_mapping = list(range(len(fields)))
    typed_record = mapper.map(record, field_mapping)

    assert typed_record.Record_Type == "AM"
    assert typed_record.Unique_System_Identifier == 123456789
    assert typed_record.ULS_File_Number == "12345678901234"
    assert typed_record.Call_Sign == "CALLSIGN"
    assert typed_record.Trustee_Name == "Trustee Name"

def test_untyped_to_typed_mapping_invalid():
    fields = [UntypedField("Invalid")]
    record = UntypedRecord(fields)
    mapper = UntypedRecordToTypedRecordMapper(AMRecord)

    field_mapping = [0]  # Map only one field
    with pytest.raises(ValueError):
        mapper.map(record, field_mapping)
