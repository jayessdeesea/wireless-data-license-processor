import pytest
from io import StringIO
from wdlp.reader.readers import DatToRecordIterator
from wdlp.schema.untyped_utils import UntypedRecord

def test_dat_to_record_iterator_single_record():
    dat_content = "Field1|Field2|Field3|\n"
    iterator = DatToRecordIterator(StringIO(dat_content))

    records = list(iterator)
    assert len(records) == 1
    assert isinstance(records[0], UntypedRecord)
    assert records[0].get_field(0) == "Field1"
    assert records[0].get_field(1) == "Field2"
    assert records[0].get_field(2) == "Field3"

def test_dat_to_record_iterator_multiple_records():
    dat_content = "Field1|Field2|Field3|\nFieldA|FieldB|FieldC|\n"
    iterator = DatToRecordIterator(StringIO(dat_content))

    records = list(iterator)
    assert len(records) == 2

    assert records[0].get_field(0) == "Field1"
    assert records[0].get_field(1) == "Field2"
    assert records[0].get_field(2) == "Field3"

    assert records[1].get_field(0) == "FieldA"
    assert records[1].get_field(1) == "FieldB"
    assert records[1].get_field(2) == "FieldC"

def test_dat_to_record_iterator_line_without_pipe():
    dat_content = "Field1|Field2|Field3\n"
    iterator = DatToRecordIterator(StringIO(dat_content))

    with pytest.raises(ValueError, match="Record does not end with a pipe.*"):
        list(iterator)

def test_dat_to_record_iterator_empty_lines():
    dat_content = "\nField1|Field2|Field3|\n\nFieldA|FieldB|FieldC|\n\n"
    iterator = DatToRecordIterator(StringIO(dat_content))

    records = list(iterator)
    assert len(records) == 2

    assert records[0].get_field(0) == "Field1"
    assert records[0].get_field(1) == "Field2"
    assert records[0].get_field(2) == "Field3"

    assert records[1].get_field(0) == "FieldA"
    assert records[1].get_field(1) == "FieldB"
    assert records[1].get_field(2) == "FieldC"
