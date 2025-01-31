"""Tests for .dat file reading and parsing"""
import logging
import pytest
from io import StringIO
from wdlp.reader import PullParser, ParseError, Record

def test_basic_record():
    """Test basic record parsing"""
    data = "AM|123456789|W1AW|A\n"
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert isinstance(record, Record)
    assert record.line == 1
    assert record.fields == ["AM", "123456789", "W1AW", "A"]

def test_empty_fields():
    """Test handling of empty fields - pipes separate fields"""
    data = "AM|||\n"  # Creates four fields: "AM" and three empty fields
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert record.fields == ["AM", "", "", ""]

def test_empty_record():
    """Test handling of records with no fields"""
    data = "|\n"  # Two empty fields is not valid
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    error = exc.value
    assert error.line == 1
    assert "minimum 1 field per record" in str(error)
    assert error.context == "Record must contain at least one field"

def test_consecutive_pipes():
    """Test handling of consecutive pipes with CRLF - pipes separate fields"""
    # Test ||\r\n sequence which creates three empty fields
    data = "field1||\r\n"
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert record.fields == ["field1", "", ""]

    # Test multiple consecutive empty fields
    data = "field1||||\r\n"
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert record.fields == ["field1", "", "", "", ""]

def test_multiple_records():
    """Test parsing multiple records"""
    data = "AM|123|A\nEN|456|B\n"
    parser = PullParser(StringIO(data))
    records = list(parser)
    assert len(records) == 2
    assert records[0].fields == ["AM", "123", "A"]
    assert records[1].fields == ["EN", "456", "B"]
    assert records[0].line == 1
    assert records[1].line == 2

def test_line_endings():
    """Test different line ending formats"""
    # Unix style (LF)
    data = "AM|1\nEN|2\n"
    parser = PullParser(StringIO(data))
    records = list(parser)
    assert len(records) == 2
    assert records[0].fields == ["AM", "1"]
    assert records[1].fields == ["EN", "2"]

    # Windows style (CRLF)
    data = "AM|1\r\nEN|2\r\n"
    parser = PullParser(StringIO(data))
    records = list(parser)
    assert len(records) == 2
    assert records[0].fields == ["AM", "1"]
    assert records[1].fields == ["EN", "2"]

def test_field_validation():
    """Test field length and content validation"""
    # Field too long (>1024 bytes)
    data = f"AM|{'x' * 1025}\n"  # Exceeds MAX_FIELD_LENGTH
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    error = exc.value
    assert error.line == 1
    assert error.column == 1028  # AM| (3) + 1025 chars
    assert error.consumed_fields == ["AM"]
    assert len(error.partial_field) == 1024  # Should contain up to max length
    assert "field length" in str(error)

    # Too many fields (>256)
    data = "|".join(["x"] * 257) + "\n"
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    error = exc.value
    assert error.line == 1
    assert len(error.consumed_fields) == 256  # Should have all fields up to the limit
    assert "field count" in str(error)

def test_malformed_input():
    """Test handling of malformed input"""
    # Missing record terminator
    data = "AM|123"  # No newline
    parser = PullParser(StringIO(data))
    record = next(parser)  # Should complete record at EOF
    assert record.fields == ["AM", "123"]

def test_empty_file():
    """Test handling of empty files"""
    parser = PullParser(StringIO(""))
    records = list(parser)
    assert len(records) == 0

def test_iterator_protocol():
    """Test iterator protocol implementation"""
    data = "AM|123\n"
    parser = PullParser(StringIO(data))
    
    # Test __iter__
    assert iter(parser) is parser
    
    # Test __next__
    record = next(parser)
    assert isinstance(record, Record)
    assert record.fields == ["AM", "123"]
    
    # Test StopIteration
    with pytest.raises(StopIteration):
        next(parser)

def test_record_line_tracking():
    """Test line number tracking in records"""
    data = "AM|1\n\nEN|2\n"  # Note the blank line
    parser = PullParser(StringIO(data))
    records = list(parser)
    assert records[0].line == 1
    assert records[1].line == 3  # After blank line

def test_error_context():
    """Test error reporting context"""
    data = "AM|123|invalid\nEN|456\n"
    parser = PullParser(StringIO(data))
    records = list(parser)  # Should succeed since newlines are allowed
    assert len(records) == 2
    assert records[0].fields == ["AM", "123", "invalid"]
    assert records[1].fields == ["EN", "456"]

def test_column_tracking():
    """Test column number tracking in errors"""
    # Test column tracking for field length error
    data = f"AM|{'x' * 1025}\n"
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    error = exc.value
    assert error.column == 1028  # AM| (3) + 1025 chars

    # Test column tracking for field count error
    data = "A|" * 256 + "overflow\n"
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    error = exc.value
    assert error.column > 0
    assert len(error.consumed_fields) == 256

def test_single_field_record():
    """Test parsing a record with a single field"""
    data = "field1\n"
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert record.fields == ["field1"]

def test_two_field_record():
    """Test parsing a record with two fields separated by a pipe"""
    data = "field1|field2\n"
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert record.fields == ["field1", "field2"]

def test_empty_fields_with_pipes():
    """Test parsing a record with empty fields created by pipes"""
    data = "|||\n"
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert record.fields == ["", "", "", ""]

def test_verbose_mode(caplog):
    """Test verbose mode logging of state transitions and inputs"""
    caplog.set_level(logging.DEBUG)
    
    # Test basic state transitions
    data = "AM|123\n"
    parser = PullParser(StringIO(data), verbose=True)
    record = next(parser)
    
    # Verify state transitions are logged
    logs = [record.message for record in caplog.records if record.levelname == 'DEBUG']
    
    # Should see input characters
    assert any("Input char: 'A'" in msg for msg in logs)
    assert any("Input char: 'M'" in msg for msg in logs)
    assert any("Input char: '|'" in msg for msg in logs)
    
    # Should see state transitions
    assert any("State transition: START -> FIELD" in msg for msg in logs)
    assert any("State transition: FIELD -> AFTER_PIPE" in msg for msg in logs)
    
    # Should see field content in state transitions
    assert any("Current field: A" in msg for msg in logs)
    assert any("Current field: 1" in msg for msg in logs)
    
    # Should see completed record
    assert any("Completed record at line 1" in msg for msg in logs)
    assert any("Fields: ['AM', '123']" in msg for msg in logs)

def test_partial_field_content():
    """Test partial field content in errors"""
    # Test partial content for long field
    data = "AM|start_of_very_long_field" + "x" * 1024 + "\n"
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    error = exc.value
    assert error.consumed_fields == ["AM"]
    assert error.partial_field.startswith("start_of_very_long_field")
    assert len(error.partial_field) == 1024  # Should be truncated at max length

    # Test partial content for malformed record
    data = "AM|incomplete_field"  # No newline
    parser = PullParser(StringIO(data))
    record = next(parser)  # Should complete record at EOF
    assert record.fields == ["AM", "incomplete_field"]
