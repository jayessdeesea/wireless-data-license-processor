"""Tests for .dat file reading and parsing"""
import pytest
from io import StringIO
from wdlp.reader import PullParser, ParseError, Record

def test_basic_record():
    """Test basic record parsing"""
    data = "AM|123456789|W1AW|A|\n"
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert isinstance(record, Record)
    assert record.line == 1
    assert record.fields == ["AM", "123456789", "W1AW", "A"]

def test_empty_fields():
    """Test handling of empty fields"""
    data = "AM|||\n"
    parser = PullParser(StringIO(data))
    record = next(parser)
    assert record.fields == ["AM", "", "", ""]

def test_multiple_records():
    """Test parsing multiple records"""
    data = "AM|123|A|\nEN|456|B|\n"
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
    data = "AM|1|\nEN|2|\n"
    parser = PullParser(StringIO(data))
    records = list(parser)
    assert len(records) == 2

    # Windows style (CRLF)
    data = "AM|1|\r\nEN|2|\r\n"
    parser = PullParser(StringIO(data))
    records = list(parser)
    assert len(records) == 2

def test_field_validation():
    """Test field length and content validation"""
    # Field too long (>1024 bytes)
    data = f"AM|{'x' * 1025}|\n"
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    assert "field length" in str(exc.value)

    # Too many fields (>256)
    data = "|".join(["x"] * 257) + "|\n"
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    assert "fields" in str(exc.value)

def test_malformed_input():
    """Test handling of malformed input"""
    # Missing field terminator
    data = "AM|123"  # No final | and no newline
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    assert "terminator" in str(exc.value)

    # Missing record terminator
    data = "AM|123|"  # No newline
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    assert "terminator" in str(exc.value)

def test_embedded_newlines():
    """Test handling of newlines within fields"""
    # Newlines should not be allowed in fields
    data = "AM|line1\nline2|test|\n"
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        next(parser)
    assert "newline" in str(exc.value)

def test_empty_file():
    """Test handling of empty files"""
    parser = PullParser(StringIO(""))
    records = list(parser)
    assert len(records) == 0

def test_iterator_protocol():
    """Test iterator protocol implementation"""
    data = "AM|123|\n"
    parser = PullParser(StringIO(data))
    
    # Test __iter__
    assert iter(parser) is parser
    
    # Test __next__
    record = next(parser)
    assert isinstance(record, Record)
    
    # Test StopIteration
    with pytest.raises(StopIteration):
        next(parser)

def test_record_line_tracking():
    """Test line number tracking in records"""
    data = "AM|1|\n\nEN|2|\n"  # Note the blank line
    parser = PullParser(StringIO(data))
    records = list(parser)
    assert records[0].line == 1
    assert records[1].line == 3  # After blank line

def test_error_context():
    """Test error reporting context"""
    data = "AM|123|invalid\nEN|456|\n"
    parser = PullParser(StringIO(data))
    with pytest.raises(ParseError) as exc:
        list(parser)  # Try to parse all records
    error = exc.value
    assert error.line == 1  # Error on first line
    assert "invalid" in str(error)  # Error message includes context
