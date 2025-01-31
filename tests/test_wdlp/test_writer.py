"""Tests for output generation"""
import pytest
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date
from tempfile import TemporaryDirectory
from wdlp.writer import (
    WriterFactory,
    WriterError,
    JSONLWriter,
    ParquetWriter,
    IonWriter,
    CSVWriter
)

@pytest.fixture
def test_records():
    """Sample records for testing"""
    return [
        {
            "record_type": "AM",
            "system_id": 123456789,
            "call_sign": "W1AW",
            "operator_class": "A",
            "status_date": date(2024, 1, 15)
        },
        {
            "record_type": "AM",
            "system_id": 987654321,
            "call_sign": "K1ABC",
            "operator_class": "E",
            "status_date": None
        }
    ]

def test_jsonl_writer(test_records):
    """Test JSONL format writing"""
    with TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.jsonl"
        
        # Write records
        with JSONLWriter(output_path) as writer:
            for record in test_records:
                writer.write(record)
        
        # Verify output
        with open(output_path) as f:
            lines = f.readlines()
            assert len(lines) == len(test_records)
            
            # Parse and verify each line
            for line, expected in zip(lines, test_records):
                record = json.loads(line)
                assert record["record_type"] == expected["record_type"]
                assert record["system_id"] == expected["system_id"]
                # Date should be serialized as ISO format string
                if expected["status_date"]:
                    assert record["status_date"] == expected["status_date"].isoformat()

def test_parquet_writer(test_records):
    """Test Parquet format writing"""
    with TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"
        
        # Write records
        with ParquetWriter(output_path) as writer:
            for record in test_records:
                writer.write(record)
        
        # Verify output
        table = pq.read_table(output_path)
        assert len(table) == len(test_records)
        
        # Check schema
        schema = table.schema
        assert schema.field("record_type").type == pa.string()
        assert schema.field("system_id").type == pa.int64()
        assert schema.field("status_date").type == pa.date32()
        
        # Check data
        df = table.to_pandas()
        assert df.record_type.tolist() == [r["record_type"] for r in test_records]
        assert df.system_id.tolist() == [r["system_id"] for r in test_records]

def test_csv_writer(test_records):
    """Test CSV format writing"""
    with TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.csv"
        
        # Write records
        with CSVWriter(output_path) as writer:
            for record in test_records:
                writer.write(record)
        
        # Verify output
        with open(output_path) as f:
            lines = f.readlines()
            # Header + data lines
            assert len(lines) == len(test_records) + 1
            
            # Check header
            header = lines[0].strip().split(",")
            assert "record_type" in header
            assert "system_id" in header
            
            # Check data
            data = [line.strip().split(",") for line in lines[1:]]
            assert data[0][header.index("record_type")] == test_records[0]["record_type"]
            assert data[0][header.index("system_id")] == str(test_records[0]["system_id"])

def test_writer_factory():
    """Test writer factory functionality"""
    with TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir) / "test"
        
        # Test supported formats
        assert isinstance(
            WriterFactory.create_writer("jsonl", base_path.with_suffix(".jsonl")),
            JSONLWriter
        )
        assert isinstance(
            WriterFactory.create_writer("parquet", base_path.with_suffix(".parquet")),
            ParquetWriter
        )
        assert isinstance(
            WriterFactory.create_writer("csv", base_path.with_suffix(".csv")),
            CSVWriter
        )
        
        # Test unknown format
        with pytest.raises(ValueError):
            WriterFactory.create_writer("unknown", base_path)

def test_atomic_operations():
    """Test atomic file operations"""
    with TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.jsonl"
        
        # Test successful write
        with JSONLWriter(output_path) as writer:
            writer.write({"test": "data"})
        assert output_path.exists()
        
        # Test failed write
        with pytest.raises(Exception):
            with JSONLWriter(output_path) as writer:
                writer.write({"test": "data"})
                raise Exception("Test error")
        
        # Original file should still exist and be unchanged
        with open(output_path) as f:
            assert f.read().strip() == '{"test": "data"}'

def test_type_conversion():
    """Test type conversion in different formats"""
    record = {
        "string": "test",
        "integer": 42,
        "float": 3.14,
        "date": date(2024, 1, 15),
        "none": None
    }
    
    with TemporaryDirectory() as tmpdir:
        # Test JSONL
        jsonl_path = Path(tmpdir) / "test.jsonl"
        with JSONLWriter(jsonl_path) as writer:
            writer.write(record)
        
        with open(jsonl_path) as f:
            data = json.loads(f.read())
            assert data["string"] == "test"
            assert data["integer"] == 42
            assert data["float"] == 3.14
            assert data["date"] == "2024-01-15"
            assert data["none"] is None
        
        # Test CSV
        csv_path = Path(tmpdir) / "test.csv"
        with CSVWriter(csv_path) as writer:
            writer.write(record)
        
        with open(csv_path) as f:
            lines = f.readlines()
            data = lines[1].strip().split(",")
            header = lines[0].strip().split(",")
            values = dict(zip(header, data))
            assert values["string"] == "test"
            assert values["integer"] == "42"
            assert values["float"] == "3.14"
            assert values["date"] == "2024-01-15"
            assert values["none"] == ""

def test_error_handling():
    """Test error handling in writers"""
    with TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.jsonl"
        
        # Test invalid record followed by valid record
        writer = JSONLWriter(output_path)
        with writer:
            # First write should fail with unserializable object
            with pytest.raises(WriterError) as exc:
                writer.write({"invalid": object()})  # Unserializable object
            assert "serialization" in str(exc.value)
        
        # Try to write valid data in a new context - should fail due to error state
        with pytest.raises(WriterError) as exc:
            with writer:
                writer.write({"test": "data"})
        assert "error state" in str(exc.value)
        
        # Test file permission error
        readonly_path = Path(tmpdir) / "readonly"
        readonly_path.mkdir()
        readonly_path.chmod(0o444)  # Read-only
        
        with pytest.raises(WriterError) as exc:
            with JSONLWriter(readonly_path / "test.jsonl") as writer:
                writer.write({"test": "data"})
        assert "permission" in str(exc.value).lower()

def test_large_dataset(test_records):
    """Test handling of larger datasets"""
    # Generate more test records
    records = test_records * 1000  # 2000 records
    
    with TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"
        
        # Write records
        with ParquetWriter(output_path) as writer:
            for record in records:
                writer.write(record)
        
        # Verify output
        table = pq.read_table(output_path)
        assert len(table) == len(records)
        
        # Check data integrity
        df = table.to_pandas()
        assert all(df.record_type == [r["record_type"] for r in records])
        assert all(df.system_id == [r["system_id"] for r in records])
