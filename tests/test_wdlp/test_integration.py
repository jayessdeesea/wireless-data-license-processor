"""Integration tests for the application"""
import pytest
import json
import zipfile
from pathlib import Path
import time
import sys
from wdlp.main import main

@pytest.mark.integration
def test_help_output(capsys):
    """Test help output"""
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    
    captured = capsys.readouterr()
    assert "--input" in captured.out
    assert "--output" in captured.out
    assert "--format" in captured.out

@pytest.mark.integration
def test_end_to_end_processing(data_dir, output_dir):
    """Test end-to-end processing"""
    # Create test ZIP file
    zip_path = data_dir / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("AM.dat", "AM|123456789|ULS123|EBF456|W1AW|A|\n")
        zf.writestr("EN.dat", "EN|987654321|ULS789|EBF012|K1ABC|I|LIC123|Test Entity|\n")
    
    # Run processing
    args = [
        "--input", str(zip_path),
        "--output", str(output_dir),
        "--format", "jsonl"
    ]
    with pytest.raises(SystemExit) as exc:
        main(args)
    assert exc.value.code == 0  # Should exit successfully
    
    # Check output files
    assert (output_dir / "AM.jsonl").exists()
    assert (output_dir / "EN.jsonl").exists()
    
    # Verify AM record content
    with open(output_dir / "AM.jsonl") as f:
        am_record = json.loads(f.read())
        assert am_record["record_type"] == "AM"
        assert am_record["system_id"] == 123456789
        assert am_record["call_sign"] == "W1AW"
    
    # Verify EN record content
    with open(output_dir / "EN.jsonl") as f:
        en_record = json.loads(f.read())
        assert en_record["record_type"] == "EN"
        assert en_record["system_id"] == 987654321
        assert en_record["entity_name"] == "Test Entity"

@pytest.mark.integration
def test_error_handling():
    """Test error handling"""
    # Test with invalid input
    with pytest.raises(SystemExit) as exc:
        main(["--input", "/nonexistent.zip", "--output", "/data/output"])
    assert exc.value.code != 0

@pytest.mark.integration
def test_performance(data_dir, output_dir):
    """Test processing performance"""
    # Create large test file
    zip_path = data_dir / "large.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        # Create file with 1000 records
        records = "AM|123456789|ULS123|EBF456|W1AW|A|\n" * 1000
        zf.writestr("AM.dat", records)
    
    # Time the processing
    start_time = time.time()
    
    args = [
        "--input", str(zip_path),
        "--output", str(output_dir),
        "--format", "jsonl"
    ]
    with pytest.raises(SystemExit) as exc:
        main(args)
    assert exc.value.code == 0  # Should exit successfully
    
    duration = time.time() - start_time
    
    # Should process at least 100 records per second
    assert duration < 10, "Processing too slow"
