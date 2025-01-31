"""Tests for CLI and end-to-end processing"""
import pytest
import zipfile
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import logging
from wdlp.main import main, process_archive, ProcessingError, setup_logging

@pytest.fixture
def test_zip():
    """Create test ZIP archive with .dat files"""
    with TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "test.zip"
        
        # Create test .dat files
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            # AM record
            zf.writestr("AM.dat", "AM|123456789|ULS123|EBF456|W1AW|A|\n".encode('utf-8'))
            
            # EN record
            zf.writestr("EN.dat", "EN|987654321|ULS789|EBF012|K1ABC|I|LIC123|Test Entity|\n".encode('utf-8'))
            
            # Non-.dat file
            zf.writestr("README.txt", "Test file".encode('utf-8'))
        
        yield zip_path

def test_process_archive(test_zip):
    """Test end-to-end archive processing"""
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "output"
        
        # Process archive
        stats = process_archive(
            input_path=test_zip,
            output_dir=output_dir,
            format="jsonl"
        )
        
        # Check output files exist
        assert (output_dir / "AM.jsonl").exists()
        assert (output_dir / "EN.jsonl").exists()
        
        # Check record counts
        assert stats.record_counts["AM"] == 1
        assert stats.record_counts["EN"] == 1
        assert len(stats.error_counts) == 0
        
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

def test_invalid_zip():
    """Test handling of invalid ZIP files"""
    with TemporaryDirectory() as tmpdir:
        # Create invalid ZIP file
        invalid_zip = Path(tmpdir) / "invalid.zip"
        invalid_zip.write_bytes(b"Not a ZIP file")
        
        with pytest.raises(ProcessingError) as exc:
            process_archive(
                input_path=invalid_zip,
                output_dir=Path(tmpdir) / "output"
            )
        assert "ZIP" in str(exc.value)

def test_missing_input():
    """Test handling of missing input file"""
    with TemporaryDirectory() as tmpdir:
        with pytest.raises(ProcessingError) as exc:
            process_archive(
                input_path=Path(tmpdir) / "nonexistent.zip",
                output_dir=Path(tmpdir) / "output"
            )
        assert "not found" in str(exc.value).lower()

def test_invalid_format():
    """Test handling of invalid output format"""
    with TemporaryDirectory() as tmpdir:
        # Create test zip file
        zip_path = Path(tmpdir) / "test.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("AM.dat", "AM|123|W1AW|A|\n".encode('utf-8'))
        
        with pytest.raises(ValueError) as exc:
            process_archive(
                input_path=zip_path,
                output_dir=Path(tmpdir) / "output",
                format="invalid"
            )
        assert "format" in str(exc.value).lower()

def test_cli_arguments(capsys):
    """Test CLI argument parsing"""
    with TemporaryDirectory() as tmpdir:
        input_path = Path(tmpdir) / "test.zip"
        output_dir = Path(tmpdir) / "output"
        
        # Test required arguments missing
        with pytest.raises(SystemExit):
            main([])
        captured = capsys.readouterr()
        assert "required" in captured.err
        assert "--input" in captured.err
        assert "--output" in captured.err
        
        # Create test zip file
        zip_path = Path(tmpdir) / "test.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("AM.dat", "AM|123|W1AW|A|\n".encode('utf-8'))
        
        # Test valid arguments
        with pytest.raises(SystemExit) as exc:
            main([
                "--input", str(zip_path),
                "--output", str(output_dir),
                "--format", "jsonl"
            ])
        assert exc.value.code == 0  # Should succeed
        captured = capsys.readouterr()
        assert "Processing Summary" in captured.out

def test_output_directory_creation():
    """Test output directory handling"""
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "nested" / "output"
        
        # Create test zip file
        zip_path = Path(tmpdir) / "test.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("AM.dat", "AM|123|W1AW|A|\n".encode('utf-8'))
        
        # Process archive
        stats = process_archive(
            input_path=zip_path,
            output_dir=output_dir
        )
        
        # Directory should be created and contain output
        assert output_dir.exists()
        assert (output_dir / "AM.jsonl").exists()

def test_progress_callback():
    """Test progress reporting"""
    progress_messages = []
    
    def callback(msg: str):
        progress_messages.append(msg)
    
    with TemporaryDirectory() as tmpdir:
        # Will fail but should still report progress
        with pytest.raises(ProcessingError):
            process_archive(
                input_path=Path("nonexistent.zip"),
                output_dir=Path(tmpdir) / "output",
                progress_callback=callback
            )
    
    # Should have at least one progress message
    assert len(progress_messages) > 0
    assert any("Processing" in msg for msg in progress_messages)

def test_error_recovery(test_zip):
    """Test partial processing with errors"""
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "output"
        
        # Create ZIP with one invalid file
        with zipfile.ZipFile(test_zip, "a") as zf:
            zf.writestr("INVALID.dat", "Invalid|Data|\n")
        
        # Process archive
        stats = process_archive(
            input_path=test_zip,
            output_dir=output_dir
        )
        
        # Should have processed valid files
        assert stats.record_counts["AM"] > 0
        assert stats.record_counts["EN"] > 0
        
        # Should have recorded error
        assert "INVALID" in stats.error_counts
        assert stats.error_counts["INVALID"] == 1

def test_performance_monitoring(test_zip):
    """Test performance statistics collection"""
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "output"
        
        # Process archive
        stats = process_archive(
            input_path=test_zip,
            output_dir=output_dir
        )
        
        # Check timing information
        assert stats.elapsed_time > 0
        assert isinstance(stats.elapsed_time, float)

def test_binary_data_handling():
    """Test handling of binary data in fields"""
    with TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "binary_test.zip"
        output_dir = Path(tmpdir) / "output"
        
        # Create test data with binary content
        test_data = []
        # Create test data with fields at their max length
        test_data.append(f"AM|123456789|{'x' * 14}|{'x' * 30}|W1AW|A|\n")  # Use max length for uls_file_number and ebf_number
        
        # Create ZIP with test data
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("AM.dat", ''.join(test_data).encode('utf-8'))
        
        # Process archive
        stats = process_archive(
            input_path=zip_path,
            output_dir=output_dir,
            format="jsonl"
        )
        
        # Verify processing succeeded
        assert stats.record_counts["AM"] == 1
        assert len(stats.error_counts) == 0
        
        # Verify record content
        with open(output_dir / "AM.jsonl") as f:
            am_record = json.loads(f.read())
            assert am_record["record_type"] == "AM"
            assert len(am_record["uls_file_number"]) == 14  # Max length field was preserved
            assert len(am_record["ebf_number"]) == 30  # Max length field was preserved
            assert am_record["call_sign"] == "W1AW"

def test_verbose_mode(test_zip, caplog):
    """Test verbose logging output"""
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "output"
        
        # Create test zip file
        zip_path = Path(tmpdir) / "test.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("AM.dat", "AM|123|W1AW|A|\n".encode('utf-8'))
        
        # Configure logging
        setup_logging(level=logging.DEBUG)
        
        # Process with verbose logging
        with pytest.raises(SystemExit) as exc:
            main([
                "--input", str(zip_path),
                "--output", str(output_dir),
                "--verbose"
            ])
        assert exc.value.code == 0  # Should exit successfully
        
        # Check log output and file creation
        log_messages = [record.message for record in caplog.records]
        assert any("Processing" in msg for msg in log_messages), "No 'Processing' message found"
        assert any("Completed" in msg for msg in log_messages), "No 'Completed' message found"
        assert (output_dir / "AM.jsonl").exists()
