"""Tests for CLI and end-to-end processing"""
import pytest
import zipfile
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from wdlp.main import main, process_archive, ProcessingError

@pytest.fixture
def test_zip():
    """Create test ZIP archive with .dat files"""
    with TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "test.zip"
        
        # Create test .dat files
        with zipfile.ZipFile(zip_path, "w") as zf:
            # AM record
            zf.writestr("AM.dat", "AM|123456789|ULS123|EBF456|W1AW|A|\n")
            
            # EN record
            zf.writestr("EN.dat", "EN|987654321|ULS789|EBF012|K1ABC|I|LIC123|Test Entity|\n")
            
            # Non-.dat file
            zf.writestr("README.txt", "Test file")
        
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
    with pytest.raises(ValueError) as exc:
        process_archive(
            input_path=Path("test.zip"),
            output_dir=Path("output"),
            format="invalid"
        )
    assert "format" in str(exc.value).lower()

def test_cli_arguments():
    """Test CLI argument parsing"""
    with TemporaryDirectory() as tmpdir:
        input_path = Path(tmpdir) / "test.zip"
        output_dir = Path(tmpdir) / "output"
        
        # Test required arguments missing
        with pytest.raises(SystemExit):
            main([])
        
        # Test valid arguments
        with pytest.raises(ProcessingError):  # Will fail because file doesn't exist
            main([
                "--input", str(input_path),
                "--output", str(output_dir),
                "--format", "jsonl"
            ])

def test_output_directory_creation():
    """Test output directory handling"""
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "nested" / "output"
        
        # Process archive (will fail but should create directory)
        with pytest.raises(ProcessingError):
            process_archive(
                input_path=Path("nonexistent.zip"),
                output_dir=output_dir
            )
        
        # Directory should be created
        assert output_dir.exists()

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

def test_verbose_mode(test_zip, caplog):
    """Test verbose logging output"""
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "output"
        
        # Process with verbose logging
        main([
            "--input", str(test_zip),
            "--output", str(output_dir),
            "--verbose"
        ])
        
        # Check log output
        assert any("Processing" in msg for msg in caplog.messages)
        assert any("Completed" in msg for msg in caplog.messages)
