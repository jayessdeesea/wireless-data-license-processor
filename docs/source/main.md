# Main Component

## Quick Start

```python
from wdlp.main import process_archive
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Process archive with progress reporting
try:
    stats = process_archive(
        input_path=Path("l_amat.zip"),
        output_dir=Path("output"),
        format="jsonl",
        progress_callback=lambda msg: print(f"Progress: {msg}")
    )
    print(f"Processed {stats.total_records} records in {stats.elapsed_time:.2f}s")
except ProcessingError as e:
    logging.error(f"Processing failed: {e}")
    raise
```

## Overview

The Main component orchestrates the data processing pipeline using Python's type system and modern CLI patterns. It coordinates the Reader, Mapper, and Writer components while providing comprehensive error handling, logging, and performance monitoring.

## Design Philosophy

The implementation follows these Python idiomatic principles:

1. **Type Safety**
   ```python
   from dataclasses import dataclass
   from pathlib import Path
   from typing import Callable, Optional
   from datetime import datetime, timedelta
   
   @dataclass(frozen=True)
   class ProcessingConfig:
       """Immutable processing configuration"""
       input_path: Path
       output_dir: Path
       format: str = "jsonl"
       batch_size: int = 1000
       progress_callback: Optional[Callable[[str], None]] = None
   
   @dataclass
   class ProcessingStats:
       """Runtime statistics"""
       start_time: datetime
       record_counts: dict[str, int]
       error_counts: dict[str, int]
       
       @property
       def elapsed_time(self) -> float:
           """Calculate processing duration"""
           return (datetime.now() - self.start_time).total_seconds()
   ```

2. **Error Handling**
   ```python
   from typing import Union
   from pathlib import Path
   
   class ProcessingError(Exception):
       """Base class for processing errors"""
       def __init__(
           self,
           message: str,
           path: Union[Path, str],
           details: Optional[str] = None
       ):
           self.path = Path(path)
           self.details = details
           super().__init__(
               f"{message} ({self.path})"
               + (f": {details}" if details else "")
           )
   
   class ArchiveError(ProcessingError):
       """ZIP archive processing errors"""
       pass
   
   class SchemaError(ProcessingError):
       """Schema validation errors"""
       pass
   ```

3. **Resource Management**
   ```python
   from contextlib import contextmanager
   from typing import Iterator, IO
   import zipfile
   
   @contextmanager
   def open_archive(path: Path) -> Iterator[zipfile.ZipFile]:
       """Safely open ZIP archive"""
       try:
           with zipfile.ZipFile(path, "r") as zf:
               yield zf
       except zipfile.BadZipFile as e:
           raise ArchiveError(
               "Invalid ZIP archive",
               path,
               str(e)
           )
   
   @contextmanager
   def process_dat_file(
       archive: zipfile.ZipFile,
       entry: str
   ) -> Iterator[IO[bytes]]:
       """Process single .dat file"""
       try:
           with archive.open(entry) as f:
               yield f
       except Exception as e:
           raise ProcessingError(
               "Error processing .dat file",
               entry,
               str(e)
           )
   ```

## Command Line Interface

```python
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from typing import Sequence
import sys

def create_parser() -> ArgumentParser:
    """Create CLI argument parser"""
    parser = ArgumentParser(
        description="Process FCC Wireless License Database files",
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "-i", "--input",
        type=Path,
        required=True,
        help="Input ZIP archive path"
    )
    
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("output"),
        help="Output directory path"
    )
    
    parser.add_argument(
        "-f", "--format",
        choices=["jsonl", "parquet", "ion", "csv"],
        default="jsonl",
        help="Output format"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (repeat for more)"
    )
    
    return parser

def main(argv: Sequence[str] = sys.argv[1:]) -> int:
    """Main entry point"""
    parser = create_parser()
    args = parser.parse_args(argv)
    
    # Configure logging based on verbosity
    log_level = max(logging.INFO - (10 * args.verbose), logging.DEBUG)
    logging.basicConfig(level=log_level)
    
    try:
        stats = process_archive(
            input_path=args.input,
            output_dir=args.output,
            format=args.format
        )
        print_stats(stats)
        return 0
    except Exception as e:
        logging.error(str(e))
        return 1
```

## Processing Pipeline

```python
from typing import Iterator, Dict, Any
import time

def process_archive(
    input_path: Path,
    output_dir: Path,
    format: str = "jsonl",
    **kwargs
) -> ProcessingStats:
    """Process ZIP archive end-to-end"""
    stats = ProcessingStats(
        start_time=datetime.now(),
        record_counts={},
        error_counts={}
    )
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open_archive(input_path) as archive:
        # Process each .dat file
        for entry in archive.namelist():
            if not entry.endswith(".dat"):
                continue
                
            record_type = Path(entry).stem.upper()
            output_path = output_dir / f"{record_type}.{format}"
            
            try:
                # Create pipeline components
                reader = create_reader(archive, entry)
                mapper = create_mapper(record_type)
                writer = create_writer(format, output_path)
                
                # Process records through pipeline
                count = process_records(reader, mapper, writer)
                stats.record_counts[record_type] = count
            except Exception as e:
                stats.error_counts[record_type] = (
                    stats.error_counts.get(record_type, 0) + 1
                )
                logging.error(f"Error processing {entry}: {e}")
                
    return stats

def process_records(reader, mapper, writer) -> int:
    """Process records through the pipeline"""
    count = 0
    with writer:
        for record in reader:
            try:
                # Transform raw record to validated model
                model = mapper(record)
                # Write model to output
                writer.write(model.model_dump())
                count += 1
            except Exception as e:
                logging.warning(f"Error on record {count + 1}: {e}")
    return count
```

## Performance Monitoring

```python
from dataclasses import dataclass, field
from typing import Dict
import time

@dataclass
class PerformanceMetrics:
    """Detailed performance tracking"""
    start_time: float = field(default_factory=time.time)
    record_counts: Dict[str, int] = field(default_factory=dict)
    processing_times: Dict[str, float] = field(default_factory=dict)
    peak_memory: float = 0.0
    
    def update_memory(self) -> None:
        """Update peak memory usage"""
        import psutil
        process = psutil.Process()
        memory = process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = max(self.peak_memory, memory)
    
    def record_time(self, operation: str, duration: float) -> None:
        """Record operation timing"""
        if operation not in self.processing_times:
            self.processing_times[operation] = 0.0
        self.processing_times[operation] += duration

class PerformanceMonitor:
    """Context manager for performance monitoring"""
    def __init__(self, metrics: PerformanceMetrics, operation: str):
        self.metrics = metrics
        self.operation = operation
        self.start_time = None
        
    def __enter__(self) -> None:
        self.start_time = time.time()
        
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = time.time() - self.start_time
        self.metrics.record_time(self.operation, duration)
        self.metrics.update_memory()
```

## Testing Strategy

```python
import pytest
from pathlib import Path
import zipfile
import io

class TestProcessing:
    """Test suite for processing pipeline"""
    
    @pytest.fixture
    def test_archive(self, tmp_path: Path) -> Path:
        """Create test ZIP archive"""
        archive_path = tmp_path / "test.zip"
        
        with zipfile.ZipFile(archive_path, "w") as zf:
            # Add test .dat file
            data = "AM|123|test|\n"
            zf.writestr("AM.dat", data)
        
        return archive_path
    
    def test_process_archive(
        self,
        test_archive: Path,
        tmp_path: Path
    ):
        """Test end-to-end processing"""
        output_dir = tmp_path / "output"
        
        stats = process_archive(
            input_path=test_archive,
            output_dir=output_dir,
            format="jsonl"
        )
        
        assert stats.record_counts["AM"] > 0
        assert (output_dir / "AM.jsonl").exists()
    
    def test_error_handling(self):
        """Test error handling"""
        with pytest.raises(ArchiveError):
            process_archive(
                input_path=Path("nonexistent.zip"),
                output_dir=Path("output")
            )
```

## Common Pitfalls

1. **Path Handling**
   ```python
   # Wrong: String paths
   output_file = os.path.join(output_dir, "AM.jsonl")
   
   # Correct: Path objects
   output_file = output_dir / "AM.jsonl"
   ```

2. **Error Recovery**
   ```python
   # Wrong: Continue after error
   try:
       process_dat_file(...)
   except Exception:
       pass  # Silent failure
   
   # Correct: Log and track errors
   try:
       process_dat_file(...)
   except Exception as e:
       stats.error_counts[record_type] += 1
       logging.error(f"Processing failed: {e}")
   ```

## Extension Guide

To add new functionality:

1. Add CLI option:
```python
parser.add_argument(
    "--validate-only",
    action="store_true",
    help="Validate without writing output"
)
```

2. Update processing:
```python
def process_archive(
    input_path: Path,
    output_dir: Path,
    validate_only: bool = False,
    **kwargs
) -> ProcessingStats:
    """Process with validation option"""
    if validate_only:
        return validate_archive(input_path, **kwargs)
    return process_archive_full(
        input_path,
        output_dir,
        **kwargs
    )
```

3. Add tests:
```python
def test_validate_only():
    """Test validation-only mode"""
    stats = process_archive(
        input_path=test_archive,
        output_dir=Path("output"),
        validate_only=True
    )
    assert stats.validation_errors == 0
