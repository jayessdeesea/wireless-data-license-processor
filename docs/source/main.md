# Main Component

## Overview

The Main component serves as the central orchestrator for the data processing pipeline. It coordinates the interaction between the Reader, Mapper, and Writer components while providing robust error handling, logging, and performance monitoring capabilities.

## Design Philosophy

The implementation follows key Python idiomatic principles:

1. **Type Safety**
   - Uses Python's type system to ensure data integrity
   - Implements immutable configuration objects to prevent runtime modifications
   - Tracks processing statistics through strongly-typed data structures
   - Leverages dataclasses for clean, type-safe data containers

2. **Error Handling**
   - Implements a hierarchical error system for precise error reporting
   - Provides detailed error context including file paths and specific error details
   - Ensures errors are properly logged and tracked in processing statistics
   - Maintains error state isolation between different record types

3. **Resource Management**
   - Safely handles file system operations using context managers
   - Ensures proper cleanup of system resources
   - Implements automatic closing of file handles
   - Provides detailed error reporting for resource access issues

## Command Line Interface

The CLI provides a user-friendly interface with the following key features:

- Required input archive path specification
- Configurable output directory (defaults to "output")
- Multiple output format options (jsonl, parquet, ion, csv)
- Adjustable verbosity levels for detailed logging
- Progress reporting during processing
- Comprehensive error reporting

## Processing Pipeline

The processing pipeline follows these steps:

1. **Initialization**
   - Validates input archive existence
   - Creates output directory if needed
   - Initializes processing statistics

2. **Archive Processing**
   - Iterates through .dat files in the archive
   - Creates type-specific output files
   - Maintains separate error counts per record type

3. **Record Processing**
   - Reads records from input files
   - Maps raw data to validated models
   - Writes transformed records to output
   - Tracks processing statistics

## Performance Monitoring

The system includes comprehensive performance tracking:

- Record counts per file type
- Processing duration per operation
- Peak memory usage monitoring
- Detailed timing breakdowns
- Resource utilization tracking

## Testing Strategy

The testing approach focuses on:

- End-to-end pipeline validation
- Error handling verification
- Resource cleanup confirmation
- Performance benchmarking
- Edge case coverage

## Common Pitfalls

Key areas to watch out for:

1. **Path Handling**
   - Always use Path objects instead of string paths
   - Maintain consistent path handling across components
   - Handle path creation and validation properly

2. **Error Recovery**
   - Never silently fail operations
   - Always log errors with context
   - Track error counts in statistics
   - Implement appropriate error recovery strategies

## Extension Guide

To extend functionality:

1. **Add CLI Options**
   - Define new command line arguments
   - Implement parameter validation
   - Update help documentation

2. **Update Processing**
   - Add new processing modes
   - Implement new validation rules
   - Extend statistics tracking

3. **Testing**
   - Add tests for new functionality
   - Update existing test cases
   - Verify error handling

## Code Examples

Below are practical examples demonstrating key functionality:

### Quick Start
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

### Configuration Example
```python
@dataclass(frozen=True)
class ProcessingConfig:
    """Immutable processing configuration"""
    input_path: Path
    output_dir: Path
    format: str = "jsonl"
    batch_size: int = 1000
    progress_callback: Optional[Callable[[str], None]] = None
```

### Error Handling Example
```python
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
