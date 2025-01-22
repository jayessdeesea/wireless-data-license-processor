# Consumer Component

## Overview

The Consumer component is responsible for writing validated records to output files in various formats. It implements a factory pattern for writer creation and provides consistent handling of data across different output formats.

## Architecture

### Factory Pattern

```python
class ConsumerFactory:
    """Factory for creating format-specific consumers"""
    @staticmethod
    def create(output: Path) -> Consumer:
        """Create a consumer for the given output path"""
        pass

class ConsumerFactoryProvider:
    """Provider for format-specific factory instances"""
    @staticmethod
    def create(file_format: str) -> ConsumerFactory:
        """Create a factory for the specified format"""
        pass
```

### Abstract Writer

```python
class AbstractWriter:
    """Base class for all format-specific writers"""
    def __init__(self, path: Path):
        """Initialize with output path"""
        self.final_path = path
        self._temp_file = None

    def __enter__(self):
        """Context manager entry - create temporary file"""
        pass

    def write(self, record: dict):
        """Write a single record"""
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - handle file finalization"""
        if exc_type is None:
            # Success - move temp file to final location
            pass
        else:
            # Error - delete temp file
            pass
```

## Supported Formats

### JSONL (JSON Lines)
- Each record serialized as a single line of JSON
- Full support for null values
- Dates formatted as ISO 8601 strings
- Example output:
  ```jsonl
  {"field1": "value1", "field2": null}
  {"field1": "value2", "field2": "2024-02-15"}
  ```

### Parquet
- Columnar storage format
- Native support for dates and complex types
- Efficient compression
- Schema enforcement
- Example schema:
  ```
  message Record {
    required binary field1 (STRING);
    optional int64 field2;
    optional int32 date_field (DATE);
  }
  ```

### Ion (Text Format)
- Amazon Ion text format (not binary)
- Full support for null values
- Rich type system
- Example output:
  ```ion
  {field1:"value1", field2:null}
  {field1:"value2", field2:2024-02-15T}
  ```

### CSV
- Simple text format
- Empty strings for null values
- Dates as ISO 8601 strings
- Example output:
  ```csv
  field1,field2,date_field
  value1,,
  value2,data,2024-02-15
  ```

## Implementation Details

### File Handling
- Uses temporary files during writing
- Atomic file operations for reliability
- Cleanup on errors
- One output file per record type

### Data Type Handling

#### Missing Fields
- JSONL: `null`
- Ion: `null`
- Parquet: Format-specific null representation
- CSV: Empty string (`""`)

#### Dates
- Input: mm/dd/yyyy (from schema)
- Output:
  - JSONL: ISO 8601 string (YYYY-MM-DD)
  - Ion: Ion timestamp (YYYY-MM-DD)
  - Parquet: Native date type
  - CSV: ISO 8601 string (YYYY-MM-DD)

#### Numbers
- Input: String representation
- Output:
  - JSONL: Native JSON number
  - Ion: Native Ion number
  - Parquet: Native numeric type
  - CSV: String representation

### Error Handling

#### File Operations
- Temporary file cleanup on any error
- Atomic file operations (rename only on success)
- Proper file handle cleanup

#### Data Errors
- Invalid field values
- Schema validation failures
- Format-specific encoding issues

#### System Errors
- Disk space exhaustion
- Permission issues
- I/O errors
- Resource limits

#### Error Messages
- Context-rich error descriptions
- File operation state at failure
- Validation failure details
- System error information

## Usage Example

```python
# Create a writer for JSONL format
with JSONLWriter("output/AM.jsonl") as writer:
    # Write records
    writer.write({"field1": "value1", "field2": None})
    writer.write({"field1": "value2", "field2": "2024-02-15"})
```

## Extension

To add support for a new format:

1. Create a new writer class extending AbstractWriter
2. Implement format-specific serialization
3. Add format to ConsumerFactoryProvider
4. Update format validation in CLI

Example:
```python
class NewFormatWriter(AbstractWriter):
    def write(self, record: dict):
        # Format-specific implementation
        pass
```
