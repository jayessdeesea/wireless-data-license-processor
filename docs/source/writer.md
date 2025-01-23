# Writer Component

## Quick Start

```python
from wdlp.writer import WriterFactory
from pathlib import Path
from typing import Iterator, Dict, Any

def write_records(records: Iterator[Dict[str, Any]], output_path: Path, format: str) -> None:
    """Write records to output file with proper error handling"""
    writer = WriterFactory.create_writer(format, output_path)
    
    with writer as w:
        try:
            for record in records:
                w.write(record)
        except WriterError as e:
            logger.error(f"Write error: {e}")
            raise

# Example usage
records = map_records(parser)  # From mapper component
write_records(records, Path("output/AM.jsonl"), "jsonl")
```

## Overview

The Writer component handles writing validated records to various output formats using Python's context managers and type system. It provides atomic file operations and consistent type handling across formats.

## Design Philosophy

The implementation follows these Python idiomatic principles:

1. **Resource Management**
   ```python
   from pathlib import Path
   from typing import Protocol, ContextManager
   from tempfile import NamedTemporaryFile
   import shutil
   
   class Writer(Protocol, ContextManager["Writer"]):
       """Protocol for format-specific writers"""
       def write(self, record: Dict[str, Any]) -> None:
           """Write single record"""
           ...
   
   class BaseWriter:
       """Base implementation with resource management"""
       def __init__(self, path: Path):
           self.final_path = path
           self._temp_file: Optional[NamedTemporaryFile] = None
           
       def __enter__(self) -> "BaseWriter":
           """Setup temporary file"""
           self._temp_file = NamedTemporaryFile(
               mode="w",
               delete=False,
               suffix=self.final_path.suffix,
               dir=self.final_path.parent
           )
           return self
           
       def __exit__(self, exc_type, exc_val, exc_tb) -> None:
           """Handle cleanup and atomic move"""
           if self._temp_file:
               if exc_type is None:
                   # Success - move temp file to final location
                   shutil.move(self._temp_file.name, self.final_path)
               else:
                   # Error - cleanup temp file
                   Path(self._temp_file.name).unlink(missing_ok=True)
   ```

2. **Type Handling**
   ```python
   from datetime import date, datetime
   from decimal import Decimal
   from typing import Any, TypeVar, Type
   
   T = TypeVar("T")
   
   class TypeConverter:
       """Consistent type conversion across formats"""
       @staticmethod
       def to_string(value: Any) -> Optional[str]:
           """Convert value to string representation"""
           if value is None:
               return None
           if isinstance(value, (date, datetime)):
               return value.isoformat()
           if isinstance(value, Decimal):
               return str(value)
           return str(value)
       
       @staticmethod
       def to_type(value: Any, target_type: Type[T]) -> Optional[T]:
           """Convert value to target type"""
           if value is None:
               return None
           return target_type(value)
   ```

3. **Error Handling**
   ```python
   from dataclasses import dataclass
   from typing import Optional
   
   @dataclass
   class WriterError(Exception):
       """Rich error information"""
       path: Path
       operation: str
       details: str
       original_error: Optional[Exception] = None
       
       def __str__(self) -> str:
           """Detailed error message"""
           msg = f"Error {self.operation} {self.path}: {self.details}"
           if self.original_error:
               msg += f"\nCaused by: {self.original_error}"
           return msg
   ```

## Format-Specific Writers

### JSONL Writer
```python
import json
from typing import Any, Dict

class JSONLWriter(BaseWriter):
    """Writer for JSON Lines format"""
    def write(self, record: Dict[str, Any]) -> None:
        """Write single JSON line"""
        try:
            line = json.dumps(record, default=str)
            self._temp_file.write(f"{line}\n")
        except Exception as e:
            raise WriterError(
                path=self.final_path,
                operation="writing",
                details="JSON serialization failed",
                original_error=e
            )
```

### Parquet Writer
```python
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List

class ParquetWriter(BaseWriter):
    """Writer for Apache Parquet format"""
    def __init__(self, path: Path):
        super().__init__(path)
        self._schema: Optional[pa.Schema] = None
        self._records: List[Dict[str, Any]] = []
    
    def write(self, record: Dict[str, Any]) -> None:
        """Buffer record for batch writing"""
        if not self._schema:
            self._schema = self._infer_schema([record])
        self._records.append(record)
        
        # Write batch if buffer is full
        if len(self._records) >= 1000:
            self._write_batch()
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Write final batch and cleanup"""
        if not exc_type and self._records:
            self._write_batch()
        super().__exit__(exc_type, exc_val, exc_tb)
```

### Ion Writer
```python
from amazon.ion import simpleion
from datetime import date

class IonWriter(BaseWriter):
    """Writer for Amazon Ion format"""
    def write(self, record: Dict[str, Any]) -> None:
        """Write single Ion value"""
        # Convert dates to Ion timestamps
        processed = {
            k: simpleion.Timestamp.from_datetime(v)
            if isinstance(v, date) else v
            for k, v in record.items()
        }
        self._temp_file.write(
            simpleion.dumps(processed, binary=False) + "\n"
        )
```

### CSV Writer
```python
import csv
from contextlib import contextmanager

class CSVWriter(BaseWriter):
    """Writer for CSV format"""
    def __init__(self, path: Path):
        super().__init__(path)
        self._writer: Optional[csv.DictWriter] = None
    
    def __enter__(self) -> "CSVWriter":
        """Setup CSV writer"""
        super().__enter__()
        self._writer = csv.DictWriter(
            self._temp_file,
            fieldnames=self._get_fieldnames(),
            extrasaction="ignore"
        )
        self._writer.writeheader()
        return self
    
    def write(self, record: Dict[str, Any]) -> None:
        """Write CSV row with proper type conversion"""
        converted = {
            k: TypeConverter.to_string(v)
            for k, v in record.items()
        }
        self._writer.writerow(converted)
```

## Performance Optimization

1. **Buffered Writing**
   ```python
   class BufferedWriter(BaseWriter):
       """Writer with record buffering"""
       def __init__(self, path: Path, buffer_size: int = 1000):
           super().__init__(path)
           self._buffer: List[Dict[str, Any]] = []
           self._buffer_size = buffer_size
           
       def write(self, record: Dict[str, Any]) -> None:
           """Buffer record and write when full"""
           self._buffer.append(record)
           if len(self._buffer) >= self._buffer_size:
               self._flush_buffer()
   ```

2. **Memory Management**
   ```python
   class StreamingWriter(BaseWriter):
       """Memory-efficient writer"""
       def write(self, record: Dict[str, Any]) -> None:
           """Write records one at a time"""
           self._write_record(record)
           self._temp_file.flush()  # Ensure written to disk
   ```

## Testing Strategy

```python
import pytest
from pathlib import Path
import json

class TestJSONLWriter:
    """Test suite for JSONL writer"""
    
    @pytest.fixture
    def output_path(self, tmp_path: Path) -> Path:
        """Create temporary output path"""
        return tmp_path / "test.jsonl"
    
    def test_atomic_write(self, output_path: Path):
        """Test atomic write operation"""
        records = [{"id": 1}, {"id": 2}]
        
        with JSONLWriter(output_path) as writer:
            for record in records:
                writer.write(record)
                
        # Verify output
        with output_path.open() as f:
            written = [json.loads(line) for line in f]
        assert written == records
    
    def test_cleanup_on_error(self, output_path: Path):
        """Test temp file cleanup on error"""
        with pytest.raises(ValueError):
            with JSONLWriter(output_path) as writer:
                writer.write({"id": 1})
                raise ValueError("Test error")
        
        # Verify no output file
        assert not output_path.exists()
```

## Common Pitfalls

1. **Resource Leaks**
   ```python
   # Wrong: Manual file handling
   writer = open("output.jsonl", "w")
   writer.write(record)  # May leak file handle
   
   # Correct: Context manager
   with open("output.jsonl", "w") as writer:
       writer.write(record)
   ```

2. **Atomic Operations**
   ```python
   # Wrong: Direct file writing
   with open("output.jsonl", "w") as f:
       f.write(record)  # Partial writes possible
   
   # Correct: Atomic operation
   with NamedTemporaryFile(mode="w", delete=False) as temp:
       temp.write(record)
   shutil.move(temp.name, "output.jsonl")
   ```

## Extension Guide

To add support for a new format:

1. Create writer class:
```python
class XMLWriter(BaseWriter):
    """Writer for XML format"""
    def __init__(self, path: Path):
        super().__init__(path)
        self._root = ET.Element("records")
    
    def write(self, record: Dict[str, Any]) -> None:
        """Write record as XML element"""
        record_elem = ET.SubElement(self._root, "record")
        for key, value in record.items():
            field = ET.SubElement(record_elem, key)
            field.text = TypeConverter.to_string(value)
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Write XML tree to file"""
        if not exc_type:
            tree = ET.ElementTree(self._root)
            tree.write(self._temp_file.name)
        super().__exit__(exc_type, exc_val, exc_tb)
```

2. Register with factory:
```python
WriterFactory.register_format("xml", XMLWriter)
```

3. Add tests:
```python
def test_xml_writer(tmp_path: Path):
    """Test XML format writing"""
    path = tmp_path / "test.xml"
    records = [{"id": 1, "name": "Test"}]
    
    with XMLWriter(path) as writer:
        for record in records:
            writer.write(record)
            
    tree = ET.parse(path)
    root = tree.getroot()
    assert root.tag == "records"
    assert len(root) == len(records)
