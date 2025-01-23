import json
import tempfile
import os
from abc import ABC, abstractmethod

class WriterError(Exception):
    """Base exception for writer errors"""
    pass

from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import amazon.ion.simpleion as ion
import csv

class AbstractWriter(ABC):
    """Base class for all format-specific writers"""
    def __init__(self, path: Path):
        """Initialize with output path"""
        self.final_path = path
        self._temp_file = None
        self._writer = None

    def __enter__(self):
        """Context manager entry - create temporary file"""
        self._temp_file = tempfile.NamedTemporaryFile(
            mode=self._get_mode(),
            suffix=self._get_suffix(),
            delete=False
        )
        self._writer = self._create_writer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - handle file finalization"""
        try:
            if self._writer:
                try:
                    self._close_writer()
                except Exception as e:
                    raise WriterError(f"Failed to close writer: {e}")

            if self._temp_file:
                if exc_type is None:
                    try:
                        # Success - move temp file to final location
                        os.makedirs(os.path.dirname(self.final_path), exist_ok=True)
                        os.replace(self._temp_file.name, self.final_path)
                    except Exception as e:
                        raise WriterError(f"Failed to finalize output file: {e}")
                else:
                    # Error occurred - clean up temp file
                    try:
                        if os.path.exists(self._temp_file.name):
                            os.unlink(self._temp_file.name)
                    except Exception:
                        # Ignore cleanup errors on failure path
                        pass
        finally:
            self._temp_file = None
            self._writer = None

    @abstractmethod
    def write(self, record: Dict[str, Any]):
        """Write a single record"""
        pass

    @abstractmethod
    def _get_mode(self) -> str:
        """Get file open mode"""
        pass

    @abstractmethod
    def _get_suffix(self) -> str:
        """Get temporary file suffix"""
        pass

    @abstractmethod
    def _create_writer(self) -> Any:
        """Create format-specific writer"""
        pass

    @abstractmethod
    def _close_writer(self):
        """Close format-specific writer"""
        pass

class JSONLWriter(AbstractWriter):
    """Writer for JSON Lines format"""
    def write(self, record: Dict[str, Any]):
        """Write record as JSON line"""
        line = self._serialize_record(record)
        self._temp_file.write(line.encode('utf-8'))
        self._temp_file.write(b'\n')

    def _get_mode(self) -> str:
        return 'wb'

    def _get_suffix(self) -> str:
        return '.jsonl'

    def _create_writer(self) -> Any:
        return None  # No special writer needed

    def _close_writer(self):
        self._temp_file.flush()

    def _serialize_record(self, record: Dict[str, Any]) -> str:
        """Convert record to JSON string"""
        def json_serializer(obj):
            if isinstance(obj, date):
                return obj.isoformat()
            if hasattr(obj, 'isoformat'):  # Handle other datetime-like objects
                return obj.isoformat()
            if hasattr(obj, '__str__'):  # Try string conversion as fallback
                return str(obj)
            raise TypeError(f"Type {type(obj)} not serializable: {obj!r}")

        try:
            return json.dumps(record, default=json_serializer)
        except TypeError as e:
            raise WriterError(f"Failed to serialize record: {e}")

class ParquetWriter(AbstractWriter):
    """Writer for Apache Parquet format"""
    def __init__(self, path: Path):
        super().__init__(path)
        self._schema = None
        self._records = []

    def write(self, record: Dict[str, Any]):
        """Buffer record for batch writing"""
        if not self._schema:
            self._schema = self._infer_schema(record)
            
        self._records.append(record)
        if len(self._records) >= 1000:
            self._write_batch()

    def _get_mode(self) -> str:
        return 'wb'

    def _get_suffix(self) -> str:
        return '.parquet'

    def _create_writer(self) -> Any:
        return None  # Created when first record arrives

    def _close_writer(self):
        if self._records:
            self._write_batch()

    def _infer_schema(self, record: Dict[str, Any]) -> pa.Schema:
        """Infer PyArrow schema from record"""
        fields = []
        for name, value in record.items():
            if isinstance(value, int):
                dtype = pa.int64()
            elif isinstance(value, float):
                dtype = pa.float64()
            elif isinstance(value, date):
                dtype = pa.date32()
            else:
                dtype = pa.string()
            fields.append(pa.field(name, dtype, nullable=True))
        return pa.schema(fields)

    def _write_batch(self):
        """Write buffered records as batch"""
        if not self._records:
            return

        try:
            table = pa.Table.from_pylist(self._records, schema=self._schema)
            
            if not os.path.exists(self._temp_file.name):
                # First write - create new file
                pq.write_table(table, self._temp_file.name)
            else:
                # Append to existing file by concatenating tables
                try:
                    existing = pq.read_table(self._temp_file.name)
                    combined = pa.concat_tables([existing, table])
                    pq.write_table(combined, self._temp_file.name)
                except Exception:
                    # If reading existing file fails, just write the new table
                    pq.write_table(table, self._temp_file.name)
                
            self._records = []
        except Exception as e:
            raise WriterError(f"Failed to write Parquet batch: {e}")

class IonWriter(AbstractWriter):
    """Writer for Amazon Ion format"""
    def write(self, record: Dict[str, Any]):
        """Write record as Ion value"""
        value = self._convert_to_ion(record)
        ion.dump(value, self._temp_file, binary=False)
        self._temp_file.write(b'\n')

    def _get_mode(self) -> str:
        return 'wb'

    def _get_suffix(self) -> str:
        return '.ion'

    def _create_writer(self) -> Any:
        return None  # No special writer needed

    def _close_writer(self):
        self._temp_file.flush()

    def _convert_to_ion(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Python values to Ion-compatible values"""
        result = {}
        for key, value in record.items():
            if isinstance(value, date):
                result[key] = ion.Timestamp.from_datetime(value)
            else:
                result[key] = value
        return result

class CSVWriter(AbstractWriter):
    """Writer for CSV format"""
    def __init__(self, path: Path):
        super().__init__(path)
        self._headers = None

    def write(self, record: Dict[str, Any]):
        """Write record as CSV row"""
        if not self._headers:
            self._headers = list(record.keys())
            self._writer.writerow(self._headers)

        row = []
        for key in self._headers:
            value = record.get(key)
            if value is None:
                row.append('')
            elif isinstance(value, date):
                row.append(value.isoformat())
            else:
                row.append(str(value))
        self._writer.writerow(row)

    def _get_mode(self) -> str:
        return 'w'

    def _get_suffix(self) -> str:
        return '.csv'

    def _create_writer(self) -> Any:
        return csv.writer(self._temp_file)

    def _close_writer(self):
        self._temp_file.flush()

class WriterFactory:
    """Factory for creating format-specific writers"""
    _writers = {
        'jsonl': JSONLWriter,
        'parquet': ParquetWriter,
        'ion': IonWriter,
        'csv': CSVWriter
    }

    @classmethod
    def create_writer(cls, format: str, path: Path) -> Optional[AbstractWriter]:
        """Create a writer for the specified format"""
        if format not in cls._writers:
            raise ValueError(f"Unsupported format: {format}")
        return cls._writers[format](path)
