import json
import tempfile
import os
from abc import ABC, abstractmethod
import pyarrow as pa
import pyarrow.parquet as pq

class WriterError(Exception):
    """Base exception for writer errors"""
    pass

from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
import amazon.ion.simpleion as ion
import csv

class AbstractWriter(ABC):
    """Base class for all format-specific writers"""
    def __init__(self, path: Path):
        """Initialize with output path"""
        self.final_path = path
        self._temp_file = None
        self._writer = None
        self._error_state = False

    def __enter__(self):
        """Context manager entry - create temporary file"""
        if self._error_state:
            raise WriterError("Writer is in error state due to previous failure")
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
                    self._error_state = True
                    raise WriterError(f"Failed to close writer: {e}")

            if self._temp_file:
                if exc_type is None:
                    try:
                        # Success - move temp file to final location
                        os.makedirs(os.path.dirname(self.final_path), exist_ok=True)
                        os.replace(self._temp_file.name, self.final_path)
                    except Exception as e:
                        self._error_state = True
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
        if not self._temp_file:
            self._error_state = True
            raise WriterError("Writer must be used within a context manager")

        if self._error_state:
            raise WriterError("Writer is in error state due to previous failure")

        try:
            # Validate record structure
            if not isinstance(record, dict):
                self._error_state = True
                raise WriterError(f"Record must be a dictionary, got {type(record)}")

            # Check for unserializable values before attempting to serialize
            for key, value in record.items():
                if not isinstance(key, str):
                    self._error_state = True
                    raise WriterError(f"Dictionary key must be a string, got {type(key)}")
                if isinstance(value, object) and not isinstance(value, (str, int, float, bool, date, dict, list, type(None))):
                    self._error_state = True
                    raise WriterError(f"Record serialization failed: Unsupported type {type(value)} for field '{key}'")
            
            line = self._serialize_record(record)
            self._temp_file.write(line.encode('utf-8'))
            self._temp_file.write(b'\n')
        except Exception as e:
            self._error_state = True
            if isinstance(e, WriterError):
                raise
            raise WriterError(f"Record serialization failed: {e}")

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
            raise TypeError(f"Record serialization failed: Type {type(obj)} not serializable")

        try:
            return json.dumps(record, default=json_serializer)
        except TypeError as e:
            raise WriterError(f"Record serialization failed: {e}")

class IonWriter(AbstractWriter):
    """Writer for Amazon Ion format"""
    def write(self, record: Dict[str, Any]):
        """Write record as Ion value"""
        if not self._temp_file:
            self._error_state = True
            raise WriterError("Writer must be used within a context manager")

        if self._error_state:
            raise WriterError("Writer is in error state due to previous failure")

        try:
            value = self._convert_to_ion(record)
            ion.dump(value, self._temp_file, binary=False)
            self._temp_file.write(b'\n')
        except Exception as e:
            self._error_state = True
            raise WriterError(f"Failed to write record: {e}")

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
        if not self._temp_file:
            self._error_state = True
            raise WriterError("Writer must be used within a context manager")

        if self._error_state:
            raise WriterError("Writer is in error state due to previous failure")

        try:
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
        except Exception as e:
            self._error_state = True
            raise WriterError(f"Failed to write record: {e}")

    def _get_mode(self) -> str:
        return 'w'

    def _get_suffix(self) -> str:
        return '.csv'

    def _create_writer(self) -> Any:
        return csv.writer(self._temp_file)

    def _close_writer(self):
        self._temp_file.flush()

class ParquetWriter(AbstractWriter):
    """Writer for Apache Parquet format"""
    def __init__(self, path: Path):
        super().__init__(path)
        self._schema = None
        self._records = []
        self._writer = None

    def write(self, record: Dict[str, Any]):
        """Write record to Parquet file"""
        if not self._temp_file:
            self._error_state = True
            raise WriterError("Writer must be used within a context manager")

        if self._error_state:
            raise WriterError("Writer is in error state due to previous failure")

        if not isinstance(record, dict):
            self._error_state = True
            raise WriterError(f"Record must be a dictionary, got {type(record)}")

        try:
            # Validate record can be converted to Arrow types
            for key, value in record.items():
                if not isinstance(key, str):
                    self._error_state = True
                    raise WriterError(f"Dictionary key must be a string, got {type(key)}")
                if not isinstance(value, (int, str, float, bool, date, type(None))):
                    self._error_state = True
                    raise WriterError(f"Record serialization failed: Type {type(value)} not serializable for field '{key}'")
            
            # Initialize schema if needed
            if not self._schema:
                fields = []
                for key, value in record.items():
                    if isinstance(value, int):
                        fields.append(pa.field(key, pa.int64()))
                    elif isinstance(value, date):
                        fields.append(pa.field(key, pa.date32()))
                    else:
                        fields.append(pa.field(key, pa.string()))
                self._schema = pa.schema(fields)
                try:
                    self._writer = pq.ParquetWriter(self._temp_file.name, self._schema)
                except Exception as e:
                    self._error_state = True
                    raise WriterError(f"Failed to create Parquet writer: {e}")

            # Convert record to Arrow array
            arrays = {}
            for key, value in record.items():
                if isinstance(value, int):
                    arrays[key] = pa.array([value], type=pa.int64())
                elif isinstance(value, date):
                    arrays[key] = pa.array([value], type=pa.date32())
                else:
                    arrays[key] = pa.array([str(value) if value is not None else None])

            # Write record
            table = pa.Table.from_pydict(arrays, schema=self._schema)
            try:
                self._writer.write_table(table)
            except Exception as e:
                self._error_state = True
                raise WriterError(f"Failed to write record: {e}")

        except Exception as e:
            self._error_state = True
            if isinstance(e, WriterError):
                raise
            raise WriterError(f"Record serialization failed: {e}")

    def _get_mode(self) -> str:
        return 'wb'

    def _get_suffix(self) -> str:
        return '.parquet'

    def _create_writer(self) -> Any:
        return None  # Writer is created when first record is written

    def _close_writer(self):
        """Close the Parquet writer"""
        try:
            if self._writer:
                self._writer.close()
            elif not self._error_state:
                # No records written, create empty file with default schema
                schema = pa.schema([
                    ('record_type', pa.string()),
                    ('system_id', pa.int64())
                ])
                writer = pq.ParquetWriter(self._temp_file.name, schema)
                writer.close()
        except Exception as e:
            self._error_state = True
            raise WriterError(f"Failed to close Parquet writer: {e}")

class WriterFactory:
    """Factory for creating format-specific writers"""
    _writers = {
        'jsonl': JSONLWriter,
        'ion': IonWriter,
        'csv': CSVWriter,
        'parquet': ParquetWriter
    }

    @classmethod
    def create_writer(cls, format: str, path: Path) -> Optional[AbstractWriter]:
        """Create a writer for the specified format"""
        if format not in cls._writers:
            raise ValueError(f"Unsupported format: {format}")
        return cls._writers[format](path)
