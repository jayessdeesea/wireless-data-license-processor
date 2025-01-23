"""
Wireless Data License Processor (wdlp)

A Python package for processing FCC Wireless License Database .dat files.
"""

__version__ = "0.1.0"

from wdlp.schema import AMRecord, ENRecord
from wdlp.reader import PullParser, ParseError, Record
from wdlp.mapper import MapperFactory, MapperError
from wdlp.writer import (
    WriterFactory,
    WriterError,
    JSONLWriter,
    ParquetWriter,
    IonWriter,
    CSVWriter,
)
from wdlp.main import main, process_archive, ProcessingError

__all__ = [
    # Version
    "__version__",
    
    # Schema
    "AMRecord",
    "ENRecord",
    
    # Reader
    "PullParser",
    "ParseError",
    "Record",
    
    # Mapper
    "MapperFactory",
    "MapperError",
    
    # Writer
    "WriterFactory",
    "WriterError",
    "JSONLWriter",
    "ParquetWriter",
    "IonWriter",
    "CSVWriter",
    
    # Main
    "main",
    "process_archive",
    "ProcessingError",
]
