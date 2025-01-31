import argparse
import logging
import os
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import IO

from .reader import PullParser, ParseError
from .mapper import MapperFactory, MapperError
from .writer import WriterFactory

logger = logging.getLogger(__name__)

class ProcessingStats:
    """Track processing statistics"""
    def __init__(self):
        self.record_counts = defaultdict(int)
        self.error_counts = defaultdict(int)
        self.start_time = time.time()
        self._elapsed_time = None
    
    def print_report(self):
        """Print processing statistics"""
        self._elapsed_time = time.time() - self.start_time
        print("\nProcessing Summary:")
        print(f"Total time: {self._elapsed_time:.2f} seconds")
        for schema, count in self.record_counts.items():
            print(f"{schema} records: {count}")
        if self.error_counts:
            print("\nErrors:")
            for schema, count in self.error_counts.items():
                print(f"{schema}: {count} errors")
    
    @property
    def elapsed_time(self) -> float:
        """Get elapsed processing time in seconds"""
        if self._elapsed_time is None:
            self._elapsed_time = time.time() - self.start_time
        return self._elapsed_time

def setup_logging(level=logging.INFO):
    """Configure logging format and level"""
    # Set the log level without removing existing handlers (for pytest caplog)
    logging.getLogger().setLevel(level)
    
    # Only add handler if none exist (preserves pytest caplog)
    if not logging.getLogger().handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(handler)

def validate_input(args):
    """Validate command line arguments"""
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
        
    if not zipfile.is_zipfile(input_path):
        raise ValueError(f"Input must be a ZIP archive: {input_path}")

def process_dat_file(stream: IO, filename: str, output_dir: Path, format: str, stats: ProcessingStats, verbose: bool = False):
    """Process a single .dat file"""
    # Determine schema from filename
    schema_type = Path(filename).stem.upper()
    
    # Create components
    from io import TextIOWrapper
    text_stream = TextIOWrapper(stream, encoding='utf-8')
    parser = PullParser(text_stream, verbose=verbose)
    mapper = MapperFactory.create_mapper(schema_type)
    
    if mapper is None:
        logger.info(f"Skipping {filename} - No schema for {schema_type} records")
        stats.error_counts[schema_type] = 1
        return
        
    output_path = output_dir / f"{schema_type}.{format}"
    writer = WriterFactory.create_writer(format, output_path)
    
    logger.info(f"Processing {filename} - {schema_type} records")
    
    # Process records
    with writer:
        for record in parser:
            try:
                typed_record = mapper(record)
                writer.write(typed_record.model_dump())
                stats.record_counts[schema_type] += 1
            except (MapperError, ParseError) as e:
                if isinstance(e, ParseError):
                    logger.error(
                        f"Parse error at line {e.line}, column {e.column}:\n"
                        f"Expected: {e.expected}\n"
                        f"Received: {e.received}\n"
                        f"Consumed fields: {e.consumed_fields}\n"
                        f"Partial field: {e.partial_field}\n"
                        f"Context: {e.context}"
                    )
                else:
                    logger.error(f"Error processing record: {e}")
                stats.error_counts[schema_type] += 1

class ProcessingError(Exception):
    """Error during file processing"""
    pass

def process_archive(input_path: Path, output_dir: Path, format: str = "jsonl", progress_callback = None, verbose: bool = False):
    """Process all .dat files in ZIP archive"""
    stats = ProcessingStats()
    logger.info(f"Processing {input_path}...")
    
    if progress_callback:
        progress_callback(f"Processing {input_path}...")
    
    if not input_path.exists():
        raise ProcessingError(f"Input file not found: {input_path}")
        
    if not zipfile.is_zipfile(input_path):
        raise ProcessingError(f"Input must be a ZIP archive: {input_path}")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(input_path, 'r') as zf:
        for entry in zf.namelist():
            if not entry.endswith('.dat'):
                logger.info(f"Skipping non-.dat file: {entry}")
                continue
            
            with zf.open(entry) as f:
                process_dat_file(f, entry, output_dir, format, stats, verbose=verbose)
    
    logger.info("Completed processing archive")
    if progress_callback:
        progress_callback("Completed processing archive")
    
    return stats

def parse_args(args=None):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Process FCC Wireless License Database files'
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s 0.1.0'
    )
    parser.add_argument(
        '--input',
        required=True,
        help='Input ZIP archive path'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Output directory path'
    )
    parser.add_argument(
        '--format',
        choices=['jsonl', 'parquet', 'ion', 'csv'],
        default='jsonl',
        help='Output format (default: jsonl)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    return parser.parse_args(args)

def main(args=None):
    """Main entry point"""
    try:
        # Parse arguments
        if isinstance(args, list):
            parsed_args = parse_args(args)
        else:
            parsed_args = parse_args()
        
        # Configure logging
        setup_logging(level=logging.DEBUG if parsed_args.verbose else logging.INFO)
        
        # Process archive
        stats = process_archive(
            input_path=Path(parsed_args.input),
            output_dir=Path(parsed_args.output),
            format=parsed_args.format,
            verbose=parsed_args.verbose
        )
        
        # Print summary
        stats.print_report()
        
    except ProcessingError as e:
        logger.error(f"Processing failed: {e}")
        raise SystemExit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise SystemExit(1)
        
    raise SystemExit(0)

if __name__ == '__main__':
    exit(main())
