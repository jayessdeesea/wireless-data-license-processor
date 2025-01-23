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
        self.schema_counts = defaultdict(int)
        self.start_time = time.time()
    
    def print_report(self):
        """Print processing statistics"""
        elapsed = time.time() - self.start_time
        print("\nProcessing Summary:")
        print(f"Total time: {elapsed:.2f} seconds")
        for schema, count in self.schema_counts.items():
            print(f"{schema} records: {count}")

def setup_logging():
    """Configure logging format and level"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def validate_input(args):
    """Validate command line arguments"""
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
        
    if not zipfile.is_zipfile(input_path):
        raise ValueError(f"Input must be a ZIP archive: {input_path}")

def process_dat_file(stream: IO, filename: str, output_dir: Path, format: str, stats: ProcessingStats):
    """Process a single .dat file"""
    # Determine schema from filename
    schema_type = Path(filename).stem.upper()
    
    # Create components
    parser = PullParser(stream)
    mapper = MapperFactory.create_mapper(schema_type)
    
    if mapper is None:
        logger.info(f"Skipping {filename} - No schema for {schema_type} records")
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
                stats.schema_counts[schema_type] += 1
            except (MapperError, ParseError) as e:
                logger.error(f"Error processing record: {e}")

def process_zip(zip_path: Path, output_dir: Path, format: str, stats: ProcessingStats):
    """Process all .dat files in ZIP archive"""
    logger.info(f"Processing {zip_path}...")
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for entry in zf.namelist():
            if not entry.endswith('.dat'):
                logger.info(f"Skipping non-.dat file: {entry}")
                continue
            
            with zf.open(entry) as f:
                process_dat_file(f, entry, output_dir, format, stats)

def parse_args():
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
        '-i', '--input',
        default='l_amat.zip',
        help='Input ZIP archive path (default: l_amat.zip)'
    )
    parser.add_argument(
        '-o', '--output',
        default='output',
        help='Output directory path (default: output/)'
    )
    parser.add_argument(
        '-f', '--format',
        choices=['jsonl', 'parquet', 'ion', 'csv'],
        default='jsonl',
        help='Output format (default: jsonl)'
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    # Parse arguments
    args = parse_args()
    
    # Configure logging
    setup_logging()
    
    try:
        # Validate input
        validate_input(args)
        
        # Create output directory
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process files
        stats = ProcessingStats()
        process_zip(Path(args.input), output_dir, args.format, stats)
        
        # Print summary
        stats.print_report()
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return 1
        
    return 0

if __name__ == '__main__':
    exit(main())
