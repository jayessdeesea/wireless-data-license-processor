# Main Component

## Overview

The Main component provides a command-line interface for processing FCC Wireless License Database files. It coordinates the interaction between all components (Producer, Mapper, Consumer) and provides comprehensive logging and error reporting.

## Command Line Interface

### Usage
```bash
wdlp [-h] [-v] -i INPUT -o OUTPUT [-f {jsonl,parquet,ion,csv}]
```

### Arguments
| Argument | Description | Default |
|----------|-------------|---------|
| -h, --help | Show help message | |
| -v, --version | Show program version | |
| -i, --input | Input ZIP archive path | l_amat.zip |
| -o, --output | Output directory path | output/ |
| -f, --format | Output format | jsonl |

### Output Formats
- jsonl: JSON Lines format
- parquet: Apache Parquet format
- ion: Amazon Ion text format
- csv: Comma-Separated Values

## Processing Flow

### 1. Initialization
```python
def main():
    # Parse command line arguments
    args = parse_args()
    
    # Configure logging
    setup_logging()
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
```

### 2. ZIP Archive Processing
```python
def process_zip(zip_path: str, output_dir: str, format: str):
    """Process all .dat files in ZIP archive"""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for entry in zf.namelist():
            if not entry.endswith('.dat'):
                logger.info(f"Skipping non-.dat file: {entry}")
                continue
            
            process_dat_file(zf.open(entry), entry, output_dir, format)
```

### 3. Record Processing
```python
def process_dat_file(stream: IO, filename: str, output_dir: str, format: str):
    """Process a single .dat file"""
    # Determine schema from filename
    schema_type = os.path.splitext(os.path.basename(filename))[0].upper()
    
    # Create components
    parser = PullParser(stream)
    mapper = MapperFactory.create_mapper(schema_type)
    writer = WriterFactory.create_writer(format, output_path)
    
    # Process records
    for record in parser:
        typed_record = mapper(record)
        writer.write(typed_record.model_dump())
```

## Error Handling

### Input Validation
```python
def validate_input(args):
    """Validate command line arguments"""
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file not found: {args.input}")
        
    if not zipfile.is_zipfile(args.input):
        raise ValueError(f"Input must be a ZIP archive: {args.input}")
```

### Processing Errors
- ZIP file errors (missing, corrupt)
- Schema validation errors
- File format errors
- I/O errors

## Logging

### Configuration
```python
def setup_logging():
    """Configure logging format and level"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
```

### Log Events
- ZIP archive processing start/end
- File processing status
- Record counts
- Errors and warnings
- Performance metrics

## Performance Reporting

### Statistics
```python
class ProcessingStats:
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
```

## Usage Example

```bash
# Process all .dat files in archive
wdlp --input l_amat.zip --output processed/ --format parquet

# Processing output
Processing l_amat.zip...
Found AM.dat - processing Amateur License records
Found EN.dat - processing Entity records
Skipping README.txt - not a .dat file

Processing Summary:
Total time: 5.23 seconds
AM records: 1234
EN records: 5678
```

## Extension

To add new functionality:

1. Add new command line arguments
2. Implement new processing logic
3. Update error handling
4. Add appropriate logging
5. Update performance metrics

Example:
```python
parser.add_argument('--validate-only', 
                   action='store_true',
                   help='Validate records without writing output')
```
