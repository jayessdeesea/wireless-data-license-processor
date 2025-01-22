# Wireless Data License Processor

## Overview

"wdlp" is a Python-based tool for processing the Federal Communications Commission (FCC) Wireless Data License Database `.dat` files. It transforms these files into various formats (JSONL, Parquet, Ion, CSV) while validating them against predefined schemas based on the [Public Access Database Definitions](https://www.fcc.gov/sites/default/files/public_access_database_definitions_20240215.pdf).

The raw `.dat` files can be obtained from the [FCC ULS Daily/Weekly Transactions](https://www.fcc.gov/uls/transactions/daily-weekly) page.

## Installation

### Using pip
```bash
pip install wdlp
```

### Using Docker
1. Build the image:
```bash
docker build -t wdlp .
```

The image uses Python 3.9-slim as its base to minimize size while maintaining compatibility.

Note: The Docker Hub repository information will be added once the project is published.

## Usage

### Command Line Interface
```bash
# Basic usage (processes all .dat files in the ZIP archive)
wdlp --input archive.zip --output output_dir/ --format jsonl

# Specify different output format
wdlp --input archive.zip --output output_dir/ --format parquet
```

The tool currently supports processing ZIP archives containing `.dat` files. Each supported record type (AM, EN) will be processed and saved to a separate file in the output directory.

### Using Docker

#### Using docker run
```bash
# Mount local directories for input/output
docker run -v $(pwd)/input:/input -v $(pwd)/output:/output wdlp \
    --input /input/archive.zip \
    --output /output \
    --format jsonl
```

#### Using docker-compose
1. The included docker-compose.yml provides a pre-configured setup:
```yaml
version: '3.8'
services:
  wdlp:
    build: .
    volumes:
      - ./data:/data  # Mount the local data directory
```

2. Place your .dat files in the local `data/` directory

3. Run using compose:
```bash
docker-compose up
```

By default, docker-compose mounts the local `data/` directory to `/data` in the container and processes files using JSONL format.

### Supported Record Types
- AM: Amateur License records
- EN: Entity records

### Output File Structure
For an input archive containing AM.dat and EN.dat, the output directory will contain:
```
output_dir/
  ├── AM.[format]  # Amateur License records
  └── EN.[format]  # Entity records
```

### Supported Output Formats
- JSONL (JSON Lines)
- Parquet
- Ion (Amazon Ion)
- CSV

## Core Components

### 1. [Schema](docs/source/schema.md)
- Pydantic models representing FCC Public Access Database Definitions
- Currently supports two record types:
  - AM: Amateur License records (18 fields)
  - EN: Entity records (30 fields)
- Comprehensive validation including:
  - Field data types (char, varchar, numeric, dates)
  - String length constraints
  - Numeric ranges
  - Pattern matching
- All fields are optional to handle partial records
- Uses native Python dates for date fields
- Detailed field descriptions for clarity and context

### 2. [Producer](docs/source/producer.md)
- Pull Parser for reading `.dat` streams following a strict format:
  - Records consist of fields. 
  - Fields are terminated by `|` character
  - Records are terminated by end of line (`\n` or `\r\n`)
  - Fields can contain any character except the field terminator
- Enforces format constraints:
  - Field length: 0-1024 bytes
  - Records: 1-256 fields
- Implements a finite state machine for robust parsing
- Provides detailed error reporting:
  - Line number tracking
  - Expected vs received character information
  - Constraint violation details
- Returns Record objects with:
  - Line number for traceability
  - List of field values
- Memory-efficient streaming implementation

### 3. [Mapper](docs/source/mapper.md)
- Factory pattern implementation for record type mapping:
  - Abstract Mapper interface defining the mapping contract
  - Concrete implementations for each record type (AMMapper, ENMapper)
  - MapperFactory for creating appropriate mappers
- Field-by-field validation with detailed error reporting:
  - Full record context including line number
  - Expected vs received value information
  - Validation failure details
- Transforms raw record fields into typed Pydantic objects
- Supports extensibility for new record types

### 4. [Consumer](docs/source/consumer.md)
- Abstract writer implementation with format-specific extensions:
  - Context manager for safe file handling
  - Temporary file management
  - Atomic file operations (move on success, delete on failure)
- Supports multiple output formats with format-specific handling:
  - JSONL: Line-delimited JSON with null support
  - Parquet: Columnar storage with native date types
  - Ion: Text format (not binary) with null support
  - CSV: Simple text format with empty strings for nulls
- Consistent handling of:
  - Missing fields (nulls or empty strings based on format)
  - Dates (ISO 8601 or native types based on format)
- Factory pattern for writer creation and configuration

### 5. [Main](docs/source/main.md)
- Command-line interface with comprehensive options:
  - `-h, --help`: Display usage instructions
  - `-v, --version`: Show program version
  - `-i, --input`: Input ZIP archive path (default: l_amat.zip)
  - `-o, --output`: Output directory (default: output_dir/)
  - `-f, --file-format`: Output format (default: jsonl)
- Robust error handling and logging:
  - ZIP archive validation and access
  - File type verification (.dat files)
  - Schema recognition from filenames
  - Processing status and errors
- Performance monitoring and reporting:
  - Schema types processed
  - Records validated per schema
  - Total processing time
- Memory-efficient processing:
  - In-memory file handling for ZIP entries
  - Streaming record processing
  - Atomic file operations

## Technical Requirements

### Python Version
- Python 3.8 or higher

### Dependencies
- pydantic: Schema validation and transformations (v2 required)
- amazon.ion: Ion file format support
- pandas: Data manipulation
- pyarrow: Parquet format support
- Built-in libraries:
  - tempfile: Temporary file handling

Note: While specific versions are not pinned in requirements.txt, the code is tested with recent versions of these libraries. It's recommended to use the latest stable versions.

### Development Dependencies
- pytest: Testing framework

## Development Setup

1. Clone the repository:
```bash
git clone https://github.com/[repository]/wdlp.git
cd wdlp
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

4. Run tests:
```bash
pytest tests/
```

## Docker Support

The included Docker configuration provides a consistent runtime environment and simplifies deployment:

- `Dockerfile`: Defines a lightweight container image based on Python 3.9-slim
- `docker-compose.yml`: Provides a pre-configured environment with:
  - Automatic volume mounting for the `data/` directory
  - Default processing configuration
  - Container name management
- Supports custom volume mounting for processing files in any location
- Runs with minimal dependencies installed

### Docker Image Details
- Base Image: python:3.9-slim
- Working Directory: /app
- Installed Dependencies: Only those listed in requirements.txt
- Default Command: Processes .dat files from the mounted data directory

### Docker Compose Configuration
- Service Name: wdlp
- Container Name: wdlp_container
- Volume Mount: ./data:/data
- Default Format: JSONL

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

Please ensure your changes:
- Include appropriate tests
- Update documentation as needed
- Follow the existing code style
- Include a clear description of changes

## License

License information pending. Please contact the project maintainers for licensing details.

## Notes

- `.dat` files are binary files containing wireless license data in a specific format defined by the Public Access Database Definitions
- The tool is designed to handle large files efficiently through streaming processing
- Error handling includes detailed logging and validation reporting
- Schema versions correspond to Public Access Database Definition updates
