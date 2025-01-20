# Wdlp: .dat File Processor

## Overview
Wdlp is a Python-based tool for processing `.dat` files, transforming them into various formats (JSONL, Parquet, Ion, CSV), and validating them against predefined schemas.

## Features
- Supports multiple input sources:
  - `.dat` files
  - ZIP archives containing `.dat` files
- Outputs supported formats:
  - JSONL
  - Parquet
  - Amazon Ion (text format)
  - CSV
- Schema validation with detailed error reporting

## Installation

### Prerequisites
- Python 3.9+
- pip

### Using pip
1. Clone the repository:
   ```bash
   git clone <repository_url>
   cd wdlp
   ```

2. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install the package:
   ```bash
   python setup.py install
   ```

### Using Docker
1. Build the Docker image:
   ```bash
   docker build -t wdlp .
   ```

2. Run the container:
   ```bash
   docker run -v $(pwd):/data wdlp --input-dat-file /data/sample.dat --output /data/output
   ```

## Usage

### Command-Line Interface
Run the tool using the `wdlp` command:

```bash
wdlp [OPTIONS]
```

### Options
- `--input-zip-archive`:
  - Path to a ZIP archive containing `.dat` files. Can be specified multiple times.
- `--input-dat-file`:
  - Path to a `.dat` file. Can be specified multiple times.
- `--output`:
  - Directory for output files. Default: `output`.
- `--output-file-format`:
  - Output file format. Choices: `jsonl`, `parquet`, `ion`, `csv`. Default: `jsonl`.

### Example
To process a single `.dat` file and output JSONL:
```bash
wdlp --input-dat-file sample.dat --output output --output-file-format jsonl
```

To process a ZIP archive and output Parquet:
```bash
wdlp --input-zip-archive samples.zip --output output --output-file-format parquet
```

## Development

### Running Tests
Install `pytest`:
```bash
pip install pytest
```

Run the tests:
```bash
pytest
```

### Project Structure
```plaintext
project_root/
├── wdlp/
│   ├── __init__.py
│   ├── main.py
│   ├── schema/
│   │   ├── __init__.py
│   │   ├── schemas.py
│   │   └── untyped_utils.py
│   ├── reader/
│   │   ├── __init__.py
│   │   └── readers.py
│   ├── writer/
│   │   ├── __init__.py
│   │   └── writers.py
├── tests/
│   ├── __init__.py
│   ├── schema/
│   │   ├── __init__.py
│   │   └── test_schemas.py
│   ├── reader/
│   │   ├── __init__.py
│   │   └── test_readers.py
├── data/
│   └── (sample .dat and test data files)
├── Dockerfile
├── README.md
├── requirements.txt
├── setup.py
```

## License
This project is licensed under the MIT License. See `LICENSE` for more details.
