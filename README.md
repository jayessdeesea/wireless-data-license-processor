# Wireless Data License Processor

## Overview

"wdlp" is a Python-based tool for processing `.dat` files, transforming them into various formats (JSONL, Parquet, Ion,
CSV), and validating them against predefined schemas.

## Core Components

### 1. [Schema](docs/source/schema.md)

- Pydantic representations of the provided Public Access Database Definitions

### 2. [Producer](docs/source/producer.md)

- Pull Parser for reading .dat streams. Returns a record object

### 3. [Mapper](docs/source/mapper.md)

- Map record object into Pydantic record.

### 4. [Consumer](docs/source/consumer.md)

- A callable abstraction accepting a Pydantic record and writing to a variety of file formats, including JSONL,
  Parquet, Ion, and CSV.

### 5. [Main](docs/source/main.md)

- A CLI for accepting user instructions (input location, output location, etc..)
- Reads .dat files either on disk or in a zip archive and transforms into records
- Maps records into specific record types
- Writes to specified file format

## Additional Artifacts

### Docker Image and Docker Compose

Enables "wdlp" tool use without installing python locally.

## Technical Requirements

* Follow idiomatic Python conventions (e.g., PEP 8, the official Python style guide)
* Code is modular and documented
* Use the following libraries
    * Pydantic v2: for schema validation and field-specific transformations.
    * amazon.ion: for Ion file format
    * pandas and pyarrow: for Parquet format
    * tempfile: for temporary files
* Do not use deprecated features. Here are some examples:
    * Replace `__fields__` with `model_fields`.
    * Replace `dict` with `model_dump`.
    * Replace `parse_obj` with `model_validate`.