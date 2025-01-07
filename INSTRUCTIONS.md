# Instructions

## 1. Purpose

* Write a Python program to process .dat files extracted from a ZIP archive and transform them into a JSONL or parquet format
* Validate the data against predefined schemas and handle edge cases like missing fields, multiline records, and invalid formats

## 2. Schema Definitions

* Define schemas in a separate file named schemas.py and add support for the EN and FA record types
* Use Pydantic v2 for schema validation and field-specific transformations.
* Each schema should:
  * Have type constraints, field lengths, and optional fields
  * Use python native date types for date fields
* It is a fatal error if a record does not pass schema validation
* Check that the first field for each schema is named record_type, is type char, and has max length 2. If this is not true, log an error message and stop processing 
* The record_type field is required. All other fields are optional
* The value of the record_type should be the same as the .dat file name prefix. For example, all the record_type values in the file EN.dat are "EN". Stop processing if this is not the case
* The program will include all fields for the schemas it supports
* The program will ignore .dat files in the zip archive when the program does not support the schema

## 3. Input Specifications

* Accept a ZIP archive containing .dat files.
* Each .dat file contains pipe-delimited ("|") rows.
* Rows can span multiple lines.

## 4. Output Specifications

* Write validated rows to a JSONL file.
* Halt processing for invalid rows, logging descriptive error messages.
* Write dates in a yyyy-mm-dd format

## 5. Command Line Interface (CLI)

* Implement a CLI with the following options:
  * -h or --help to display usage instructions.
  * -v or --version to show the program version.
  * -i or --input to specify the path to the ZIP archive. The default is a file called l_amat.zip in the current directory
  * -o or --output to specify the output directory. The default is a directory called wdlp-output in the current directory. Create the directory if it does not already exist.
  * -t or --file-type to specify the output file type. This could be JSONL or parquet. If not specified, infer the file type based on the output file name. If it is still ambiguous, assume JSONL

## 6. Edge Cases to Handle

* Properly process:
  * Multiline records with spaces.
  * Missing optional fields (fill with null in JSONL).
* Do not publish partial or corrupted results
  * Output files from previous runs are deleted when beginning processing
  * Intermediate files are saved with a temporary name and renamed after successful processing.
* Handle large input files
  * Avoid placing entire files into memory by using streaming. There is no threshold, always use streaming

## 7. Error Handling

* When encountering missing required fields (e.g., Record Type), invalid date formats, or corrupted rows 
  * Log errors with enough context for the operator to debug. For example, zip archive name, .dat file name, line number, what was expected, and the invalid value
  * Abort processing 

## 9. Implementation

* The program will not extract the .dat file to disk. Instead, it will stream process the entries in the ZIP archive
* For each .dat file type
  * Match the .dat file type to a corresponding schema. If you cannot match to a corresponding schema, indicate you are skipping this file
  * Indicate you are processing the .dat file
  * Process each file, validating and transforming records into the requested format
* Place the Pydantic schemas in a separate file
* Generate and place the tests in a separate file

## 10. Testing

Correctness is important. Run the following test cases each time you generate code.

Here is a sample schema 

```python

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class ExampleSchema(BaseModel):
  record_type: str = Field(default=None, max_length=2, alias="Record Type")
  number_1: Optional[int] = Field(max_length=256, alias="Number 1")
  number_2: Optional[int] = Field(max_length=256, alias="Number 2")
  string_1: Optional[str] = Field(max_length=256, alias="String 1")
  string_2: Optional[str] = Field(max_length=256, alias="String 2")
  date_1: Optional[str] = Field(max_length=256, alias="Date 1")
  date_2: Optional[str] = Field(max_length=256, alias="Date 2")

  @field_validator("asof", mode="before")
  def validate_date(cls, value):
    if value:
      return datetime.strptime(value, "%m/%d/%Y").strftime("%Y-%m-%d")
    return value
```

### 10.1. Single record

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
```

Expected Output:

```json
[
  {
    "Record Type": "EX",
    "Number 1": 1,
    "Number 2": 2,
    "String 1": "A",
    "String 2": "B",
    "Date 1": "2024-12-31",
    "Date 2": "2025-01-01"
  }
]
```

### 10.2. Multiple records

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
EX|3|4|C|D|12/31/2025|1/1/2026|
```

Expected Output:

```json
[
  {
    "Record Type": "EX",
    "Number 1": 1,
    "Number 2": 2,
    "String 1": "A",
    "String 2": "B",
    "Date 1": "2024-12-31",
    "Date 2": "2025-01-01"
  },
  {
    "Record Type": "EX",
    "Number 1": 3,
    "Number 2": 4,
    "String 1": "C",
    "String 2": "D",
    "Date 1": "2025-12-31",
    "Date 2": "2026-01-01"
  }
]

```

### 10.3. Multi-line record

Input:

```text
EX|1|2|A
B|B
C|12/31/2024|1/1/2025|
```

Expected Output:

```json
[
  {
    "Record Type": "EX",
    "Number 1": 1,
    "Number 2": 2,
    "String 1": "A\nB",
    "String 2": "B\nC",
    "Date 1": "2024-12-31",
    "Date 2": "2025-01-01"
  }
]
```

### 10.4. Multi-line record with spaces

Input:

```text
EX|1|2|A 
B|B    
C|12/31/2024|1/1/2025|
```

Expected Output:

```json
[
  {
    "Record Type": "EX",
    "Number 1": 1,
    "Number 2": 2,
    "String 1": "A \nB",
    "String 2": "B   \nC",
    "Date 1": "2024-12-31",
    "Date 2": "2025-01-01"
  }
]
```

### 10.5. Records with missing fields

Input:

```text
EX|||||||
```

Expected Output:

```json
[
  {
    "Record Type": "EX",
    "Number 1": null,
    "Number 2": null,
    "String 1": null,
    "String 2": null,
    "Date 1": null,
    "Date 2": null
  }
]
```

### 10.6. Ensure Proper Transformation of Dates

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
```

Expected Output:

```json
[
  {
    "Record Type": "EX",
    "Number 1": 1,
    "Number 2": 2,
    "String 1": "A",
    "String 2": "B",
    "Date 1": "2024-12-31",
    "Date 2": "2025-01-01"
  }
]
```

### 10.7. Detect and Handle Corrupted Rows

Input:

```text
Input:

```text
EX|Not an integer|2|A|B|12/31/2024|1/1/2025|
```

Expected Behavior:

* Log: "Error: Invalid row format at line 1: EX|Not an integer|2|A|B|12/31/2024|1/1/2025|"
* Processing: Halt further processing

### 10.8. Missing Required Fields

Input:

```text
|1|2|A|B|12/31/2024|1/1/2025|
```

Expected Behavior:

* Log: "Error: Missing required field 'Record Type' at line 1."
* Processing: Halt further processing

### 10.7. Invalid Date Format

Input:

```text
EX|1|2|A|B|12/31/2024|31/12/2025|
```

Expected Behavior:

* Log: "Error: Invalid date format at line 1. Expected MM/DD/YYYY, not 31/12/2025."
* Processing: Halt further processing

### 10.8. Output JSONL Format

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
```

Expected Output:

```json
[
  {
    "Record Type": "EX",
    "Number 1": 1,
    "Number 2": 2,
    "String 1": "A",
    "String 2": "B",
    "Date 1": "2024-12-31",
    "Date 2": "2025-01-01"
  }
]
```

## 12. Ensure Maintainability

* Comment the code.
* Use clear, modular functions.
* The function signatures should not make it hard to build tests
* readable code is better than clever code

## 13. Package Layout

```text
project_root/
├── wdlp/
│   ├── __init__.py           # Makes this directory a package.
│   ├── main.py               # Contains the main entry point and core logic.
│   ├── schemas.py            # Defines Pydantic schemas for data validation.
│   ├── utils.py              # Contains reusable helper functions 
│   └── constants.py          # Stores constants 
├── tests/
│   ├── __init__.py           # Makes this directory a package.
│   ├── test_main.py          # Tests for `main.py`.
│   ├── test_schemas.py       # Tests for `schemas.py`.
│   └── test_utils.py         # Tests for utility functions.
├── data/                     # Store sample `.dat` files or test data.
├── Dockerfile                # A way to run the project
├── INSTRUCTIONS.md           # This file
├── README.md                 # Documentation for the project.
├── requirements.txt          # Python dependencies (e.g., `pydantic`, `pytest`).
└── setup.py                  # For packaging and installation.
```