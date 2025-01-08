Please use the updated instructions

# Instructions

## 1. Purpose

* Write a Python program to process .dat files extracted from a ZIP archive and transform them into specified output
  file formats: JSONL, Parquet, or Amazon Ion (text format only).
* Validate the data against predefined schemas and handle edge cases like missing fields, multiline records, and invalid
  formats.

## 2. Schema Definitions

* Use predefined schemas for AM and EN record types as defined in the public access database definitions PDF file.
* Each schema should have type constraints, field lengths, and optional fields.
* Update the schemas so all fields are optional.
* Place schemas in a separate schemas.py file for maintainability.

## 3. Input Specifications

* The program must accept a ZIP archive containing .dat files.
* Order of File Processing: Files can be processed in any order; no sorting is required.
* .dat files should be read and processed directly from the ZIP archive stream without creating temporary .dat files on
  disk.
* Skip non-.dat files and log this in verbose mode.
* Schema Selection: Determine the schema for each .dat file based on its prefix (e.g., use the EN schema for EN.dat).
  Skip files with unknown schemas and log their names.
* Each .dat file contains rows that:
    * Are pipe delimited (|).
    * May span multiple lines (A row ends only when a line ends with a |)
    * Represent missing values using a double pipe (||). For example, if the last two values are missing, the line ends
      in
      three pipes (|||).
* Validate all rows against the selected schema
* Halt all processing and raise a fatal exception when
    * the number of columns is less than or greater than expected for the schema.
    * there are invalid rows
    * Schema validation failures (e.g., type constraints, field lengths)
    * Mismatched Record Type values (the Record Type field must match the schema name).
* Treat the following scenarios as non-exceptional
    * ZIP archives without .dat files
    * A .dat file that is empty (log that no rows are found)

## 4. Output Specifications

* Support the following file formats
    * JSONL: Each row is serialized as a single line of JSON
    * Parquet: Rows are sorted in an efficient, columnar format
    * Amazon Ion (text format only): Each row is serialized as a single line of Amazon Ion text
* Write a separate output file for each table type. For example:
    * For EN.dat in JSONL file format, write to EN.jsonl
    * For AM.dat in Parquet format, write to AM.parquet
* Output null values for missing optional fields if the format supports them (e.g., JSONL, Ion).
* For formats that don't support nulls, use an empty string ("") for missing optional fields.
* Dates must be formatted as:
    * ISO 8601 (yyyy-mm-dd) for formats without native date support.
    * Native date types for formats that support them (e.g., Parquet).

## 5. Command Line Interface (CLI)

Implement a CLI with the following options:

* -h or --help: Display usage instructions.
* -v or --version: Show the program version.
* --verbose: Enable debugging messages. Verbose mode should log processing progress, skipped files, and errors.
* -i or --input: Specify the path to the ZIP archive. Default: l_amat.zip.
* -o or --output: Specify the output directory. Default: wdlp-output. Create the directory if it does not exist.
* -t or --output-file-format: Specify the output file format. If not provided, infer the format based on the output file
  name. Default: JSONL.

## 6. Error Handling

Halt processing and raise exceptions for:

* Missing or invalid Record Type.
* Mismatched column counts.
* Schema validation failures.

Provide debugging context for fatal exceptions, including:

* ZIP archive name
* .dat file name
* Line number of the error
* Expected value and what was received
* Expected field name and type for schema validation failures.

## 7. Implementation

* Use Python for implementation.
* Use Pydantic v2 for schema validation and field-specific transformations.
* Do not use deprecated features like \_\_fields\_\_ (use model_fields) or dict (use model_dump)
* For Ion output, use ion.dumps() with binary=False to emit Ion text
* The program must not create intermediate files on disk for .dat processing. The .dat file content should be read,
  validated, and transformed directly from memory streams to the output formats.
* Ensure records are written immediately to disk after processing, without buffering in memory, except where required by
  file formats like Parquet.
* The program must:
    * Automatically delete output files from previous runs before starting.
    * Save intermediate files as temporary file and rename them after successful processing.
    * The runtime environment might not be UNIX so do not use /tmp for temporary storage
* Print the program version, input, output, and output file format when the program starts.
* Provide periodic updates, including:
    * Start and end of each .dat file processing.
    * Number of valid rows processed for each .dat file.
    * Log skipped files, including the file name and reason.
* Print a final summary, including:
    * Total number of schema types processed.
    * Total number of records validated per schema type.
    * Total number of skipped files, including reasons for skipping (e.g., unsupported files or unknown schemas).
    * Total number of valid records processed across all schemas.

## 9. Testing

* Run test cases when you modify the python code. This may uncover code that does not compile (ex. missing import
  statements) or logic bugs

### 9.1. Example Data File Format

Schema:

| Position | Data Element     | Definition   |
|----------|------------------|--------------|
| 1        | Record Type [EX] | char(2)      |
| 2        | Number 1         | numeric(9,0) |
| 3        | Number 2         | numeric(9,0) |
| 4        | String 1         | varchar(256) |
| 5        | String 2         | varchar(256) |
| 6        | Date 1           | mm/dd/yyyy   |
| 7        | Date 2           | mm/dd/yyyy   |

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
```

JSONL Output:

```text
{ "Record Type": "EX", "Number 1": 1, "Number 2": 2, "String 1": "A", "String 2": "B", "Date 1": "2024-12-31", "Date 2": "2025-01-01" }
```

### 9.2. Test Single record

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
```

Expected Output:

```text
 { "Record Type": "EX", "Number 1": 1, "Number 2": 2, "String 1": "A", "String 2": "B", "Date 1": "2024-12-31", "Date 2": "2025-01-01"  }
```

### 9.3. Test Multiple records

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
EX|3|4|C|D|12/31/2025|1/1/2026|
```

Expected Output:

```text
{ "Record Type": "EX", "Number 1": 1, "Number 2": 2, "String 1": "A", "String 2": "B", "Date 1": "2024-12-31", "Date 2": "2025-01-01" }
{ "Record Type": "EX", "Number 1": 3, "Number 2": 4, "String 1": "C", "String 2": "D", "Date 1": "2025-12-31", "Date 2": "2026-01-01" }
```

### 9.4. Test Multi-line record

Input:

```text
EX|1|2|A
B|B
C|12/31/2024|1/1/2025|
```

Expected Output:

```text
{ "Record Type": "EX", "Number 1": 1, "Number 2": 2, "String 1": "A\nB", "String 2": "B\nC", "Date 1": "2024-12-31", "Date 2": "2025-01-01" }
```

### 9.5. Test Multi-line record with spaces

Input:

```text
EX|1|2|A 
B|B    
C|12/31/2024|1/1/2025|
```

Expected Output:

```text
{ "Record Type": "EX", "Number 1": 1, "Number 2": 2, "String 1": "A \nB", "String 2": "B   \nC", "Date 1": "2024-12-31", "Date 2": "2025-01-01" }
```

### 9.6. Test Records with missing fields

Input:

```text
EX|||||||
```

Expected Output:

```text
{ "Record Type": "EX", "Number 1": null, "Number 2": null, "String 1": null, "String 2": null, "Date 1": null, "Date 2": null }
```

### 9.7. Test Ensure Proper Transformation of Dates

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
```

Expected Output:

```text
{ "Record Type": "EX", "Number 1": 1, "Number 2": 2, "String 1": "A", "String 2": "B", "Date 1": "2024-12-31", "Date 2": "2025-01-01" }
```

### 9.8. Test Detect and Handle Corrupted Rows

Input:

```text
Input:

```text
EX|Not an integer|2|A|B|12/31/2024|1/1/2025|
```

Expected Behavior:

* Log: "Error: Invalid row format at line 1: EX|Not an integer|2|A|B|12/31/2024|1/1/2025|"
* Processing: Halt further processing

### 9.9. Test Missing Required Fields

Input:

```text
|1|2|A|B|12/31/2024|1/1/2025|
```

Expected Behavior:

* Log: "Error: Missing required field 'Record Type' at line 1."
* Processing: Halt further processing

### 9.10. Test Invalid Date Format

Input:

```text
EX|1|2|A|B|12/31/2024|31/12/2025|
```

Expected Behavior:

* Log: "Error: Invalid date format at line 1. Expected MM/DD/YYYY, not 31/12/2025."
* Processing: Halt further processing

### 9.11. Test Output JSONL Format

Input:

```text
EX|1|2|A|B|12/31/2024|1/1/2025|
```

Expected Output:

```text
{ "Record Type": "EX", "Number 1": 1, "Number 2": 2, "String 1": "A", "String 2": "B", "Date 1": "2024-12-31", "Date 2": "2025-01-01" }
```

## 10. Ensure Maintainability

* Comment the code.
* Use clear, modular functions.
* The function signatures should not make it hard to build tests
* readable code is better than clever code

## 11. Package Layout

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