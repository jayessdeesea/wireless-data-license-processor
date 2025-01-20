# Objective

Develop a Python tool to process .dat files and ZIP archives containing .dat files, transforming them into specified
output formats (JSONL, Parquet, Ion, and CSV) while validating against predefined schemas (AM and EN).

# Part 1: Administrative

## Directory Tree Layout

```text
project_root/
├── src/                          # Main package
│   ├── __init__.py               # Marks the directory as a package
│   ├── mapper/                   # Map untyped records to typed records
│   │   ├── __init__.py
│   │   └── mappers.py            # Map untyped records to typed records
│   ├── schema/                   # Schema definitions
│   │   ├── __init__.py
│   │   ├── schemas.py            # Pydantic schema definitions
│   │   └── untyped_utils.py      # Utilities for untyped records
│   ├── reader/                   # Data reading utilities
│   │   ├── __init__.py
│   │   └── readers.py            # Map .dat streams into untyped records
│   ├── writer/                   # Data writing utilities
│   │   ├── __init__.py
│   │   └── writers.py            # AbstractWriter and format-specific writers
├── tests/                        # Test cases
│   ├── __init__.py
│   ├── schema/                   # Tests for schema package
│   │   ├── __init__.py
│   │   └── test_schemas.py
│   └── reader/                   # Tests for reader package
│       ├── __init__.py
│       └── test_readers.py
├── main.py                       # CLI entry point and core logic
├── data/                         # Sample .dat files and test data
├── Dockerfile                    # Containerization file
├── docker-compose.yml            # Containerization file
├── README.md                     # Project documentation
├── requirements.md               # Requirements document for the product
├── requirements.txt              # Python dependencies
└── setup.py                      # Packaging and installation script
```

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

# Part 2: Pydantic Data Objects

## Requirements

Build Pydantic data objects using the provided Public Access Database Definitions

- validate constraints (e.g., string length, numeric ranges, and regular expressions)
- Each Pydantic data object
    - Defines attribute name and type
    - Makes attributes `Optional`
    - Uses validation constraints (`min_length`, `max_length`, `pattern`, `description`) where possible
    - Makes `description` validation constraint value comprehensive for context and clarity
    - Uses `pattern` validation constraint instead of the deprecated `regex` validation constraints
    - Uses native Python dates for date types rather than strings
- Define the schema classes directly instead of encapsulating them in functions.
    - On-the-fly generation or isolation of the schema definitions is not required

Place the schema record types in src/schema/schemas.py

## Tasks

Run each task individually and ask me if I wish to proceed

### Task 1: AM Schema

Build an AMRecord Pydantic data object for the AM Schema

- if there is a table immediately following, use it for the schema
    - otherwise extract the schema from the provided Public Access Database Definitions

| **Position** | **Data Element**            | **Type**     | **Description**                                |
|--------------|-----------------------------|--------------|------------------------------------------------|
| 1            | Record Type                 | char(2)      | Record Type identifier for the record.         |
| 2            | Unique System Identifier    | numeric(9,0) | Unique identifier for the system.              |
| 3            | ULS File Number             | char(14)     | Universal Licensing System (ULS) File Number.  |
| 4            | EBF Number                  | varchar(30)  | EBF Number (optional).                         |
| 5            | Call Sign                   | char(10)     | Call sign associated with the record.          |
| 6            | Operator Class              | char(1)      | Class of operator.                             |
| 7            | Group Code                  | char(1)      | Code for the group.                            |
| 8            | Region Code                 | tinyint      | Region code for the record.                    |
| 9            | Trustee Call Sign           | char(10)     | Call sign of the trustee.                      |
| 10           | Trustee Indicator           | char(1)      | Indicator if a trustee is assigned.            |
| 11           | Physician Certification     | char(1)      | Certification status from a physician.         |
| 12           | VE Signature                | char(1)      | Volunteer Examiner signature status.           |
| 13           | Systematic Call Sign Change | char(1)      | Indicator of systematic call sign change.      |
| 14           | Vanity Call Sign Change     | char(1)      | Indicator of vanity call sign change.          |
| 15           | Vanity Relationship         | char(12)     | Description of the vanity relationship.        |
| 16           | Previous Call Sign          | char(10)     | Previous call sign associated with the record. |
| 17           | Previous Operator Class     | char(1)      | Previous operator class of the licensee.       |
| 18           | Trustee Name                | varchar(50)  | Name of the trustee.                           |

### Task 2: EN Schema

Build an ENRecord Pydantic data object for the EN Schema

- if there is a table immediately following, use it for the schema
    - otherwise extract the schema from the provided Public Access Database Definitions

| **Position** | **Data Element**                | **Type**     | **Description**                               |
|--------------|---------------------------------|--------------|-----------------------------------------------|
| 1            | Record Type                     | char(2)      | Record Type identifier for the record.        |
| 2            | Unique System Identifier        | numeric(9,0) | Unique identifier for the system.             |
| 3            | ULS File Number                 | char(14)     | Universal Licensing System (ULS) File Number. |
| 4            | EBF Number                      | varchar(30)  | EBF Number (optional).                        |
| 5            | Call Sign                       | char(10)     | Call sign associated with the record.         |
| 6            | Entity Type                     | char(2)      | Type of the entity.                           |
| 7            | Licensee ID                     | char(9)      | Unique ID for the licensee.                   |
| 8            | Entity Name                     | varchar(200) | Name of the entity.                           |
| 9            | First Name                      | varchar(20)  | First name of the entity.                     |
| 10           | MI                              | char(1)      | Middle initial of the entity.                 |
| 11           | Last Name                       | varchar(20)  | Last name of the entity.                      |
| 12           | Suffix                          | char(3)      | Suffix for the entity's name.                 |
| 13           | Phone                           | char(10)     | Contact phone number.                         |
| 14           | Fax                             | char(10)     | Fax number.                                   |
| 15           | Email                           | varchar(50)  | Contact email address.                        |
| 16           | Street Address                  | varchar(60)  | Entity's street address.                      |
| 17           | City                            | varchar(20)  | City of the entity's address.                 |
| 18           | State                           | char(2)      | State of the entity's address.                |
| 19           | Zip Code                        | char(9)      | Zip code of the entity's address.             |
| 20           | PO Box                          | varchar(20)  | PO Box address (if applicable).               |
| 21           | Attention Line                  | varchar(35)  | Attention line for the address.               |
| 22           | SGID                            | char(3)      | System-generated ID.                          |
| 23           | FCC Registration Number (FRN)   | char(10)     | FCC registration number.                      |
| 24           | Applicant Type Code             | char(1)      | Code indicating type of applicant.            |
| 25           | Applicant Type Code Other       | varchar(40)  | Additional details for applicant type.        |
| 26           | Status Code                     | char(1)      | Status code of the entity.                    |
| 27           | Status Date                     | mm/dd/yyyy   | Date of the status update.                    |
| 28           | 3.7 GHz License Type            | char(1)      | Type of 3.7 GHz license.                      |
| 29           | Linked Unique System Identifier | numeric(9,0) | Linked unique identifier.                     |
| 30           | Linked Call Sign                | char(10)     | Linked call sign.                             |

# Part 3: .dat File State Transition Table

## .dat file EBNF

Follow this grammar exactly
- Step through this grammar mechanically, one step at a time
- a field can have whitespaces, including newline and carriage returns

```text
record        = field*, end-of-record ;
field         = field-value, end-of-field ;

field-value   = field-byte* ;
end-of-field  = "|" ;
field-byte    = ? any character except end-of-field ? ;

end-of-record = "\n" | ("\r", "\n") ;
```

## Constraints

### Length

- field 
  - Minimum length is 0
  - Maximum length is 256

### Size

- record
  - Minimum number of fields is 0
  - Maximum number of fields is 64

## Test Cases

- The input is in the .dat file format
- The expected results is in the JSON file format

### Test 1: Single Record Single Field Zero Length

Input:

```text
|
```

Expected Results:

```json
[
  [
    ""
  ]
]
```

### Test 2: Single Record Single Field 

Input:

```text
value|
```

Expected Results:

```json
[
  [
    "value"
  ]
]
```

### Test 3: Single Record Multiple Fields 

Input:

```text
value1|value2|
```

Expected Results:

```json
[
  [
    "value1",
    "value2"
  ]
]
```

### Test 4: Two Records

Input:

```text
value1|
value2|
```

Expected Results:

```json
[
  [
    "value1"
  ],
  [
    "value2"
  ]
]
```

### Test 5: Field with newline

Input:

```text
a|
b
c|d
e|
f|
```

Expected Results:

```json
[
  [
    "a"
  ],
  [
    "b\nc",
    "d\ne"
  ],
  [
    "f"
  ]
]
```

## Tasks

Run each task individually and ask me if I wish to proceed

### Task 1: Validate Test Cases on the EBNF

For each test case
- Mechanically walk through the provided input using the provided EBNF
  - Verify you get the expected result
  - Print pass if you get the expected result, fail otherwise

### Task 2: Initial State Transition Table

Use the .dat file EBNF to generate an initial state transition table

For each test case
- Mechanically walk through the provided input using the generated initial state transition table
  - Verify you get the expected result
  - Print pass if you get the expected result, fail otherwise

Print out the initial state transition table

### Task 3: Optimized State Transition Table (Using Ragel)

Use the Ragel algorithm, not Ragel itself, to optimize the initial state transition table.

For each test case
- Mechanically walk through the provided input using the generated optimized state transition table
  - Verify you get the expected result
  - Print pass if you get the expected result, fail otherwise

Print out the optimized state transition table





## Mapper Module

This module has logic to transform lines from a .dat file to typed records (e.g., AMRecord, ENRecord)

Please implement the following

### Process sequential lines from a .dat file and transform them to an untyped record (list of strings)

Steps

* Implement a DatLineToUntypedRecordProcessor class
    * Has a deque of untyped records attribute
    * Has a partial untyped record (list of strings) attribute
    * a method called process that accepts a sequential line from a .dat file.
        * split the line into untyped fields using the pipe (|) character and add them to the partial untyped record
        * if the line ends in pipe (|), add the partial untyped record to the back of the deque and assign the partial
          untyped record to a new list
    * a method called receive that returns a untyped record (list of strings) if available
        * Return the front of the deque or None if empty
    * a method called has_partial_record that will return true if a partial record is present
        * Return true if the partial untyped record list is non-empty
* Tests
    * An untyped record with a single field can be parsed
    * An untyped record with multiple fields can be parsed

* verify transformation of multiple records passes
* verify transformation of records with multi-line fields passes
* verify transformation of a .dat stream not ending in a pipe ("|") and eol fails
* verify line number included when raising an error
* verify line value is included when raising an error

Place the DatStreamToUntypedRecordIterator class in src/reader/readers.py

Place test for the DatStreamToUntypedRecordIterator class in tests/reader/test_readers.py

This module will map a single untyped record (list of strings) to a typed record (AMRecord, ENRecord)

Steps

* Implement a UntypedRecordToTypedRecordMapper class
    * Accept a record type schema (e.g., AMRecord, ENRecord)
    * has a map method that accepts a untyped record and returns a typed record

## Writer Module

* Support the following file formats
    * JSONL: Each row is serialized as a single line of JSON
    * Parquet: Rows are sorted in an efficient, columnar format
    * Amazon Ion (text format only): Each row is serialized as a single line of Ion text
    * Comma Separated Value (CSV): Each row represents a record, columns are separated by a comma
* Handle missing fields as:
    * null for formats supporting nulls (JSONL, Ion)
    * Empty string ("") for formats not supporting nulls (Parquet, CSV)
* Dates are formatted as:
    * ISO 8601 (yyyy-mm-dd) for formats without native date support.
    * Native date types for formats that support them (e.g., Parquet).

Implementation

* Implement an AbstractWriter
    * Accepts a path to the final file destination.
    * Is a context manager. Will manage the lifetime of a temporary file in the same directory as the final file
      destination
    * A write method for writing a single record to the temporary file
    * on exit
        * Will close the temporary file
        * If processing was successful, move temporary file to final location. Otherwise delete it.
* Extend AbstractWriter for each format (JSONLWriter, ParquetWriter, etc.)
* Write a separate output file for each table type:
    * Example: For EN.dat in JSONL format, write to EN.jsonl.
* The IonWriter should emit Ion text, not Ion binary
* Remember to import the tempfile module as necessary

# CLI Tool Integration

Provide a command-line interface to tie all modules together.

Steps

* Use `argparse` or related capabilities to accept command line inputs
    * -h or --help: Display usage instructions.
    * -v or --version: Show the program version.
    * -i or --input: Specify the path to the ZIP archive. Default: l_amat.zip.
    * -o or --output: Specify the output directory. Default: output. Create the directory if it does not exist.
    * -f or --output-file-format: Specify the output file format. Default: jsonl. Possible values are jsonl, parquet,
      ion, and
      csv
* Parse arguments and pass them to the appropriate modules
* Handle errors (e.g., invalid file paths, schema validation errors) by displaying debugging context and halting
  processing

Specific Requirements:

* The .dat file name prefix determines the schema for the file. E.g., AM.dat uses the AM schema

---


Place AbstractWriter and extended classes in wdlp/writer/writers.py

### Create CLI and Main Method

prompt

* Implement CLI options:
* Halt with a detailed error message for:
    * Missing or unreadable ZIP Archive
* Log the following:
    * ZIP archive path before opening.
    * Each ZIP entry names and action taken (e.g., reasons for skipping files such as skipping file abcd.txt because it
      does not end in .dat)
* Handle Zip Archive file by
    * Skipping non-.dat file entries
    * Skipping .dat files with unknown schemas
    * Opening the dat stream from the zip entry stream. Use in-memory file-like objects for ZIP archive processing
      instead of temporary files.
    * Deleting output files that match the schema and format of the current run and log when doing so
    * Create a DatToUntypedIterator to transform the .dat input stream into a untyped record iterable
    * Create a UntypedRecordToTypedRecordMapper to transform the untyped record into a typed record
    * Create an appropriate writer (e.g., CSVWriter) with the appropriate path (e.g., AM.csv)
    * Iterate through the untyped records, map them to typed records, and write them to disk
* Print a plain text table with
    * Total schema types processed.
    * Total records validated per schema.
    * Total processing time.

Place the CLI logic in project_root/wdlp/main.py.

## Generate the `_init__.py` files

prompt

Generate the `_init__.py` files

## Generate package layout

prompt

Generate the package layout using a directory tree layout






---

* Make all fields optional:
    * Missing fields are ignored during validation (no error is raised).
    * Present fields must conform to their type and constraint. Otherwise:
        * Log the row as invalid
        * Halt processing for the entire archive.
            * No partial output files should be left
            * Logs should contain a clear summary of the issue

### Step 5. Testing

Prompt:

* Write tests for all critical components:
    * Pydantic schemas.
    * DatIterable row validation and iteration logic.
    * Writer classes for JSONL, Parquet, and Ion formats.
    * CLI and main method integration.
* Include edge cases in tests:
    * Empty .dat files.
    * Files with missing fields.
    * Files with invalid rows.
    * Multiline rows.

Tell me when the instructions are incomplete, ambiguous, or inconsistent.

        * One invalid row in a .dat file stops processing the entire ZIP archive.
        * The program should not process remaining files in the archive after encountering an invalid file.

* Missing values are null values
* Optional fields without a value will be represented with a missing value.
* Optional fields are validated if present and must conform to constraints like type and length.
* Log debugging context and halt the program if any of the following is true
    * the number of columns is less than or greater than expected for the schema. When this happens, log
        * The line number
        * The row
        * the expected number of columns
        * the actual number of columns
    * Schema validation failures (e.g., type constraints, field lengths). When this happens, log
        * The line number
        * The row
        * The field name and type that failed validation
        * The field value
    * Mismatched Record Type values (the Record Type field must match the schema name).
        * The line number
        * The row
        * The expected value and the actual value
    * Invalid rows
        * The line number
        * The row


* Order of File Processing: ZIP entries can be visited in any order; no sorting is required.
* Schema Selection: Determine the schema for each .dat file based on its prefix (e.g., use the EN schema for EN.dat).
* When iterating through the zip entries, log the ZIP entry name and action
    * Skip non .dat files
    * Process .dat files if the schema is know (e.g., EN for EN.dat) otherwise skip
* Read .dat files directly from the ZIP entry stream. Do not create temporary .dat files on disk.
* Create a DatIterable with the ZIP entry stream and schema
* Create a temporary file to write to
* Create an appropriate writer (e.g., ParquetWriter) with the temporary file stream
* Pass the DatIterable to the write method of the writer
* Close the temporary file
* Move the temporary file to the final location (e.g., EN.parquet)
* Ensure temporary files are deleted if processing fails or is interrupted before renaming.
* Treat the following scenarios as non-exceptional
    * ZIP archives without .dat files
* Print a plain text final summary (key value pairs), including:
    * Total number of schema types processed.
    * Total number of records validated per schema type.
    * Total number of files skipped.
    * How long it took to run.

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

Please use the following layout:

```text
project_root/
├── wdlp/
│   ├── writer/
│   │   ├── abstract_writer.py    # Abstract class for writers
│   │   ├── ion_writer.py         # Logic for writing to Ion files
│   │   ├── jsonl_writer.py       # Logic for writing to JSON files
│   │   └── parquet_writer.py     # Logic for writing to Parquet files
│   ├── reader/
│   │   └── dat_iterator.py       # Contains DatIterable logic
│   ├── schemas/
│   │   ├── am_record.py          # Defines AMRecord schema
│   │   └── en_record.py          # Defines ENRecord schema
│   └── main.py                   # Main entry point and core logic
├── data/                         # Store sample `.dat` files or test data
├── Dockerfile                    # A way to containerize the project
├── INSTRUCTIONS.md               # Detailed instructions for project structure and usage
├── README.md                     # Documentation for the project
├── requirements.txt              # Python dependencies
└── setup.py                      # Packaging and installation script
```