# Reader Component

## Overview

The Reader component provides a memory-efficient solution for parsing FCC Wireless License Database `.dat` files. It implements a pull parser pattern using Python's iterator protocol and a finite state machine (FSM) to ensure robust parsing and validation of the data.

## Design Philosophy

The implementation follows key Python idiomatic principles:

1. **Iterator Protocol**
   - Implements Python's iterator protocol for memory efficiency
   - Uses lazy evaluation to process records on-demand
   - Maintains minimal memory footprint during parsing
   - Provides clean iteration interface for consumers

2. **Context Management**
   - Ensures proper resource cleanup through context managers
   - Handles file operations safely
   - Manages parser lifecycle automatically
   - Prevents resource leaks

3. **Type Safety**
   - Leverages Python's type system for interface clarity
   - Uses protocols to define parser states
   - Implements immutable record structures
   - Provides type hints for better IDE support

## Format Specification

The `.dat` file format follows these rules:

1. **File Structure**
   - Contains zero or more records
   - UTF-8 encoding is required

2. **Record Format**
   - Minimum of 1 field per record
   - Maximum of 256 fields per record
   - Records end with an end of line (`\n` or `\r\n`)

3. **Field Format**
   - Minimum length of 0 bytes
   - Maximum length of 1024 bytes
   - Fields are separated by the pipe character (|)

## State Machine Implementation

The parser uses a finite state machine with the following states:

1. **Start State**
   - Initial state for each record
   - Begins field collection
   - Handles empty line cases
   - Records starting line number

2. **Field State**
   - Accumulates field content
   - Validates field constraints
   - Handles embedded newlines
   - Tracks column position

3. **After Pipe State**
   - Finalizes field content
   - Validates field count
   - Handles empty fields
   - Prepares for next field or record end

## Error Handling

The error handling system provides comprehensive context for debugging and error recovery:

1. **Location Information**
   - Input line number where record started
   - Precise column number where error occurred
   - Tracks line numbers across embedded newlines
   - Maintains accurate position in multi-line fields

2. **Consumed Content**
   - List of successfully parsed fields before error
   - Partial content of field where error occurred
   - Maximum field length enforcement
   - Field count validation

3. **Error Categories**
   - Field Length Violations
     * Reports line and column where limit exceeded
     * Includes all consumed fields before error
     * Shows partial field up to violation point
     * Provides context about length constraint

   - Field Count Violations
     * Reports line where limit exceeded
     * Lists all fields up to maximum allowed
     * Indicates total attempted fields
     * Includes context about count limitation

   - Malformed Input
     * Details expected vs received content
     * Shows successfully parsed fields
     * Includes partial field if applicable
     * Provides context about format violation

4. **Error Recovery**
   - Clean error reporting with complete context
   - Safe resource cleanup after errors
   - Proper error propagation
   - Logging integration with full details

## Performance Optimization

Key performance features include:

1. **Buffer Management**
   - Efficient 8KB read buffer
   - Minimal memory allocation
   - Optimized string handling
   - Character-by-character processing

2. **Memory Efficiency**
   - List-based field accumulation
   - Minimal object creation
   - Reusable context object
   - Efficient string joining

## Code Examples

Below are practical examples demonstrating key functionality:

### Basic Usage
```python
from wdlp.reader import PullParser

def process_dat_file(path: str):
    """Process records from a .dat file efficiently"""
    with open(path, "r", encoding="utf-8") as f:
        parser = PullParser(f)
        for record in parser:
            # Each record contains line number and fields
            print(f"Record at line {record.line}: {record.fields}")
```

### Error Handling
```python
from wdlp.reader import PullParser, ParseError

def process_with_error_handling(path: str):
    """Process records with detailed error handling"""
    with open(path, "r", encoding="utf-8") as f:
        parser = PullParser(f)
        try:
            for record in parser:
                process_record(record)
        except ParseError as e:
            print(f"Error at line {e.line}, column {e.column}")
            print(f"Successfully parsed fields: {e.consumed_fields}")
            if e.partial_field:
                print(f"Partial field content: {e.partial_field}")
            print(f"Context: {e.context}")
            raise
```

### Field Validation Example
```python
def validate_record(record):
    """Example of working with record data"""
    # Access line number where record started
    print(f"Processing record from line {record.line}")
    
    # Access fields as a list
    for i, field in enumerate(record.fields):
        print(f"Field {i}: {field}")
        
    # Records are immutable for safety
    return process_fields(record.fields)
```

## Common Pitfalls

Important considerations:

1. **Error Handling**
   - Always check error messages for line and column
   - Examine consumed fields for context
   - Consider partial field content
   - Log full error details

2. **Field Processing**
   - Handle embedded newlines properly
   - Respect field length limits
   - Consider empty field cases
   - Validate field counts

3. **Resource Management**
   - Use context managers for files
   - Handle cleanup in error cases
   - Monitor buffer usage
   - Close files properly

## Extension Points

To extend functionality:

1. **Custom Validation**
   - Add field content rules
   - Implement format checks
   - Add record validation
   - Extend error reporting

2. **Format Variations**
   - Modify field separators
   - Change length limits
   - Add header handling
   - Implement checksums

3. **Error Handling**
   - Add custom error types
   - Extend error context
   - Implement recovery
   - Add validation rules
