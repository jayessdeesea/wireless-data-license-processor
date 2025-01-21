# Producer Component

## Overview

The Producer component implements a pull parser for reading FCC Wireless License Database `.dat` files. It uses a finite state machine (FSM) to ensure robust parsing and validation of the input format.

## .dat File Format Specification

The format is defined by the following EBNF grammar:

```ebnf
dat-file      = record*, EOF ; 
record        = field*, end-of-record ;
field         = field-value, end-of-field ;
field-value   = field-byte* ;
end-of-field  = "|" ;
field-byte    = ? any character except end-of-field ? ;
end-of-record = "\n" | ("\r", "\n") ;
```

### Format Constraints

1. Field Constraints:
   - Minimum length: 0 bytes (empty field)
   - Maximum length: 1024 bytes
   - Cannot contain the field separator (|)

2. Record Constraints:
   - Minimum fields: 1
   - Maximum fields: 256
   - Must end with a newline
   - Line number tracking required

## Implementation

### Record Class

```python
class Record:
    """Represents a single record from a .dat file"""
    def __init__(self, line: int, fields: List[str]):
        self.line = line      # Line number where record starts
        self.fields = fields  # List of field values

    def __str__(self):
        """String representation for debugging"""
        return f"Record(line={self.line}, fields={self.fields})"
```

### Finite State Machine

The parser uses a state machine to track parsing progress:

```python
class ParserState:
    """Base class for parser states"""
    def process_char(self, char: str, context: 'ParserContext') -> 'ParserState':
        """Process a single character and return next state"""
        pass

class StartState(ParserState):
    """Initial state, expecting field content or end-of-field"""
    pass

class FieldState(ParserState):
    """Processing field content"""
    pass

class EndOfFieldState(ParserState):
    """Found field separator, expecting next field or end-of-record"""
    pass

class ErrorState(ParserState):
    """Terminal state for parsing errors"""
    def __init__(self, line: int, expected: str, received: str):
        self.line = line
        self.expected = expected
        self.received = received
```

### Pull Parser

```python
class PullParser:
    """Iterator-based parser for .dat files"""
    def __init__(self, input_stream: IO):
        self.stream = input_stream
        self.fsm = FiniteStateMachine()
        self.current_line = 1
        self.error = None

    def __iter__(self):
        return self

    def __next__(self) -> Record:
        """Get next record or raise StopIteration/ParseError"""
        if self.error:
            raise self.error

        try:
            return self._parse_next_record()
        except ParseError as e:
            self.error = e
            raise
```

## Error Handling

The parser provides detailed error information:

```python
class ParseError(Exception):
    def __init__(self, line: int, expected: str, received: str):
        self.line = line
        self.expected = expected
        self.received = received
        super().__init__(
            f"Parse error at line {line}: "
            f"expected {expected}, got {received}"
        )
```

## Usage Example

```python
# Parse a .dat file
with open('AM.dat', 'r') as f:
    parser = PullParser(f)
    try:
        for record in parser:
            # Process each record
            print(f"Record at line {record.line}:")
            for i, field in enumerate(record.fields):
                print(f"  Field {i}: {field}")
    except ParseError as e:
        print(f"Error: {e}")
```

## Test Cases

### 1. Empty Field
```text
Input: |
Expected: Record(line=1, fields=[""])
```

### 2. Single Field
```text
Input: value|
Expected: Record(line=1, fields=["value"])
```

### 3. Multiple Fields
```text
Input: value1|value2|
Expected: Record(line=1, fields=["value1", "value2"])
```

### 4. Multiple Records
```text
Input: value1|
value2|
Expected: [
    Record(line=1, fields=["value1"]),
    Record(line=2, fields=["value2"])
]
```

### 5. Field with Newline
```text
Input: a\nb|
Expected: Record(line=1, fields=["a\nb"])
```

## Extension

To support new record formats:

1. Ensure format follows .dat file grammar
2. Verify field and record constraints
3. Add appropriate validation in parser
4. Update error handling as needed
