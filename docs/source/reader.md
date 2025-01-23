# Reader Component

## Quick Start

```python
from wdlp.reader import PullParser
from typing import Iterator

# Stream records from a .dat file
def process_dat_file(path: str) -> Iterator[str]:
    """Process records from a .dat file efficiently"""
    with open(path, "r", encoding="utf-8") as f:
        parser = PullParser(f)
        try:
            for record in parser:
                yield process_record(record)
        except ParseError as e:
            logger.error(f"Parse error at line {e.line}: {e}")
            raise

# Example usage
for processed_record in process_dat_file("AM.dat"):
    print(processed_record)
```

## Overview

The Reader component implements a memory-efficient pull parser for reading FCC Wireless License Database `.dat` files. It uses Python's iterator protocol and a finite state machine (FSM) to ensure robust parsing and validation.

## Design Philosophy

The implementation follows these Python idiomatic principles:

1. **Iterator Protocol**
   ```python
   from typing import Iterator, IO
   from dataclasses import dataclass
   
   @dataclass(frozen=True)
   class Record:
       """Immutable record with line tracking"""
       line: int
       fields: tuple[str, ...]  # Immutable tuple of fields
       
       def __iter__(self) -> Iterator[str]:
           """Make record directly iterable"""
           return iter(self.fields)
   
   class PullParser:
       """Memory-efficient parser using iterators"""
       def __init__(self, stream: IO[str]):
           self._stream = stream
           self._line = 1
       
       def __iter__(self) -> Iterator[Record]:
           """Make parser iterable"""
           return self
       
       def __next__(self) -> Record:
           """Get next record or signal end"""
           record = self._parse_next()
           if record is None:
               raise StopIteration
           return record
   ```

2. **Context Management**
   ```python
   class DatFileReader:
       """Safe .dat file handling"""
       def __init__(self, path: str):
           self.path = path
           self._file = None
           
       def __enter__(self) -> PullParser:
           """Setup parser with file context"""
           self._file = open(self.path, "r", encoding="utf-8")
           return PullParser(self._file)
           
       def __exit__(self, exc_type, exc_val, exc_tb):
           """Ensure file cleanup"""
           if self._file:
               self._file.close()
   ```

3. **Type Hints**
   ```python
   from typing import Optional, Protocol
   
   class ParserState(Protocol):
       """Protocol for parser states"""
       def process_char(self, char: str) -> Optional["ParserState"]:
           """Process character and return next state"""
           ...
   ```

## Format Specification

The `.dat` file format follows this grammar:

```ebnf
dat-file     = record* EOF ;
record       = field+ EOL ;
field        = value SEPARATOR ;
value        = CHAR* ;
SEPARATOR    = "|" ;
EOL          = "\n" | "\r\n" ;
CHAR         = ? any UTF-8 except SEPARATOR ? ;
```

### Constraints
```python
class FormatConstraints:
    """Format validation constants"""
    MAX_FIELD_LENGTH: Final[int] = 1024
    MAX_FIELDS_PER_RECORD: Final[int] = 256
    FIELD_SEPARATOR: Final[str] = "|"
    
    @classmethod
    def validate_field(cls, value: str) -> None:
        """Validate field constraints"""
        if len(value) > cls.MAX_FIELD_LENGTH:
            raise ValueError(f"Field exceeds {cls.MAX_FIELD_LENGTH} bytes")
        if cls.FIELD_SEPARATOR in value:
            raise ValueError("Field contains separator character")
```

## State Machine Implementation

```python
from enum import Enum, auto
from typing import Optional

class StateType(Enum):
    """Parser states"""
    START = auto()
    FIELD = auto()
    END_FIELD = auto()
    END_RECORD = auto()
    ERROR = auto()

class ParserContext:
    """Shared parser context"""
    def __init__(self):
        self.current_field = []
        self.fields = []
        self.line = 1
        
    def add_char(self, char: str) -> None:
        """Add character to current field"""
        self.current_field.append(char)
        
    def end_field(self) -> None:
        """Complete current field"""
        self.fields.append("".join(self.current_field))
        self.current_field = []

class FiniteStateMachine:
    """Parser state machine"""
    def __init__(self):
        self.state = StartState()
        self.context = ParserContext()
    
    def process_char(self, char: str) -> None:
        """Process single character"""
        next_state = self.state.process_char(char)
        if next_state:
            self.state = next_state
```

## Error Handling

```python
from dataclasses import dataclass
from typing import Any

@dataclass
class ParseError(Exception):
    """Detailed parse error information"""
    line: int
    column: int
    expected: str
    received: str
    context: str = ""
    
    def __str__(self) -> str:
        """Human-readable error message"""
        msg = [
            f"Parse error at line {self.line}, column {self.column}:",
            f"Expected: {self.expected}",
            f"Received: {self.received}"
        ]
        if self.context:
            msg.append(f"Context: {self.context}")
        return "\n".join(msg)

class ErrorReporter:
    """Structured error reporting"""
    @staticmethod
    def field_too_long(line: int, length: int) -> ParseError:
        return ParseError(
            line=line,
            column=length,
            expected=f"field length <= {FormatConstraints.MAX_FIELD_LENGTH}",
            received=str(length)
        )
```

## Performance Optimization

1. **Buffer Management**
   ```python
   from io import StringIO
   
   class BufferedParser(PullParser):
       """Parser with efficient buffering"""
       BUFFER_SIZE = 8192  # 8KB buffer
       
       def __init__(self, stream: IO[str]):
           super().__init__(stream)
           self._buffer = StringIO()
           
       def _fill_buffer(self) -> bool:
           """Fill internal buffer"""
           chunk = self._stream.read(self.BUFFER_SIZE)
           if not chunk:
               return False
           self._buffer.write(chunk)
           return True
   ```

2. **Memory Efficiency**
   ```python
   from array import array
   
   class MemoryEfficientRecord:
       """Memory-optimized record storage"""
       __slots__ = ("line", "_fields")
       
       def __init__(self, line: int):
           self.line = line
           self._fields = array("u")  # Unicode array
   ```

## Testing Strategy

```python
import pytest
from io import StringIO

class TestPullParser:
    """Comprehensive parser tests"""
    
    @pytest.fixture
    def parser(self) -> PullParser:
        """Create parser with test data"""
        data = StringIO("field1|field2|\n")
        return PullParser(data)
    
    def test_basic_record(self, parser):
        """Test basic record parsing"""
        record = next(parser)
        assert record.line == 1
        assert record.fields == ("field1", "field2")
    
    @pytest.mark.parametrize("input_data,expected_error", [
        ("a" * 1025 + "|", "field too long"),
        ("no_terminator", "missing terminator"),
        ("|" * 257, "too many fields")
    ])
    def test_error_cases(self, input_data: str, expected_error: str):
        """Test error handling"""
        parser = PullParser(StringIO(input_data))
        with pytest.raises(ParseError) as exc:
            next(parser)
        assert expected_error in str(exc.value)
```

## Common Pitfalls

1. **Resource Management**
   ```python
   # Wrong: Manual file handling
   f = open("data.dat")
   parser = PullParser(f)  # File may not be closed
   
   # Correct: Context manager
   with open("data.dat") as f:
       parser = PullParser(f)
   ```

2. **Error Recovery**
   ```python
   # Wrong: Continue after error
   try:
       for record in parser:
           process(record)
   except ParseError:
       continue  # Data may be corrupted
   
   # Correct: Stop processing
   try:
       for record in parser:
           process(record)
   except ParseError as e:
       logger.error(f"Stopping: {e}")
       raise
   ```

## Extension Guide

To support new format variations:

1. Create custom state:
```python
class CustomState(ParserState):
    """New parser state"""
    def process_char(self, char: str) -> Optional[ParserState]:
        """Custom processing logic"""
        if char == CUSTOM_SEPARATOR:
            return EndFieldState()
        return None
```

2. Add format validation:
```python
class CustomFormatConstraints(FormatConstraints):
    """Extended format constraints"""
    CUSTOM_SEPARATOR: Final[str] = ";"
    
    @classmethod
    def validate_custom_field(cls, value: str) -> None:
        """Additional validation rules"""
        super().validate_field(value)
        if not cls.is_valid_custom_format(value):
            raise ValueError("Invalid custom format")
```

3. Update parser configuration:
```python
class CustomParser(PullParser):
    """Parser with custom behavior"""
    def __init__(self, stream: IO[str], **config):
        super().__init__(stream)
        self.config = config
        
    def _create_state_machine(self) -> FiniteStateMachine:
        """Configure custom state machine"""
        return CustomStateMachine(self.config)
