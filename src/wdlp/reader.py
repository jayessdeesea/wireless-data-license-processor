import logging
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, IO, Iterator

@dataclass
class Record:
    """Represents a single record from a .dat file.
    
    A record consists of fields separated by pipe characters (|).
    For example:
    - "a|b" creates two fields: "a" and "b"
    - "|" creates two empty fields: "" and ""
    - "field1" creates one field: "field1"
    """
    line: int
    fields: List[str]

    def __str__(self) -> str:
        """String representation for debugging"""
        return f"Record(line={self.line}, fields={self.fields})"

@dataclass
class ParseError(Exception):
    """Error during record parsing with detailed context"""
    line: int
    column: int
    expected: str
    received: str
    consumed_fields: List[str]
    partial_field: str
    context: str = ""

    def __str__(self) -> str:
        msg = [
            f"Parse error at line {self.line}, column {self.column}:",
            f"Expected: {self.expected}",
            f"Received: {self.received}",
            f"Consumed fields: {self.consumed_fields}"
        ]
        if self.partial_field:
            msg.append(f"Partial field: {self.partial_field}")
        if self.context:
            msg.append(f"Context: {self.context}")
        return "\n".join(msg)

class ParserState(Enum):
    """Parser state machine states for handling pipe-separated fields"""
    START = auto()        # Start of field or record
    FIELD = auto()        # Processing field content until a separator
    AFTER_PIPE = auto()   # Found a separator (|), ready for next field
    AFTER_CR = auto()     # Found CR, checking for LF

class ParserContext:
    """Context for parser state machine that processes pipe-separated fields.
    
    Tracks the current field being built (characters before a separator),
    completed fields (those that have been separated by pipes), and various
    metrics like line numbers and field counts."""
    def __init__(self):
        self.current_field = []  # List of characters for current field
        self.fields = []         # List of completed fields
        self.field_length = 0    # Length of current field
        self.field_count = 0     # Number of fields in current record
        self.line = 1           # Current line number
        self.column = 1         # Current column number
        self.record_start_line = 1  # Line where current record started
        self.error = None       # Any error that occurred
        self.in_record = False  # Flag to track if we're in a record

class PullParser:
    """Iterator-based parser for .dat files.
    
    Parses records where fields are separated by pipe characters (|).
    For example:
    - "a|b" creates two fields: "a" and "b"
    - "|" creates two empty fields: "" and ""
    - "field1" creates one field: "field1"
    
    Records must:
    - Have at least one field
    - End with a newline
    - Have fields <= MAX_FIELD_LENGTH characters
    - Have <= MAX_FIELDS fields
    """
    MAX_FIELD_LENGTH = 1024  # Maximum length for each field
    MAX_FIELDS = 256  # Maximum number of fields per record as per specification

    def __init__(self, input_stream: IO, verbose: bool = False):
        self.stream = input_stream
        self.context = ParserContext()
        self.state = ParserState.START
        self.buffer = ""
        self.pos = 0
        self.eof = False
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)

    def __iter__(self) -> Iterator[Record]:
        return self

    def _complete_field(self) -> str:
        """Convert current field characters to string"""
        field = ''.join(c if isinstance(c, str) else chr(c) for c in self.context.current_field)
        # Strip trailing newline if present
        if field.endswith('\n'):
            field = field[:-1]
        if field.endswith('\r'):
            field = field[:-1]
        return field

    def _log_state_transition(self, old_state: ParserState, char: str):
        """Log state transition if verbose mode is enabled"""
        if self.verbose and old_state != self.state:
            self.logger.debug(
                f"State transition: {old_state.name} -> {self.state.name}\n"
                f"  Input: '{char}'\n"
                f"  Current field: {''.join(self.context.current_field)}\n"
                f"  Fields: {self.context.fields}"
            )

    def __next__(self) -> Record:
        """Get next record or raise StopIteration/ParseError"""
        if self.context.error:
            raise self.context.error

        while True:
            # Process next character if available
            if self.pos < len(self.buffer):
                char = self.buffer[self.pos]
                self.pos += 1
            else:
                # Try to read more data
                chunk = self.stream.read(8192)
                if not chunk:
                    # Handle EOF
                    if self.state == ParserState.START and not self.context.fields:
                        raise StopIteration
                    elif self.state == ParserState.FIELD:
                        # Complete the record at EOF with current field
                        self._add_field(self._complete_field())
                        return self._complete_record()
                    else:
                        raise StopIteration
                self.buffer = chunk
                self.pos = 0
                char = self.buffer[self.pos]
                self.pos += 1
            
            # Log input character
            if self.verbose:
                self.logger.debug(f"Input char: '{char}' (ASCII: {ord(char)})")

            # Track column position
            if char == '\n':
                self.context.column = 1
            else:
                self.context.column += 1

            old_state = self.state
            if self.state == ParserState.START:
                if char == '|':
                    if not self.context.fields:
                        raise ParseError(
                            line=self.context.line,
                            column=self.context.column,
                            expected="minimum 1 field per record",
                            received="empty record",
                            consumed_fields=[],
                            partial_field="",
                            context="Record must contain at least one field"
                        )
                    self.state = ParserState.AFTER_PIPE
                    self.context.record_start_line = self.context.line
                    self.context.in_record = True
                    self._add_field("")
                elif char == '\n':
                    if self.context.in_record:
                        raise ParseError(
                            line=self.context.line,
                            column=self.context.column,
                            expected="minimum 1 field per record",
                            received="empty record",
                            consumed_fields=[],
                            partial_field="",
                            context="Empty record found"
                        )
                    self.context.line += 1
                    continue  # Skip blank lines
                elif char == '\r':
                    if self.context.in_record:
                        self.state = ParserState.AFTER_CR
                    continue  # Skip CR
                else:
                    self.state = ParserState.FIELD
                    self.context.record_start_line = self.context.line
                    self.context.current_field.append(char)
                    self.context.field_length = 1
                    self.context.in_record = True

                self._log_state_transition(old_state, char)

            elif self.state == ParserState.FIELD:
                if char == '|':
                    self.state = ParserState.AFTER_PIPE
                    self._add_field(self._complete_field())
                    self._log_state_transition(old_state, char)
                elif char == '\r':
                    self.state = ParserState.AFTER_CR
                    self._log_state_transition(old_state, char)
                elif char == '\n':
                    # Complete field and record at newline
                    self._add_field(self._complete_field())
                    record = self._complete_record()
                    if self.verbose:
                        self.logger.debug(f"Completed record at line {record.line}:\n  Fields: {record.fields}")
                    self.context.line += 1
                    self.context.in_record = False
                    return record
                else:
                    if self.context.field_length >= self.MAX_FIELD_LENGTH:
                        raise ParseError(
                            line=self.context.line,
                            column=self.context.column - 1,
                            expected=f"field length <= {self.MAX_FIELD_LENGTH}",
                            received=str(self.context.field_length + 1),
                            consumed_fields=self.context.fields.copy(),
                            partial_field=self._complete_field(),
                            context="Field length constraint violation"
                        )
                    self.context.current_field.append(char)
                    self.context.field_length += 1

            elif self.state == ParserState.AFTER_PIPE:
                if char == '|':
                    # Consecutive pipes create empty fields
                    self._add_field("")
                    self._log_state_transition(old_state, char)
                elif char == '\r':
                    self.state = ParserState.AFTER_CR
                    self._add_field("")
                    self._log_state_transition(old_state, char)
                elif char == '\n':
                    # Complete record with empty final field
                    self._add_field("")
                    record = self._complete_record()
                    if self.verbose:
                        self.logger.debug(f"Completed record at line {record.line}:\n  Fields: {record.fields}")
                    self.context.line += 1
                    self.context.in_record = False
                    return record
                else:
                    self.state = ParserState.FIELD
                    self.context.current_field = [char]
                    self.context.field_length = 1
                    self._log_state_transition(old_state, char)

            elif self.state == ParserState.AFTER_CR:
                if char == '\n':
                    # Complete field and record at CRLF
                    if self.context.current_field:
                        self._add_field(self._complete_field())
                    record = self._complete_record()
                    if self.verbose:
                        self.logger.debug(f"Completed record at line {record.line}:\n  Fields: {record.fields}")
                    self.context.line += 1
                    self.context.in_record = False
                    return record
                else:
                    # CR not followed by LF, treat as field content
                    self.state = ParserState.FIELD
                    self.context.current_field.append('\r')
                    self.context.field_length += 1
                    self.context.current_field.append(char)
                    self.context.field_length += 1
                    self._log_state_transition(old_state, char)

    def _add_field(self, field: str):
        """Add a field to the current record"""
        self.context.fields.append(field)
        self.context.current_field = []
        self.context.field_length = 0
        self.context.field_count += 1
        if self.context.field_count > self.MAX_FIELDS:
            raise ParseError(
                line=self.context.line,
                column=self.context.column,
                expected=f"field count <= {self.MAX_FIELDS}",
                received=str(self.context.field_count),
                consumed_fields=self.context.fields[:-1],
                partial_field="",
                context=f"Too many fields in record. This may indicate missing newlines or record delimiters in the input file. Fields found: {self.context.fields}"
            )

    def _complete_record(self) -> Record:
        """Complete and return the current record"""
        if len(self.context.fields) < 1:
            raise ParseError(
                line=self.context.record_start_line,
                column=self.context.column,
                expected="minimum 1 field per record",
                received=f"{len(self.context.fields)} fields",
                consumed_fields=self.context.fields.copy(),
                partial_field="",
                context="Record must contain at least one field"
            )
        
        record = Record(self.context.record_start_line, self.context.fields)
        self.context.fields = []
        self.context.field_count = 0
        self.state = ParserState.START
        return record
