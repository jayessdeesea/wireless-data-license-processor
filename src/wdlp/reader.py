from dataclasses import dataclass
from enum import Enum, auto
from typing import List, IO, Iterator

@dataclass
class Record:
    """Represents a single record from a .dat file"""
    line: int
    fields: List[str]

    def __str__(self) -> str:
        """String representation for debugging"""
        return f"Record(line={self.line}, fields={self.fields})"

class ParseError(Exception):
    """Error during record parsing"""
    def __init__(self, line: int, expected: str, received: str, context: str = ""):
        self.line = line
        self.expected = expected
        self.received = received
        self.context = context
        msg = f"Parse error at line {line}: expected {expected}, got {received}"
        if context:
            msg += f" ({context})"
        super().__init__(msg)

class ParserState(Enum):
    """Parser state machine states"""
    START = auto()        # Start of field or record
    FIELD = auto()        # Processing field content
    AFTER_PIPE = auto()   # Found a pipe, checking if record ends

class ParserContext:
    """Context for parser state machine"""
    def __init__(self):
        self.current_field = []
        self.fields = []
        self.field_length = 0
        self.field_count = 0
        self.line = 1
        self.record_start_line = 1
        self.error = None

class PullParser:
    """Iterator-based parser for .dat files"""
    MAX_FIELD_LENGTH = 1024  # Maximum length for each field
    MAX_FIELDS = 256

    def __init__(self, input_stream: IO):
        self.stream = input_stream
        self.context = ParserContext()
        self.state = ParserState.START
        self.buffer = ""
        self.pos = 0
        self.eof = False

    def __iter__(self) -> Iterator[Record]:
        return self

    def __next__(self) -> Record:
        """Get next record or raise StopIteration/ParseError"""
        if self.context.error:
            raise self.context.error

        while True:
            # Read more data if buffer is empty
            if self.pos >= len(self.buffer):
                chunk = self.stream.read(8192)
                if not chunk:
                    if self.state == ParserState.AFTER_PIPE:
                        # Complete the record at EOF if we're after a pipe
                        record = Record(self.context.record_start_line, self.context.fields + [""])
                        self.state = ParserState.START
                        raise StopIteration
                    elif self.state != ParserState.START:
                        raise ParseError(
                            self.context.line,
                            "| followed by newline",
                            "EOF"
                        )
                    raise StopIteration
                self.buffer = chunk
                self.pos = 0

            # Process each character
            char = self.buffer[self.pos]
            self.pos += 1

            if self.state == ParserState.START:
                if char == '|':
                    self.state = ParserState.AFTER_PIPE
                    self.context.record_start_line = self.context.line
                    self._add_field("")
                elif char == '\n':
                    self.context.line += 1
                    continue  # Skip blank lines
                else:
                    self.state = ParserState.FIELD
                    self.context.record_start_line = self.context.line
                    self.context.current_field.append(char)
                    self.context.field_length = 1

            elif self.state == ParserState.FIELD:
                if char == '|':
                    self.state = ParserState.AFTER_PIPE
                    self._add_field(''.join(self.context.current_field))
                else:
                    self.context.field_length += 1
                    if self.context.field_length > self.MAX_FIELD_LENGTH:
                        raise ParseError(
                            self.context.line,
                            f"field length <= {self.MAX_FIELD_LENGTH}",
                            str(self.context.field_length)
                        )
                    self.context.current_field.append(char)

            elif self.state == ParserState.AFTER_PIPE:
                if char == '|':
                    self._add_field("")
                elif char == '\n':
                    if self.context.fields:  # Only complete record if we have fields
                        record = Record(self.context.record_start_line, self.context.fields + [""])
                        self.context.fields = []
                        self.context.field_count = 0
                        self.state = ParserState.START
                        self.context.line += 1
                        return record
                    self.state = ParserState.START  # Reset for next record
                    self.context.line += 1
                else:
                    self.state = ParserState.FIELD
                    self.context.current_field = [char]
                    self.context.field_length = 1

            if char == '\n' and self.state != ParserState.AFTER_PIPE:
                self.context.line += 1

    def _add_field(self, field: str):
        """Add a field to the current record"""
        self.context.fields.append(field)
        self.context.current_field = []
        self.context.field_length = 0
        self.context.field_count += 1
        if self.context.field_count > self.MAX_FIELDS:
            raise ParseError(
                self.context.line,
                f"field count <= {self.MAX_FIELDS}",
                str(self.context.field_count),
                "too many fields in record"
            )
