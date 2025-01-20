from typing import Iterator
from wdlp.schema.untyped_utils import UntypedField, UntypedRecord

class DatToRecordIterator:
    def __init__(self, file_stream):
        self.file_stream = file_stream

    def __iter__(self) -> Iterator[UntypedRecord]:
        for line_number, line in enumerate(self.file_stream, start=1):
            line = line.strip()
            if not line.endswith('|'):
                raise ValueError(f"Line {line_number}: Record does not end with a pipe (|). Content: {line}")

            # Split fields and remove the trailing pipe
            raw_fields = line[:-1].split('|')
            fields = [UntypedField(value) for value in raw_fields]

            yield UntypedRecord(fields)
