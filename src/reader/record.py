from typing import List

class Record:
    """Represents a single record from a .dat file"""
    def __init__(self, line: int, fields: List[str]):
        """
        Initialize a record
        
        Args:
            line: Line number where record starts
            fields: List of field values
        """
        self.line = line
        self.fields = fields

        # Validate record constraints
        if len(fields) < 1:
            raise ValueError(f"Record at line {line} has no fields")
        if len(fields) > 256:
            raise ValueError(f"Record at line {line} has too many fields: {len(fields)}")
        
        # Validate field constraints
        for i, field in enumerate(fields):
            if len(field) > 1024:
                raise ValueError(f"Field {i} at line {line} exceeds maximum length: {len(field)}")
            if '|' in field:
                raise ValueError(f"Field {i} at line {line} contains invalid character '|'")

    def __str__(self) -> str:
        """String representation for debugging"""
        return f"Record(line={self.line}, fields={self.fields})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return self.__str__()

    def __len__(self) -> int:
        """Number of fields in record"""
        return len(self.fields)

    def __getitem__(self, index: int) -> str:
        """Get field value by index"""
        return self.fields[index]
