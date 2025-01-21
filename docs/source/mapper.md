# Mapper Component

## Overview

The Mapper component transforms raw records from the Producer into validated Pydantic models. It uses the factory pattern to create appropriate mappers for different record types (AM, EN) and provides detailed validation feedback.

## Architecture

### Mapper Interface

```python
from abc import ABC, abstractmethod
from typing import TypeVar, Type
from pydantic import BaseModel

T = TypeVar('T', bound=BaseModel)

class Mapper(ABC):
    """Abstract base class for record mappers"""
    @abstractmethod
    def __call__(self, record: Record) -> T:
        """Transform a record into a Pydantic model"""
        pass
```

### Factory Implementation

```python
class MapperFactory:
    """Factory for creating record type-specific mappers"""
    _mappers = {
        'AM': AMMapper,
        'EN': ENMapper
    }

    @classmethod
    def create_mapper(cls, mapper_type: str) -> Mapper:
        """Create a mapper for the specified record type"""
        if mapper_type not in cls._mappers:
            raise ValueError(f"Unsupported record type: {mapper_type}")
        return cls._mappers[mapper_type]()
```

### Record Type Mappers

```python
class AMMapper(Mapper):
    """Mapper for Amateur License records"""
    def __call__(self, record: Record) -> AMRecord:
        try:
            return AMRecord(
                record_type=record.fields[0],
                system_id=record.fields[1],
                # ... additional fields
            )
        except ValidationError as e:
            raise MapperError(
                record=record,
                expected=str(e.model_fields),
                received=record.fields
            )

class ENMapper(Mapper):
    """Mapper for Entity records"""
    def __call__(self, record: Record) -> ENRecord:
        try:
            return ENRecord(
                record_type=record.fields[0],
                system_id=record.fields[1],
                # ... additional fields
            )
        except ValidationError as e:
            raise MapperError(
                record=record,
                expected=str(e.model_fields),
                received=record.fields
            )
```

## Error Handling

```python
class MapperError(Exception):
    """Error during record mapping"""
    def __init__(self, record: Record, expected: str, received: List[str]):
        self.record = record
        self.expected = expected
        self.received = received
        super().__init__(
            f"Mapping error at line {record.line}:\n"
            f"Expected: {expected}\n"
            f"Received: {received}"
        )
```

## Validation Process

1. Field Count Validation
   - Verify number of fields matches schema
   - Raise error if mismatch

2. Data Type Validation
   - Convert strings to appropriate types
   - Validate against Pydantic field constraints
   - Handle optional fields

3. Field-Specific Validation
   - Length constraints
   - Pattern matching
   - Numeric ranges
   - Date formats

## Usage Example

```python
# Create mapper for AM records
mapper = MapperFactory.create_mapper('AM')

try:
    # Map raw record to Pydantic model
    record = Record(line=1, fields=['AM', '123456789', ...])
    am_record = mapper(record)
    
    # Access validated fields
    print(f"System ID: {am_record.system_id}")
    print(f"Call Sign: {am_record.call_sign}")
except MapperError as e:
    print(f"Validation failed: {e}")
```

## Extension

To add support for a new record type:

1. Create Pydantic model for the record type
2. Implement new mapper class
3. Add to MapperFactory._mappers
4. Update validation logic as needed

Example:
```python
class NewRecordMapper(Mapper):
    def __call__(self, record: Record) -> NewRecord:
        # Implementation for new record type
        pass

MapperFactory._mappers['NEW'] = NewRecordMapper
```
