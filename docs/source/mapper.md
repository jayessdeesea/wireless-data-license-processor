# Mapper Component

## Overview

The Mapper component is responsible for transforming raw data records into validated Pydantic models. It employs Python's type system and validation patterns to ensure data integrity and provides detailed feedback for any validation failures. The implementation follows the factory pattern for extensibility and maintainability.

## Design Philosophy

The implementation follows key Python idiomatic principles:

1. **Type Safety**
   - Leverages Python's type system for interface clarity
   - Uses generic protocols for flexible mapping
   - Implements type-safe model creation
   - Provides comprehensive type hints

2. **Validation Pipeline**
   - Implements multi-step validation process
   - Tracks validation context and errors
   - Provides detailed error reporting
   - Allows custom validation rules

3. **Error Handling**
   - Provides rich error information
   - Includes line numbers for context
   - Details expected vs received values
   - Supports custom error messages

## Factory Implementation

The mapper factory provides:

1. **Dynamic Mapper Creation**
   - Creates record-specific mappers
   - Caches mapper instances
   - Supports runtime registration
   - Handles unknown record types

2. **Registration System**
   - Allows new mapper registration
   - Prevents duplicate registration
   - Maintains type safety
   - Supports mapper replacement

3. **Performance Features**
   - Implements caching strategy
   - Minimizes instance creation
   - Optimizes memory usage
   - Provides thread safety

## Record Type Mappers

### Amateur License Mapper

Handles Amateur License records with:

1. **Field Processing**
   - System ID conversion
   - Date parsing and validation
   - Operator class validation
   - Status tracking

2. **Data Transformations**
   - Numeric field conversion
   - Date standardization
   - Enumeration validation
   - Format normalization

### Entity Mapper

Processes Entity records with:

1. **Complex Field Handling**
   - Name parsing
   - Phone number standardization
   - Address formatting
   - Contact information validation

2. **Format Standardization**
   - Phone number formatting
   - Name capitalization
   - Address normalization
   - Data cleansing

## Validation Rules

The validation system provides:

1. **Field Validators**
   - Date format validation
   - Phone number verification
   - Email format checking
   - Numeric range validation

2. **Custom Rules**
   - Business logic validation
   - Cross-field validation
   - Conditional validation
   - Format enforcement

## Performance Optimization

Key performance features:

1. **Caching Strategy**
   - Result caching
   - Instance reuse
   - Lazy validation
   - Memory optimization

2. **Efficient Processing**
   - Minimal object creation
   - Optimized validation
   - Reduced memory usage
   - Fast field access

## Testing Strategy

The testing approach includes:

1. **Unit Tests**
   - Individual mapper testing
   - Validation rule verification
   - Error handling coverage
   - Edge case testing

2. **Integration Tests**
   - End-to-end mapping
   - Factory functionality
   - Error propagation
   - Performance verification

## Common Pitfalls

Important considerations:

1. **Field Access**
   - Use safe field access methods
   - Handle missing fields gracefully
   - Validate field presence
   - Provide default values

2. **Type Conversion**
   - Implement safe conversions
   - Handle invalid data
   - Provide clear errors
   - Use appropriate types

## Extension Guide

To add new record type support:

1. **Model Creation**
   - Define record schema
   - Specify field types
   - Add validation rules
   - Document constraints

2. **Mapper Implementation**
   - Create custom mapper
   - Add field processing
   - Implement validation
   - Handle edge cases

3. **Integration**
   - Register new mapper
   - Add test coverage
   - Update documentation
   - Verify performance

## Code Examples

Below are practical examples demonstrating key functionality:

### Quick Start
```python
from wdlp.mapper import MapperFactory
from wdlp.producer import Record
from typing import Iterator

def map_records(records: Iterator[Record]) -> Iterator[dict]:
    """Map raw records to validated models"""
    for record in records:
        mapper = MapperFactory.create_mapper(record.fields[0])
        if not mapper:
            logger.warning(f"No mapper for type: {record.fields[0]}")
            continue
            
        try:
            model = mapper(record)
            yield model.model_dump()
        except MapperError as e:
            logger.error(f"Mapping error: {e}")
            raise
```

### Type Safety Implementation
```python
from typing import Protocol, TypeVar, Type
from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)

class Mapper(Protocol[ModelT]):
    """Protocol defining mapper interface"""
    def __call__(self, record: Record) -> ModelT:
        """Map raw record to validated model"""
        ...

class BaseMapper(Generic[ModelT]):
    """Base implementation with common functionality"""
    model_class: ClassVar[Type[ModelT]]
    
    def __call__(self, record: Record) -> ModelT:
        """Template method for mapping"""
        fields = self._extract_fields(record)
        return self._create_model(fields)
```

### Error Handling Example
```python
@dataclass
class MapperError(Exception):
    """Rich error information"""
    record_line: int
    field_name: str
    expected: str
    received: str
    context: Optional[str] = None
    
    def __str__(self) -> str:
        parts = [
            f"Mapping error at line {self.record_line}",
            f"Field: {self.field_name}",
            f"Expected: {self.expected}",
            f"Received: {self.received}"
        ]
        if self.context:
            parts.append(f"Context: {self.context}")
        return "\n".join(parts)
