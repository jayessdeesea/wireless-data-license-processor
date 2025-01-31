# Schema Component

## Overview

The Schema component defines and validates FCC Wireless License Database records using Pydantic models. It implements comprehensive validation rules based on the [Public Access Database Definitions](https://www.fcc.gov/sites/default/files/public_access_database_definitions_20240215.pdf) while following Python best practices for type safety and data validation.

## Design Philosophy

The implementation follows key Python idiomatic principles:

1. **Type Safety**
   - Enforces strict type checking
   - Provides clear field definitions
   - Implements immutable records
   - Prevents extra field addition

2. **Descriptive Exceptions**
   - Provides detailed error messages
   - Includes field-specific context
   - Reports multiple validation errors
   - Offers error resolution hints

3. **Rich Comparisons**
   - Implements value-based equality
   - Supports record comparison
   - Provides hash functionality
   - Enables sorting capabilities

4. **String Representations**
   - Offers debug-friendly output
   - Provides human-readable format
   - Includes essential fields
   - Maintains consistency

## Record Types

### Amateur License Record (AM)

Key features include:

1. **Core Fields**
   - Record type identifier
   - System identifier
   - Operator class
   - License status

2. **Validation Rules**
   - System ID range checks
   - Valid operator classes
   - Date format validation
   - Required field checks

### Entity Record (EN)

Key features include:

1. **Core Fields**
   - Entity type classification
   - Status tracking
   - Contact information
   - Location details

2. **Validation Rules**
   - Entity type validation
   - Date range checks
   - Contact format validation
   - Address verification

## Validation Rules

The validation system implements:

1. **String Field Rules**
   - Length constraints
   - Pattern matching
   - Character set validation
   - Format verification

2. **Numeric Field Rules**
   - Range validation
   - Precision control
   - Type conversion
   - Format checking

3. **Date Field Rules**
   - Range restrictions
   - Format validation
   - Timezone handling
   - Historical constraints

## Performance Considerations

Key optimizations include:

1. **Memory Efficiency**
   - Immutable instances
   - Restricted field addition
   - Optimized validation
   - Efficient storage

2. **Validation Optimization**
   - Result caching
   - Lazy validation
   - Batch processing
   - Quick field access

## Testing Strategy

The testing approach covers:

1. **Validation Testing**
   - Field constraints
   - Error conditions
   - Edge cases
   - Format rules

2. **Integration Testing**
   - Cross-field validation
   - Record relationships
   - Data consistency
   - Error propagation

## Common Pitfalls

Important considerations:

1. **Optional vs Required Fields**
   - Use explicit Optional types
   - Handle missing fields
   - Provide defaults
   - Validate presence

2. **Date Format Handling**
   - Parse dates properly
   - Handle timezones
   - Validate ranges
   - Format consistently

3. **Validation Error Handling**
   - Use specific exceptions
   - Provide context
   - Handle multiple errors
   - Log appropriately

## Extension Guide

To add new record types:

1. **Schema Definition**
   - Create record class
   - Define field types
   - Add constraints
   - Document fields

2. **Validation Rules**
   - Implement validators
   - Add custom rules
   - Define error messages
   - Test validation

3. **Integration**
   - Update mappings
   - Add factory support
   - Include tests
   - Update documentation

## Code Examples

Below are practical examples demonstrating key functionality:

### Quick Start
```python
from wdlp.schema import AMRecord, ENRecord
from datetime import date

# Create and validate an Amateur License record
am_record = AMRecord(
    record_type="AM",
    system_id=123456789,
    call_sign="W1AW",
    operator_class="A"
)

# Create and validate an Entity record
en_record = ENRecord(
    record_type="EN",
    system_id=987654321,
    entity_name="Test Entity",
    status_date=date(2024, 2, 15)
)
```

### Record Type Implementation
```python
class AMRecord(BaseRecord):
    """Amateur License record schema with comprehensive validation"""
    record_type: Literal["AM"] = Field(
        description="Record type identifier [AM]"
    )
    system_id: Optional[int] = Field(
        None,
        ge=0,
        le=999999999,
        description="Unique system identifier (0-999999999)"
    )
    operator_class: Optional[Literal["A", "E", "G", "N", "T"]] = Field(
        None,
        description="Operator class (A=Advanced, E=Amateur Extra, etc.)"
    )
```

### Validation Example
```python
from datetime import date
from pydantic import validator
from typing import Optional

class DateField:
    """Mixin for date field validation"""
    status_date: Optional[date] = None
    
    @validator("status_date")
    def validate_date(cls, v: Optional[date]) -> Optional[date]:
        """Validate date is within acceptable range"""
        if v is None:
            return None
        if v.year < 1900:
            raise ValueError("Date must be after 1900")
        if v > date.today():
            raise ValueError("Future dates not allowed")
        return v
