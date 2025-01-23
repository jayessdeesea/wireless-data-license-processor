# Schema Component

## Quick Start

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

# Access validated data
print(am_record.model_dump_json(indent=2))
print(en_record.model_dump_json(indent=2))
```

## Overview

The Schema component provides Pydantic models that represent and validate FCC Wireless License Database records. These models follow Python best practices and implement comprehensive validation rules based on the [Public Access Database Definitions](https://www.fcc.gov/sites/default/files/public_access_database_definitions_20240215.pdf).

## Design Philosophy

The schema implementation follows these Python idiomatic principles:

1. **Type Safety**
   ```python
   from typing import Optional, Literal
   from datetime import date
   
   class BaseRecord(BaseModel):
       """Base class for all record types"""
       record_type: str  # Required in all records
       system_id: Optional[int] = None  # Optional in all records
       
       class Config:
           """Pydantic model configuration"""
           frozen = True  # Immutable after creation
           extra = "forbid"  # No extra fields allowed
   ```

2. **Descriptive Exceptions**
   ```python
   from pydantic import ValidationError
   
   try:
       record = AMRecord(record_type="XX")  # Invalid type
   except ValidationError as e:
       print(e.errors())  # Detailed validation errors
   ```

3. **Rich Comparisons**
   ```python
   def __eq__(self, other: object) -> bool:
       """Implement value-based equality"""
       if not isinstance(other, BaseRecord):
           return NotImplemented
       return self.model_dump() == other.model_dump()
   ```

4. **String Representations**
   ```python
   def __repr__(self) -> str:
       """Unambiguous string representation"""
       return f"{self.__class__.__name__}({self.model_dump()})"
   
   def __str__(self) -> str:
       """Human-readable string representation"""
       return f"{self.record_type} Record: {self.system_id}"
   ```

## Record Types

### Amateur License Record (AM)

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
    # ... additional fields with proper typing
```

[View full AM field list](https://github.com/[repository]/wdlp/blob/main/docs/fields/am_fields.md)

### Entity Record (EN)

```python
class ENRecord(BaseRecord):
    """Entity record schema with comprehensive validation"""
    record_type: Literal["EN"] = Field(
        description="Record type identifier [EN]"
    )
    entity_type: Optional[Literal["I", "B", "G", "M", "L"]] = Field(
        None,
        description="Entity type (I=Individual, B=Business, etc.)"
    )
    status_date: Optional[date] = Field(
        None,
        description="Status effective date"
    )
    # ... additional fields with proper typing
```

[View full EN field list](https://github.com/[repository]/wdlp/blob/main/docs/fields/en_fields.md)

## Validation Rules

### String Fields
```python
from pydantic import constr

# Fixed-length strings
call_sign: Optional[constr(min_length=10, max_length=10)] = None

# Variable-length strings with pattern
email: Optional[constr(pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")] = None
```

### Numeric Fields
```python
from pydantic import confloat, conint

# Integer with range
region_code: Optional[conint(ge=0, le=9)] = None

# Decimal with precision
latitude: Optional[confloat(ge=-90, le=90, decimal_places=6)] = None
```

### Date Fields
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
```

## Performance Considerations

1. **Memory Efficiency**
   ```python
   from pydantic import BaseModel
   
   class Config:
       """Optimize memory usage"""
       frozen = True  # Immutable instances
       extra = "forbid"  # No dynamic fields
       validate_assignment = False  # No runtime checks
   ```

2. **Validation Caching**
   ```python
   from functools import lru_cache
   
   @lru_cache(maxsize=1024)
   def validate_call_sign(value: str) -> bool:
       """Cache validation results for common values"""
       return bool(re.match(r"^[A-Z0-9]{10}$", value))
   ```

## Testing Strategy

```python
import pytest
from datetime import date

def test_am_record_validation():
    """Test AM record validation rules"""
    # Valid record
    record = AMRecord(
        record_type="AM",
        system_id=123,
        operator_class="A"
    )
    assert record.operator_class == "A"
    
    # Invalid operator class
    with pytest.raises(ValidationError) as exc:
        AMRecord(operator_class="X")
    assert "operator_class" in str(exc.value)

def test_date_validation():
    """Test date field validation"""
    # Invalid future date
    future = date.today().replace(year=date.today().year + 1)
    with pytest.raises(ValidationError) as exc:
        ENRecord(status_date=future)
    assert "Future dates not allowed" in str(exc.value)
```

## Common Pitfalls

1. **Optional vs Required Fields**
   ```python
   # Wrong: Implicit Optional
   field: str = None  # Type checker won't catch this
   
   # Correct: Explicit Optional
   field: Optional[str] = None
   ```

2. **Date Format Handling**
   ```python
   # Wrong: Direct string assignment
   status_date = "02/15/2024"  # Will raise ValidationError
   
   # Correct: Parse string to date
   from datetime import datetime
   status_date = datetime.strptime("02/15/2024", "%m/%d/%Y").date()
   ```

3. **Validation Error Handling**
   ```python
   # Wrong: Generic except
   try:
       record = AMRecord(...)
   except Exception as e:
       pass  # Catches too much
   
   # Correct: Specific except
   try:
       record = AMRecord(...)
   except ValidationError as e:
       # Handle validation errors
       for error in e.errors():
           print(f"Field: {error['loc']}, Error: {error['msg']}")
   ```

## Extension Guide

To add support for a new record type:

1. Create the schema class:
```python
from typing import Optional, Literal
from pydantic import BaseModel, Field

class NewRecord(BaseRecord):
    """New record type schema"""
    record_type: Literal["XX"] = Field(
        description="Record type identifier [XX]"
    )
    
    class Config:
        """Schema configuration"""
        schema_extra = {
            "example": {
                "record_type": "XX",
                "system_id": 123456789
            }
        }
```

2. Add validation rules:
```python
    @validator("field_name")
    def validate_field(cls, v: Optional[str]) -> Optional[str]:
        """Custom validation logic"""
        if v is not None:
            # Implement validation
            pass
        return v
```

3. Update factory mapping:
```python
SCHEMA_MAPPING = {
    "AM": AMRecord,
    "EN": ENRecord,
    "XX": NewRecord  # Add new schema
}
```

4. Add tests:
```python
def test_new_record():
    """Test new record type validation"""
    record = NewRecord(record_type="XX", ...)
    assert record.record_type == "XX"
```
