# Mapper Component

## Quick Start

```python
from wdlp.mapper import MapperFactory
from wdlp.producer import Record
from typing import Iterator

def map_records(records: Iterator[Record]) -> Iterator[dict]:
    """Map raw records to validated models"""
    for record in records:
        # Get appropriate mapper for record type
        mapper = MapperFactory.create_mapper(record.fields[0])
        if not mapper:
            logger.warning(f"No mapper for type: {record.fields[0]}")
            continue
            
        try:
            # Map and validate record
            model = mapper(record)
            yield model.model_dump()
        except MapperError as e:
            logger.error(f"Mapping error: {e}")
            raise

# Example usage
with open("AM.dat") as f:
    parser = PullParser(f)
    for mapped_record in map_records(parser):
        process_record(mapped_record)
```

## Overview

The Mapper component transforms raw records into validated Pydantic models using Python's type system and validation patterns. It follows the factory pattern for extensibility and provides detailed validation feedback.

## Design Philosophy

The implementation follows these Python idiomatic principles:

1. **Type Safety**
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

2. **Validation Pipeline**
   ```python
   from dataclasses import dataclass
   from typing import Any, Dict
   
   @dataclass
   class ValidationContext:
       """Context for validation pipeline"""
       record: Record
       fields: Dict[str, Any]
       errors: List[str] = field(default_factory=list)
       
       def add_error(self, field: str, error: str) -> None:
           """Track validation errors"""
           self.errors.append(f"{field}: {error}")
   
   class ValidationPipeline:
       """Chain of validation steps"""
       def __init__(self, steps: List[ValidationStep]):
           self.steps = steps
           
       def validate(self, context: ValidationContext) -> None:
           """Run all validation steps"""
           for step in self.steps:
               step.validate(context)
           if context.errors:
               raise ValidationError(context.errors)
   ```

3. **Error Handling**
   ```python
   from dataclasses import dataclass
   from typing import List, Optional
   
   @dataclass
   class MapperError(Exception):
       """Rich error information"""
       record_line: int
       field_name: str
       expected: str
       received: str
       context: Optional[str] = None
       
       def __str__(self) -> str:
           """Detailed error message"""
           parts = [
               f"Mapping error at line {self.record_line}",
               f"Field: {self.field_name}",
               f"Expected: {self.expected}",
               f"Received: {self.received}"
           ]
           if self.context:
               parts.append(f"Context: {self.context}")
           return "\n".join(parts)
   ```

## Factory Implementation

```python
from typing import Dict, Type, Optional
from functools import lru_cache

class MapperFactory:
    """Factory for creating record-specific mappers"""
    _mappers: Dict[str, Type[Mapper]] = {
        "AM": AMMapper,
        "EN": ENMapper
    }
    
    @classmethod
    @lru_cache(maxsize=None)
    def create_mapper(cls, record_type: str) -> Optional[Mapper]:
        """Get cached mapper instance"""
        mapper_class = cls._mappers.get(record_type)
        if not mapper_class:
            return None
        return mapper_class()
    
    @classmethod
    def register_mapper(cls, record_type: str, mapper_class: Type[Mapper]) -> None:
        """Register new mapper type"""
        if record_type in cls._mappers:
            raise ValueError(f"Mapper already registered for {record_type}")
        cls._mappers[record_type] = mapper_class
        cls.create_mapper.cache_clear()  # Clear cache
```

## Record Type Mappers

### Amateur License Mapper

```python
class AMMapper(BaseMapper[AMRecord]):
    """Mapper for Amateur License records"""
    model_class = AMRecord
    
    def _extract_fields(self, record: Record) -> Dict[str, Any]:
        """Extract and convert fields"""
        fields = super()._extract_fields(record)
        
        # Type conversions
        if fields.get("system_id"):
            fields["system_id"] = int(fields["system_id"])
            
        # Date parsing
        if fields.get("status_date"):
            fields["status_date"] = parse_date(fields["status_date"])
            
        return fields
        
    def _validate_fields(self, fields: Dict[str, Any]) -> None:
        """Additional field validation"""
        if "operator_class" in fields:
            validate_operator_class(fields["operator_class"])
```

### Entity Mapper

```python
class ENMapper(BaseMapper[ENRecord]):
    """Mapper for Entity records"""
    model_class = ENRecord
    
    def _extract_fields(self, record: Record) -> Dict[str, Any]:
        """Extract and convert fields"""
        fields = super()._extract_fields(record)
        
        # Handle complex fields
        if "name" in fields:
            fields.update(parse_name(fields["name"]))
            
        # Format standardization
        if "phone" in fields:
            fields["phone"] = standardize_phone(fields["phone"])
            
        return fields
```

## Validation Rules

```python
from datetime import datetime
from typing import Any

class FieldValidator:
    """Field-specific validation rules"""
    
    @staticmethod
    def validate_date(value: Any) -> datetime:
        """Validate and parse date field"""
        if not value:
            return None
        try:
            return datetime.strptime(value, "%m/%d/%Y")
        except ValueError as e:
            raise ValidationError(f"Invalid date format: {value}")
    
    @staticmethod
    def validate_phone(value: str) -> str:
        """Validate phone number format"""
        if not value:
            return None
        clean = re.sub(r"\D", "", value)
        if len(clean) != 10:
            raise ValidationError(f"Invalid phone number: {value}")
        return clean
```

## Performance Optimization

1. **Caching**
   ```python
   from functools import lru_cache
   
   class CachedMapper(BaseMapper):
       """Mapper with result caching"""
       @lru_cache(maxsize=1024)
       def __call__(self, record: Record) -> ModelT:
           """Cache mapped results"""
           return super().__call__(record)
   ```

2. **Lazy Loading**
   ```python
   class LazyMapper(BaseMapper):
       """Mapper with lazy validation"""
       def __call__(self, record: Record) -> ModelT:
           """Defer validation until access"""
           fields = self._extract_fields(record)
           return self.model_class.construct(**fields)
   ```

## Testing Strategy

```python
import pytest
from datetime import date

class TestAMMapper:
    """Test suite for Amateur License mapper"""
    
    @pytest.fixture
    def mapper(self) -> AMMapper:
        """Create test mapper"""
        return AMMapper()
    
    def test_valid_record(self, mapper):
        """Test successful mapping"""
        record = Record(
            line=1,
            fields=["AM", "123456", "W1AW", "A"]
        )
        result = mapper(record)
        assert result.system_id == 123456
        assert result.call_sign == "W1AW"
    
    def test_invalid_date(self, mapper):
        """Test date validation"""
        record = Record(
            line=1,
            fields=["AM", "123456", "", "13/99/2024"]
        )
        with pytest.raises(MapperError) as exc:
            mapper(record)
        assert "Invalid date" in str(exc.value)
```

## Common Pitfalls

1. **Field Access**
   ```python
   # Wrong: Direct index access
   value = record.fields[5]  # IndexError risk
   
   # Correct: Safe access
   value = record.fields[5] if len(record.fields) > 5 else None
   ```

2. **Type Conversion**
   ```python
   # Wrong: Direct conversion
   number = int(value)  # ValueError risk
   
   # Correct: Safe conversion
   number = int(value) if value and value.isdigit() else None
   ```

## Extension Guide

To add support for a new record type:

1. Create model and mapper:
```python
class XXRecord(BaseModel):
    """New record type schema"""
    record_type: Literal["XX"]
    field1: str
    field2: Optional[int]

class XXMapper(BaseMapper[XXRecord]):
    """Mapper for new record type"""
    model_class = XXRecord
    
    def _extract_fields(self, record: Record) -> Dict[str, Any]:
        """Custom field extraction"""
        fields = super()._extract_fields(record)
        # Add custom processing
        return fields
```

2. Register with factory:
```python
# Register new mapper
MapperFactory.register_mapper("XX", XXMapper)

# Use new mapper
mapper = MapperFactory.create_mapper("XX")
result = mapper(record)
```

3. Add tests:
```python
def test_xx_mapper():
    """Test new record type mapping"""
    mapper = XXMapper()
    record = Record(line=1, fields=["XX", "value1", "123"])
    result = mapper(record)
    assert result.field1 == "value1"
    assert result.field2 == 123
```
