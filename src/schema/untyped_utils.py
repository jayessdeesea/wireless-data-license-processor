from typing import List, Type
from pydantic import BaseModel, ValidationError

class UntypedField:
    def __init__(self, value: str):
        self.value = value

    def get_value(self) -> str:
        return self.value

class UntypedRecord:
    def __init__(self, fields: List[UntypedField]):
        self.fields = fields

    def get_field(self, index: int) -> str:
        try:
            return self.fields[index].get_value()
        except IndexError:
            raise ValueError(f"Field index {index} out of range for record with {len(self.fields)} fields.")

class UntypedRecordToTypedRecordMapper:
    def __init__(self, schema_class: Type[BaseModel]):
        self.schema_class = schema_class

    def map(self, record: UntypedRecord, field_mapping: List[int]) -> BaseModel:
        data = {}
        for field_name, index in zip(self.schema_class.model_fields.keys(), field_mapping):
            try:
                data[field_name] = record.get_field(index) if index < len(record.fields) else None
            except ValueError as e:
                raise ValueError(f"Error mapping field '{field_name}': {e}")

        try:
            return self.schema_class.model_validate(data)
        except ValidationError as e:
            raise ValueError(f"Validation error for record: {e}")
