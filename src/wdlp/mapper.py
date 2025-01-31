from abc import ABC, abstractmethod
from datetime import date
from typing import TypeVar, Type, Optional
from pydantic import BaseModel

from .reader import Record
from .schema import AMRecord, ENRecord

T = TypeVar('T', bound=BaseModel)

class MapperError(Exception):
    """Error during record mapping"""
    def __init__(self, record: Record, expected: str, received: str):
        self.record = record
        self.expected = expected
        self.received = received
        super().__init__(
            f"Mapping error at line {record.line}:\n"
            f"Expected: {expected}\n"
            f"Received: {received}"
        )

class Mapper(ABC):
    """Abstract base class for record mappers"""
    @abstractmethod
    def __call__(self, record: Record) -> T:
        """Transform a record into a Pydantic model"""
        pass

class AMMapper(Mapper):
    """Mapper for Amateur License records"""
    def __call__(self, record: Record) -> AMRecord:
        try:
            # Validate record type first
            if not record.fields or record.fields[0] != "AM":
                raise ValueError("Invalid record_type: must be 'AM'")
                
            # Then validate field count
            if len(record.fields) < 2:  # Need at least record_type and system_id
                raise ValueError("Insufficient number of fields")
            
            # Convert field values to appropriate types
            def get_field(fields: list, index: int, default=None):
                """Safely get field value with default"""
                return fields[index] if index < len(fields) and fields[index] else default
            
            # Parse system_id with specific error handling
            system_id_str = get_field(record.fields, 1)
            if system_id_str:
                try:
                    system_id = int(system_id_str)
                except ValueError:
                    raise ValueError(f"Invalid system_id value: {system_id_str}")
            else:
                system_id = None
            region_code = int(get_field(record.fields, 7)) if get_field(record.fields, 7) else None
            
            return AMRecord(
                record_type=get_field(record.fields, 0),
                system_id=system_id,
                uls_file_number=get_field(record.fields, 2),
                ebf_number=get_field(record.fields, 3),
                call_sign=get_field(record.fields, 4),
                operator_class=get_field(record.fields, 5),
                group_code=get_field(record.fields, 6),
                region_code=region_code,
                trustee_call_sign=get_field(record.fields, 8),
                trustee_indicator=get_field(record.fields, 9),
                physician_certification=get_field(record.fields, 10),
                ve_signature=get_field(record.fields, 11),
                systematic_call_sign_change=get_field(record.fields, 12),
                vanity_call_sign_change=get_field(record.fields, 13),
                vanity_relationship=get_field(record.fields, 14),
                previous_call_sign=get_field(record.fields, 15),
                previous_operator_class=get_field(record.fields, 16),
                trustee_name=get_field(record.fields, 17)
            )
        except (IndexError, ValueError) as e:
            raise MapperError(
                record=record,
                expected="Valid Amateur License record fields",
                received=f"Error: {str(e)}"
            )

class ENMapper(Mapper):
    """Mapper for Entity records"""
    def __call__(self, record: Record) -> ENRecord:
        try:
            # Convert field values to appropriate types
            def get_field(fields: list, index: int, default=None):
                """Safely get field value with default"""
                return fields[index] if index < len(fields) and fields[index] else default
            
            # Parse system_id fields
            system_id = int(get_field(record.fields, 1)) if get_field(record.fields, 1) else None
            linked_system_id = int(get_field(record.fields, 28)) if get_field(record.fields, 28) else None
            
            # Format phone number
            phone = get_field(record.fields, 12)
            if phone and len(phone) == 10:
                phone = f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"
            
            # Parse date field
            status_date = None
            date_str = get_field(record.fields, 26)
            if date_str:
                try:
                    month, day, year = date_str.split('/')
                    status_date = date(int(year), int(month), int(day))
                except ValueError as e:
                    raise ValueError(f"Invalid status_date format: {date_str}")
            
            return ENRecord(
                record_type=get_field(record.fields, 0),
                system_id=system_id,
                uls_file_number=get_field(record.fields, 2),
                ebf_number=get_field(record.fields, 3),
                call_sign=get_field(record.fields, 4),
                entity_type=get_field(record.fields, 5),
                licensee_id=get_field(record.fields, 6),
                entity_name=get_field(record.fields, 7),
                first_name=get_field(record.fields, 8),
                mi=get_field(record.fields, 9),
                last_name=get_field(record.fields, 10),
                suffix=get_field(record.fields, 11),
                phone=phone,
                fax=get_field(record.fields, 13),
                email=get_field(record.fields, 14),
                street_address=get_field(record.fields, 15),
                city=get_field(record.fields, 16),
                state=get_field(record.fields, 17),
                zip_code=get_field(record.fields, 18),
                po_box=get_field(record.fields, 19),
                attention_line=get_field(record.fields, 20),
                sgin=get_field(record.fields, 21),
                frn=get_field(record.fields, 22),
                applicant_type_code=get_field(record.fields, 23),
                applicant_type_code_other=get_field(record.fields, 24),
                status_code=get_field(record.fields, 25),
                status_date=status_date,
                license_type=get_field(record.fields, 27),
                linked_system_id=linked_system_id,
                linked_call_sign=get_field(record.fields, 29)
            )
        except (IndexError, ValueError) as e:
            raise MapperError(
                record=record,
                expected="Valid Entity record fields",
                received=f"Error: {str(e)}"
            )

class MapperFactory:
    """Factory for creating record type-specific mappers"""
    _mappers = {
        'AM': AMMapper,
        'EN': ENMapper
    }

    @classmethod
    def create_mapper(cls, mapper_type: str) -> Optional[Mapper]:
        """Create a mapper for the specified record type"""
        if mapper_type not in cls._mappers:
            return None  # Return None for unknown record types (non-fatal)
        return cls._mappers[mapper_type]()
