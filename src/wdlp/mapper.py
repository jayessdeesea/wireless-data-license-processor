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
            # Convert field values to appropriate types
            system_id = int(record.fields[1]) if record.fields[1] else None
            region_code = int(record.fields[7]) if record.fields[7] else None
            
            return AMRecord(
                record_type=record.fields[0],
                system_id=system_id,
                uls_file_number=record.fields[2],
                ebf_number=record.fields[3],
                call_sign=record.fields[4],
                operator_class=record.fields[5],
                group_code=record.fields[6],
                region_code=region_code,
                trustee_call_sign=record.fields[8],
                trustee_indicator=record.fields[9],
                physician_certification=record.fields[10],
                ve_signature=record.fields[11],
                systematic_call_sign_change=record.fields[12],
                vanity_call_sign_change=record.fields[13],
                vanity_relationship=record.fields[14],
                previous_call_sign=record.fields[15],
                previous_operator_class=record.fields[16],
                trustee_name=record.fields[17]
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
            system_id = int(record.fields[1]) if record.fields[1] else None
            linked_system_id = int(record.fields[28]) if record.fields[28] else None
            
            # Parse date field
            status_date = None
            if record.fields[26]:
                try:
                    month, day, year = record.fields[26].split('/')
                    status_date = date(int(year), int(month), int(day))
                except ValueError as e:
                    raise ValueError(f"Invalid status_date format: {record.fields[26]}")
            
            return ENRecord(
                record_type=record.fields[0],
                system_id=system_id,
                uls_file_number=record.fields[2],
                ebf_number=record.fields[3],
                call_sign=record.fields[4],
                entity_type=record.fields[5],
                licensee_id=record.fields[6],
                entity_name=record.fields[7],
                first_name=record.fields[8],
                mi=record.fields[9],
                last_name=record.fields[10],
                suffix=record.fields[11],
                phone=record.fields[12],
                fax=record.fields[13],
                email=record.fields[14],
                street_address=record.fields[15],
                city=record.fields[16],
                state=record.fields[17],
                zip_code=record.fields[18],
                po_box=record.fields[19],
                attention_line=record.fields[20],
                sgin=record.fields[21],
                frn=record.fields[22],
                applicant_type_code=record.fields[23],
                applicant_type_code_other=record.fields[24],
                status_code=record.fields[25],
                status_date=status_date,
                license_type=record.fields[27],
                linked_system_id=linked_system_id,
                linked_call_sign=record.fields[29]
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
