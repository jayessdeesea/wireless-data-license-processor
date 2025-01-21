# Schema Component

## Overview

The Schema component provides Pydantic models that represent and validate FCC Wireless License Database records. These models are based on the [Public Access Database Definitions](https://www.fcc.gov/sites/default/files/public_access_database_definitions_20240215.pdf) and implement comprehensive validation rules.

## Record Types

### Amateur License Record (AM)

```python
class AMRecord(BaseModel):
    """Amateur License record schema"""
    record_type: Optional[str] = Field(
        None,
        min_length=2,
        max_length=2,
        pattern="^AM$",
        description="Record type identifier, must be 'AM'"
    )
    system_id: Optional[int] = Field(
        None,
        ge=0,
        le=999999999,
        description="Unique system identifier (0-999999999)"
    )
    # ... additional fields
```

Full field list (18 fields):
| Position | Field Name | Type | Constraints | Description |
|----------|------------|------|-------------|-------------|
| 1 | record_type | str | char(2) | Record type [AM] |
| 2 | system_id | int | numeric(9,0) | Unique system identifier |
| 3 | uls_file_number | str | char(14) | ULS file number |
| 4 | ebf_number | str | varchar(30) | EBF number |
| 5 | call_sign | str | char(10) | Call sign |
| 6 | operator_class | str | char(1) | Operator class |
| 7 | group_code | str | char(1) | Group code |
| 8 | region_code | int | tinyint | Region code |
| 9 | trustee_call_sign | str | char(10) | Trustee call sign |
| 10 | trustee_indicator | str | char(1) | Trustee indicator |
| 11 | physician_certification | str | char(1) | Physician certification |
| 12 | ve_signature | str | char(1) | VE signature |
| 13 | systematic_call_sign_change | str | char(1) | Systematic call sign change |
| 14 | vanity_call_sign_change | str | char(1) | Vanity call sign change |
| 15 | vanity_relationship | str | char(12) | Vanity relationship |
| 16 | previous_call_sign | str | char(10) | Previous call sign |
| 17 | previous_operator_class | str | char(1) | Previous operator class |
| 18 | trustee_name | str | varchar(50) | Trustee name |

### Entity Record (EN)

```python
class ENRecord(BaseModel):
    """Entity record schema"""
    record_type: Optional[str] = Field(
        None,
        min_length=2,
        max_length=2,
        pattern="^EN$",
        description="Record type identifier, must be 'EN'"
    )
    system_id: Optional[int] = Field(
        None,
        ge=0,
        le=999999999,
        description="Unique system identifier (0-999999999)"
    )
    # ... additional fields
```

Full field list (30 fields):
| Position | Field Name | Type | Constraints | Description |
|----------|------------|------|-------------|-------------|
| 1 | record_type | str | char(2) | Record type [EN] |
| 2 | system_id | int | numeric(9,0) | Unique system identifier |
| 3 | uls_file_number | str | char(14) | ULS file number |
| 4 | ebf_number | str | varchar(30) | EBF number |
| 5 | call_sign | str | char(10) | Call sign |
| 6 | entity_type | str | char(2) | Entity type |
| 7 | licensee_id | str | char(9) | Licensee identifier |
| 8 | entity_name | str | varchar(200) | Entity name |
| 9 | first_name | str | varchar(20) | First name |
| 10 | mi | str | char(1) | Middle initial |
| 11 | last_name | str | varchar(20) | Last name |
| 12 | suffix | str | char(3) | Name suffix |
| 13 | phone | str | char(10) | Phone number |
| 14 | fax | str | char(10) | Fax number |
| 15 | email | str | varchar(50) | Email address |
| 16 | street_address | str | varchar(60) | Street address |
| 17 | city | str | varchar(20) | City |
| 18 | state | str | char(2) | State |
| 19 | zip_code | str | char(9) | Zip code |
| 20 | po_box | str | varchar(20) | PO Box |
| 21 | attention_line | str | varchar(35) | Attention line |
| 22 | sgin | str | char(3) | Signature identifier |
| 23 | frn | str | char(10) | FCC Registration Number |
| 24 | applicant_type_code | str | char(1) | Applicant type code |
| 25 | applicant_type_code_other | str | char(40) | Other applicant type |
| 26 | status_code | str | char(1) | Status code |
| 27 | status_date | date | mm/dd/yyyy | Status date |
| 28 | license_type | str | char(1) | 3.7 GHz license type |
| 29 | linked_system_id | int | numeric(9,0) | Linked system identifier |
| 30 | linked_call_sign | str | char(10) | Linked call sign |

## Validation Rules

### String Fields
- Fixed-length (char): Exact length required
- Variable-length (varchar): Maximum length enforced
- Pattern matching where applicable (e.g., record types)
- All fields are optional to handle partial records

### Numeric Fields
- Integer ranges enforced
- Type conversion from string input
- Validation of decimal places

### Date Fields
- Parsed from mm/dd/yyyy format
- Converted to native Python date objects
- Range validation where applicable

## Usage Example

```python
try:
    # Create and validate an Amateur License record
    am_record = AMRecord(
        record_type="AM",
        system_id=123456789,
        call_sign="W1AW",
        operator_class="A"
    )
    print(f"Valid record: {am_record.model_dump()}")
except ValidationError as e:
    print(f"Validation failed: {e}")
```

## Extension

To add support for new record types:

1. Create new Pydantic model class
2. Define fields with appropriate types and constraints
3. Add validation rules as needed
4. Update schema documentation

Example:
```python
class NewRecord(BaseModel):
    """New record type schema"""
    record_type: Optional[str] = Field(
        None,
        min_length=2,
        max_length=2,
        pattern="^XX$",
        description="Record type identifier, must be 'XX'"
    )
    # Additional fields...
```
