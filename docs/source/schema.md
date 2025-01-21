# Generate


## AM Record Schema

* Use the table immediately following this for the AM Record Schema
* If the table does not exist, extract the schema from the provided `Public Access Database Definitions`

| Position | Data Element                | Definition                        | Data Type    |
|----------|-----------------------------|-----------------------------------|--------------|
| 1        | Record Type [AM]            | Record type                       | char(2)      |
| 2        | Unique System Identifier    | Unique identifier for the system  | numeric(9,0) |
| 3        | ULS File Number             | ULS file number                   | char(14)     |
| 4        | EBF Number                  | EBF number                        | varchar(30)  |
| 5        | Call Sign                   | Call sign                         | char(10)     |
| 6        | Operator Class              | Operator class                    | char(1)      |
| 7        | Group Code                  | Group code                        | char(1)      |
| 8        | Region Code                 | Region code                       | tinyint      |
| 9        | Trustee Call Sign           | Trustee call sign                 | char(10)     |
| 10       | Trustee Indicator           | Trustee indicator                 | char(1)      |
| 11       | Physician Certification     | Physician certification           | char(1)      |
| 12       | VE Signature                | Volunteer Examiner (VE) signature | char(1)      |
| 13       | Systematic Call Sign Change | Systematic call sign change       | char(1)      |
| 14       | Vanity Call Sign Change     | Vanity call sign change           | char(1)      |
| 15       | Vanity Relationship         | Vanity relationship               | char(12)     |
| 16       | Previous Call Sign          | Previous call sign                | char(10)     |
| 17       | Previous Operator Class     | Previous operator class           | char(1)      |
| 18       | Trustee Name                | Name of trustee                   | varchar(50)  |

## EN Record Schema

* Use the table immediately following this for the EN Record Schema
* If the table does not exist, extract the schema from the provided `Public Access Database Definitions`

| Position | Data Element                    | Definition                       | Data Type    |
|----------|---------------------------------|----------------------------------|--------------|
| 1        | Record Type [EN]                | Record type                      | char(2)      |
| 2        | Unique System Identifier        | Unique identifier for the system | numeric(9,0) |
| 3        | ULS File Number                 | ULS file number                  | char(14)     |
| 4        | EBF Number                      | EBF number                       | varchar(30)  |
| 5        | Call Sign                       | Call sign                        | char(10)     |
| 6        | Entity Type                     | Type of entity                   | char(2)      |
| 7        | Licensee ID                     | Licensee identifier              | char(9)      |
| 8        | Entity Name                     | Name of the entity               | varchar(200) |
| 9        | First Name                      | First name of individual         | varchar(20)  |
| 10       | MI                              | Middle initial                   | char(1)      |
| 11       | Last Name                       | Last name                        | varchar(20)  |
| 12       | Suffix                          | Name suffix                      | char(3)      |
| 13       | Phone                           | Phone number                     | char(10)     |
| 14       | Fax                             | Fax number                       | char(10)     |
| 15       | Email                           | Email address                    | varchar(50)  |
| 16       | Street Address                  | Street address                   | varchar(60)  |
| 17       | City                            | City                             | varchar(20)  |
| 18       | State                           | State                            | char(2)      |
| 19       | Zip Code                        | Zip code                         | char(9)      |
| 20       | PO Box                          | PO Box                           | varchar(20)  |
| 21       | Attention Line                  | Attention line                   | varchar(35)  |
| 22       | SGIN                            | Signature identifier             | char(3)      |
| 23       | FCC Registration Number (FRN)   | FCC registration number          | char(10)     |
| 24       | Applicant Type Code             | Applicant type code              | char(1)      |
| 25       | Applicant Type Code Other       | Other applicant type code        | char(40)     |
| 26       | Status Code                     | Status code                      | char(1)      |
| 27       | Status Date                     | Status date                      | mm/dd/yyyy   |
| 28       | 3.7 GHz License Type            | 3.7 GHz license type             | char(1)      |
| 29       | Linked Unique System Identifier | Linked unique system identifier  | numeric(9,0) |
| 30       | Linked Call Sign                | Linked call sign                 | char(10)     |

## Tasks

### Task 1. Generate Pydantic data object for provided schemas 

Build Pydantic data objects using the provided Public Access Database Definitions

- validate constraints (e.g., string length, numeric ranges, and regular expressions)
- Each Pydantic data object
    - Defines attribute name and type
    - Makes attributes `Optional`
    - Uses validation constraints (`min_length`, `max_length`, `pattern`, `description`) where possible
    - Makes `description` validation constraint value comprehensive for context and clarity
    - Uses `pattern` validation constraint instead of the deprecated `regex` validation constraints
    - Uses native Python dates for date types rather than strings
- Define the schema classes directly instead of encapsulating them in functions.
    - On-the-fly generation or isolation of the schema definitions is not required

Place the schema record types in src/schema/schemas.py

