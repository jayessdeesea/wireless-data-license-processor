# Transform Record into Pydantic Data Object

## Overview

Transform Record into Pydantic Data Object

## Tasks

### Task 1: Build Mapper

We are using the factory pattern


There is a Mapper interface
- callable method that accepts a record and returns a Pydantic object

Each of the record types supported (e.g., AM, EN, etc.) have an implementation of Mapper (AMMapper, ENMapper, etc.) 

The concrete implementation will implement the abstract method so that it
- will accept a record
- validate the record field by field
- raise an error if the validation fails with the following message
  - The entire record, including line number
  - What was expected (string of a length, integer, etc.)
  - What was received
- Return the Pydantic object

There is a factory called MapperFactory
- method create_mapper(mapper_type: str) -> Mapper
  - mapper_type is "AM", "EN", etc.
