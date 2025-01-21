# .dat stream Pull Parser 

## Objective

Build a Python Pull Parser for the .dat file format.

## .dat file format

```text
dat-file      = record*, EOF ; 
record        = field*, end-of-record ;
field         = field-value, end-of-field ;

field-value   = field-byte* ;
end-of-field  = "|" ;
field-byte    = ? any character except end-of-field ? ;

end-of-record = "\n" | ("\r", "\n") ;
```

## Constraints

### Length Constraints

Field:
    - Minimum length: 0 bytes (empty field).
    - Maximum length: 1024 bytes.

### Size Limit Constraints

Record:
    - Minimum number of fields: 1.
    - Maximum number of fields: 256.

## Additional Requirements

Track the line where the record starts.

## Test Cases

- The input is in the .dat file format
- The expected results is in the JSON file format

### Test 1: Single Record Single Field Zero Length

Input:

```text
|
```

Expected Results:

```json
[
  [
    ""
  ]
]
```

### Test 2: Single Record Single Field 

Input:

```text
value|
```

Expected Results:

```json
[
  [
    "value"
  ]
]
```

### Test 3: Single Record Multiple Fields 

Input:

```text
value1|value2|
```

Expected Results:

```json
[
  [
    "value1",
    "value2"
  ]
]
```

### Test 4: Two Records

Input:

```text
value1|
value2|
```

Expected Results:

```json
[
  [
    "value1"
  ],
  [
    "value2"
  ]
]
```

### Test 5: Field with newline

Input:

```text
a
b|
```

Expected Results:

```json
[
  [
    "a\nb"
  ]
]
```

## Tasks

### Task 1: Record class

Build a Record class
- Accepts 
  - line (type int) the line number of the .dat stream
  - fields (list of strings)
- Methods
  - `__str__(self)`

Record lives in src/producer/record.py

### Task 2: State Transition Table

Build a state transition table
- Base it off the .dat file format EBNF
- error states are terminal.

For each test case
- Mechanically walk through the provided input using the generated optimized state transition table
  - Verify you get the expected result
  - Print pass if you get the expected result, fail otherwise


Print the state transition table

### Task 3: Finite State Machine

Build a Python finite state machine
- Use the state transition table above
- Use the State design pattern
- error is a terminal state and has the following context
  - line number.
  - what was expected (like field-char).
  - what was received.
- EOF (End Of File) when not at the start state is a framing error
 
Context and State objects live in src/producer/fsm.py

### Task 4: Pull Parser

The Pull Parser uses the Finite State Machine to transform a .dat file input stream into a stream of Records

PullParser
- Is an Iterator
- Accepts
  - an input stream representing a .dat file
- Enforce grammar and other constraints (e.g., length, size)
- If constraint check fails
  - the pull parser is in a terminal error state
  - raise an error with
     - line number
     - What was expected (like field-char)
     - and what was received. Use ascii code if the character is unprintable
  - further attempts to pull records from a parser will raise the same error 

PullParser lives in src/producer/pull_parser.py

