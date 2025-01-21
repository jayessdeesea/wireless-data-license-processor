# CLI Tool Integration

## Objective

Provide a command-line interface to tie all modules together.

## CLI

    - -h or --help: Display usage instructions.
    - -v or --version: Show the program version.
    - -i or --input: Specify the path to the ZIP archive. Default: l_amat.zip.
    - -o or --output: Specify the output directory. Default: output. Create the directory if it does not exist.
    - -f or --file-format: Specify the output file format. Default: jsonl. Possible values are jsonl, parquet,
      ion, and
      csv

## Business Logic

- Process CLI
- Open ZIP archive
  - Halt with a detailed error message for missing or unreadable ZIP Archive
  - Log the name of the ZIP archive
  - For each ZIP Entry
    - Log and skip if the Zip entry does not end in .dat
    - Use the 
- Log the following:
    * ZIP archive path before opening.
    * Each ZIP entry names and action taken (e.g., reasons for skipping files such as skipping file abcd.txt because it
      does not end in .dat)
* Handle Zip Archive file by
    * Skipping non-.dat file entries
    * Skipping .dat files with unknown schemas
    * Opening the dat stream from the zip entry stream. Use in-memory file-like objects for ZIP archive processing
      instead of temporary files.
    * Deleting output files that match the schema and format of the current run and log when doing so
    * Create a DatToUntypedIterator to transform the .dat input stream into a untyped record iterable
    * Create a UntypedRecordToTypedRecordMapper to transform the untyped record into a typed record
    * Create an appropriate writer (e.g., CSVWriter) with the appropriate path (e.g., AM.csv)
    * Iterate through the untyped records, map them to typed records, and write them to disk
* Print a plain text table with
    * Total schema types processed.
    * Total records validated per schema.
    * Total processing time.

Place the CLI logic in project_root/wdlp/main.py.


* Parse arguments and pass them to the appropriate modules
* Handle errors (e.g., invalid file paths, schema validation errors) by displaying debugging context and halting
  processing

Specific Requirements:

* The .dat file name prefix determines the schema for the file. E.g., AM.dat uses the AM schema

---


Place AbstractWriter and extended classes in wdlp/writer/writers.py

### Create CLI and Main Method


