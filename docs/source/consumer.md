# Writer Module

## Objective

## 

Consumer
- Method
  - accept()


ConsumerFactory
- Method
  - create(output: Path) -> Consumer

ConsumerFactoryProvider
- Method
  - create(file_format: str) -> ConsumerFactory



# Writer Module

* Support the following file formats
    * JSONL: Each row is serialized as a single line of JSON
    * Parquet: Rows are sorted in an efficient, columnar format
    * Amazon Ion (text format only): Each row is serialized as a single line of Ion text
    * Comma Separated Value (CSV): Each row represents a record, columns are separated by a comma
* Handle missing fields as:
    * null for formats supporting nulls (JSONL, Ion)
    * Empty string ("") for formats not supporting nulls (Parquet, CSV)
* Dates are formatted as:
    * ISO 8601 (yyyy-mm-dd) for formats without native date support.
    * Native date types for formats that support them (e.g., Parquet).

Implementation

* Implement an AbstractWriter
    * Accepts a path to the final file destination.
    * Is a context manager. Will manage the lifetime of a temporary file in the same directory as the final file
      destination
    * A write method for writing a single record to the temporary file
    * on exit
        * Will close the temporary file
        * If processing was successful, move temporary file to final location. Otherwise delete it.
* Extend AbstractWriter for each format (JSONLWriter, ParquetWriter, etc.)
* Write a separate output file for each table type:
    * Example: For EN.dat in JSONL format, write to EN.jsonl.
* The IonWriter should emit Ion text, not Ion binary
* Remember to import the tempfile module as necessary
