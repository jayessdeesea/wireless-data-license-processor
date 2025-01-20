import os
import zipfile
import argparse
from reader.readers import DatToRecordIterator
from schema.untyped_utils import UntypedRecordToTypedRecordMapper
from schema.schemas import AMRecord, ENRecord
from writer.writers import JSONLWriter, ParquetWriter, IonWriter, CSVWriter

SCHEMAS = {
    "AM": AMRecord,
    "EN": ENRecord
}

WRITERS = {
    "jsonl": JSONLWriter,
    "parquet": ParquetWriter,
    "ion": IonWriter,
    "csv": CSVWriter
}

def process_zip(zip_path, output_format, output_dir):
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for entry in zf.namelist():
            if not entry.endswith('.dat'):
                print(f"Skipping non-.dat file: {entry}")
                continue

            schema_key = os.path.splitext(os.path.basename(entry))[0].upper()
            if schema_key not in SCHEMAS:
                print(f"Skipping file with unknown schema: {entry}")
                continue

            schema_class = SCHEMAS[schema_key]
            output_path = os.path.join(output_dir, f"{schema_key}.{output_format}")
            writer_class = WRITERS.get(output_format)

            if not writer_class:
                raise ValueError(f"Unsupported output format: {output_format}")

            with zf.open(entry) as file_stream:
                print(f"Processing {entry} with schema {schema_key}")
                with writer_class(output_path) as writer:
                    iterator = DatToRecordIterator(file_stream)
                    mapper = UntypedRecordToTypedRecordMapper(schema_class)
                    for record in iterator:
                        typed_record = mapper.map(record, list(range(len(record.fields))))
                        writer.write(typed_record.model_dump())

def main():
    parser = argparse.ArgumentParser(description="Process ZIP archives containing .dat files into various formats.")
    parser.add_argument("--input", required=True, help="Path to the ZIP archive.")
    parser.add_argument("--output", required=True, help="Directory to save output files.")
    parser.add_argument("--format", choices=WRITERS.keys(), default="jsonl", help="Output file format.")

    args = parser.parse_args()

    input_path = args.input
    output_dir = args.output
    output_format = args.format

    os.makedirs(output_dir, exist_ok=True)

    if zipfile.is_zipfile(input_path):
        process_zip(input_path, output_format, output_dir)
    else:
        raise ValueError(f"Input must be a ZIP archive: {input_path}")

if __name__ == "__main__":
    main()
