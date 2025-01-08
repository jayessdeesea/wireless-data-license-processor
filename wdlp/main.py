import zipfile
import os
import json
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict
from schemas import ENSchema, AMSchema
from pydantic import ValidationError

# Constants
SUPPORTED_SCHEMAS = {
    "EN": ENSchema,
    "AM": AMSchema
}
DEFAULT_INPUT_FILE = "l_amat.zip"
DEFAULT_OUTPUT_DIR = "wdlp-output"
DEFAULT_OUTPUT_FORMAT = "JSONL"


def process_dat_file(file_name: str, input_stream, output_dir: str, output_format: str):
    logging.info(f"Starting processing for file: {file_name}")
    """
    Process a single .dat file, validate its records, and write them directly to the output file.

    :param file_name: Name of the .dat file.
    :param input_stream: Input stream of the .dat file.
    :param output_dir: Directory to save the output file.
    :param output_format: Format of the output (JSONL, PARQUET, ION).
    """
    schema_name = file_name.split('.')[0].upper()
    if schema_name not in SUPPORTED_SCHEMAS:
        logging.info(f"Skipping file with unknown schema: {file_name}")
        return

    schema = SUPPORTED_SCHEMAS[schema_name]
    output_path = os.path.join(output_dir, f"{schema_name}.{output_format.lower()}")

    if output_format.upper() == "JSONL":
        with open(output_path, 'w', encoding='utf-8') as output_stream:
            for line in input_stream:
                try:
                    record = parse_line(line.decode('utf-8').strip(), schema)
                    output_stream.write(json.dumps(record) + '\n')
                except ValidationError as e:
                    raise ValueError(f"Schema validation error in file {file_name}: {e}")
    elif output_format.upper() == "PARQUET":
        records = []
        for line in input_stream:
            try:
                record = parse_line(line.decode('utf-8').strip(), schema)
                records.append(record)
            except ValidationError as e:
                raise ValueError(f"Schema validation error in file {file_name}: {e}")
        table = pa.Table.from_pylist(records)
        pq.write_table(table, output_path)
    elif output_format.upper() == "ION":
        try:
            import amazon.ion.simpleion as ion
            with open(output_path, 'w', encoding='utf-8') as output_stream:
                for line in input_stream:
                    try:
                        record = parse_line(line.decode('utf-8').strip(), schema)
                        ion_text = ion.dumps(record, binary=False)
                        output_stream.write(ion_text + '\n')
                    except ValidationError as e:
                        raise ValueError(f"Schema validation error in file {file_name}: {e}")
        except ImportError:
            raise NotImplementedError(
                "ION support requires the amazon.ion library. Install it with 'pip install amazon.ion'")
    else:
        raise NotImplementedError(f"Output format {output_format} is not supported yet.")


def parse_line(line: str, schema):
    """
    Parse and validate a single line from a .dat file.

    :param line: The line content.
    :param schema: The schema to validate against.
    :return: Validated record as a dictionary.
    """
    fields = [field if field != '' else None for field in line.split('|')[:-1]]
    return schema(**dict(zip(schema.model_fields.keys(), fields))).model_dump()


def extract_zip(zip_path: str, output_dir: str, output_format: str):
    """
    Extract and process .dat files from a ZIP archive.

    :param zip_path: Path to the ZIP archive.
    :param output_dir: Directory to save the output files.
    :param output_format: Format of the output (JSONL, PARQUET, ION).
    """
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Invalid ZIP file: {zip_path}")

    os.makedirs(output_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if not file_name.endswith(".dat"):
                logging.info(f"Skipping unsupported file: {file_name}")
                continue

            with zip_ref.open(file_name) as input_stream:
                process_dat_file(file_name, input_stream, output_dir, output_format)


def print_summary(total_files: int, total_skipped_files: int):
    """
    Print a summary of the processing.

    :param total_files: Total number of files processed.
    :param total_skipped_files: Total number of skipped files.
    """
    logging.info("\n--- Processing Summary ---")
    logging.info(f"Total files processed: {total_files}")
    logging.info(f"Total skipped files: {total_skipped_files}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process and validate .dat files from a ZIP archive.")
    parser.add_argument("-i", "--input", default=DEFAULT_INPUT_FILE, help="Path to the ZIP archive.")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT_DIR, help="Output directory.")
    parser.add_argument("-t", "--output-file-format", default=DEFAULT_OUTPUT_FORMAT,
                        help="Output file format (e.g., JSONL, PARQUET, ION).")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    try:
        logging.info(f"Starting processing for {args.input}")
        total_files = 0
        skipped_files = 0

        with zipfile.ZipFile(args.input, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if not file_name.endswith(".dat"):
                    logging.info(f"Skipping unsupported file: {file_name}")
                    skipped_files += 1
                    continue

                total_files += 1
                with zip_ref.open(file_name) as input_stream:
                    process_dat_file(file_name, input_stream, args.output, args.output_file_format)

        print_summary(total_files, skipped_files)
        logging.info("Processing completed successfully.")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
