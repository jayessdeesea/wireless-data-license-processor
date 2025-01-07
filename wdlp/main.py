import os
import zipfile
import json
import argparse
import tempfile
import pyarrow.parquet as pq
import pandas as pd
from io import TextIOWrapper
from pathlib import Path

from pydantic import ValidationError

from schemas import ENSchema, FASchema
from typing import Optional, List

def log_error(context: str, filename: str, line_number: int, expected: str, found: str):
    print(f"Error in file '{filename}', line {line_number}: {context}. Expected: {expected}, Found: {found}")

def process_zip_stream(input_zip: str, output_path: str, output_type: str, schema_mapping: dict):
    with zipfile.ZipFile(input_zip, 'r') as zip_ref:
        for entry in zip_ref.infolist():
            if not entry.filename.endswith(".dat"):
                continue

            record_type = Path(entry.filename).stem
            if record_type not in schema_mapping:
                print(f"Skipping unsupported file: {entry.filename} (No matching schema for record type '{record_type}').")
                continue

            schema_cls = schema_mapping[record_type]
            temp_file = Path(output_path) / f"{record_type}.tmp"
            output_file = Path(output_path) / f"{record_type}.{output_type.lower()}"

            print(f"Processing file: {entry.filename}")

            try:
                with zip_ref.open(entry, 'r') as infile, open(temp_file, 'w') as outfile:
                    line_number = 0
                    current_record = []
                    for line in TextIOWrapper(infile):
                        line_number += 1
                        if line.strip():
                            current_record.append(line.rstrip())

                            if line.strip().endswith('|'):
                                raw_record = schema_cls.combine_multiline(current_record)
                                fields = raw_record.split('|')

                                fields = [field.strip() if field else None for field in fields]
                                try:
                                    if not fields[0]:
                                        log_error("Missing required field 'Record Type'", entry.filename, line_number, "Record Type", str(fields[0]))
                                        return  # Halt processing

                                    if fields[0] != record_type:
                                        log_error("Mismatched record type", entry.filename, line_number, record_type, fields[0])
                                        return  # Halt processing

                                    data = schema_cls(**{
                                        "Record Type": fields[0],
                                        "Unique System Identifier": int(fields[1]) if fields[1] and fields[1].isdigit() else None,
                                        "ULS File Number": fields[2],
                                        "EBF Number": fields[3],
                                        "Call Sign": fields[4],
                                        "Description": fields[5],
                                        "Status Date": schema_cls.validate_date(fields[6], line_number)
                                    })
                                    outfile.write(json.dumps(data.dict(by_alias=True)) + "\n")
                                except (ValidationError, ValueError) as e:
                                    log_error("Invalid row format", entry.filename, line_number, "Valid data", raw_record)
                                    return  # Halt processing
                                finally:
                                    current_record = []

                if output_type.lower() == "jsonl":
                    os.rename(temp_file, output_file)
                elif output_type.lower() == "parquet":
                    with open(temp_file, 'r') as jsonl_file:
                        records = [json.loads(line) for line in jsonl_file]
                        df = pd.DataFrame(records)
                        df.to_parquet(output_file, engine='pyarrow', index=False)
                    os.remove(temp_file)

            except Exception as e:
                log_error("File processing failed", entry.filename, line_number, "Successful processing", str(e))
                return

def main():
    parser = argparse.ArgumentParser(description="Process .dat files from a ZIP archive and transform them into JSONL or Parquet format.")
    parser.add_argument("-i", "--input", default="l_amat.zip", help="Path to the input ZIP archive. Default is 'l_amat.zip'.")
    parser.add_argument("-o", "--output", default="wdlp-output", help="Path to the output directory. Default is 'wdlp-output/'.")
    parser.add_argument("-t", "--output-type", default="JSONL", choices=["JSONL", "parquet"], help="Output format: JSONL or parquet. Default is JSONL.")
    parser.add_argument("-v", "--version", action="version", version="dat_to_jsonl_processor 2.0", help="Show program version and exit.")

    args = parser.parse_args()

    input_zip = args.input
    output_path = args.output
    output_type = args.output_type

    os.makedirs(output_path, exist_ok=True)

    schema_mapping = {
        "EN": ENSchema,
        "FA": FASchema,
    }

    process_zip_stream(input_zip, output_path, output_type, schema_mapping)

if __name__ == "__main__":
    main()
