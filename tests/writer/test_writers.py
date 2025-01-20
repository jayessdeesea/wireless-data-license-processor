import os
import pytest
import pandas as pd
import pyarrow.parquet as pq
from amazon.ion.simpleion import loads as ion_loads
from wdlp.writer.writers import JSONLWriter, ParquetWriter, IonWriter, CSVWriter

def test_jsonl_writer(tmp_path):
    output_path = os.path.join(tmp_path, "output.jsonl")
    data = [{"field1": "value1", "field2": "value2"}, {"field1": "value3", "field2": "value4"}]

    with JSONLWriter(output_path) as writer:
        for record in data:
            writer.write(record)

    with open(output_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    assert len(lines) == 2
    for line, record in zip(lines, data):
        assert record == json.loads(line.strip())

def test_parquet_writer(tmp_path):
    output_path = os.path.join(tmp_path, "output.parquet")
    data = [{"field1": "value1", "field2": "value2"}, {"field1": "value3", "field2": "value4"}]

    with ParquetWriter(output_path) as writer:
        for record in data:
            writer.write(record)

    table = pq.read_table(output_path)
    df = table.to_pandas()

    assert len(df) == 2
    assert df.to_dict(orient='records') == data

def test_ion_writer(tmp_path):
    output_path = os.path.join(tmp_path, "output.ion")
    data = [{"field1": "value1", "field2": "value2"}, {"field1": "value3", "field2": "value4"}]

    with IonWriter(output_path) as writer:
        for record in data:
            writer.write(record)

    with open(output_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    assert len(lines) == 2
    for line, record in zip(lines, data):
        assert record == ion_loads(line.strip())

def test_csv_writer(tmp_path):
    output_path = os.path.join(tmp_path, "output.csv")
    data = [{"field1": "value1", "field2": "value2"}, {"field1": "value3", "field2": "value4"}]

    with CSVWriter(output_path) as writer:
        for record in data:
            writer.write(record)

    df = pd.read_csv(output_path)

    assert len(df) == 2
    assert df.to_dict(orient='records') == data
