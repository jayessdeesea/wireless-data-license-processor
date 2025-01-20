import os
import tempfile
import json
from typing import Any, List
from abc import ABC, abstractmethod
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from amazon.ion.simpleion import dump as ion_dump

class AbstractWriter(ABC):
    def __init__(self, output_path: str):
        self.output_path = output_path
        self.temp_file = None
        self.success = False

    def __enter__(self):
        temp_dir = os.path.dirname(self.output_path)
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, dir=temp_dir)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.temp_file:
            self.temp_file.close()
            if self.success and not exc_type:
                os.replace(self.temp_file.name, self.output_path)
            else:
                os.remove(self.temp_file.name)

    @abstractmethod
    def write(self, record: Any):
        pass

class JSONLWriter(AbstractWriter):
    def __enter__(self):
        super().__enter__()
        self.temp_file = open(self.temp_file.name, 'w', encoding='utf-8')
        return self

    def write(self, record: Any):
        json.dump(record, self.temp_file)
        self.temp_file.write("\n")

class ParquetWriter(AbstractWriter):
    def __init__(self, output_path: str):
        super().__init__(output_path)
        self.records = []

    def write(self, record: Any):
        self.records.append(record)

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            df = pd.DataFrame(self.records)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, self.temp_file.name)
        self.success = True
        super().__exit__(exc_type, exc_value, traceback)

class IonWriter(AbstractWriter):
    def __enter__(self):
        super().__enter__()
        self.temp_file = open(self.temp_file.name, 'w', encoding='utf-8')
        return self

    def write(self, record: Any):
        ion_dump(record, self.temp_file, single_line=True)
        self.temp_file.write("\n")

class CSVWriter(AbstractWriter):
    def __init__(self, output_path: str):
        super().__init__(output_path)
        self.records = []

    def write(self, record: Any):
        self.records.append(record)

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            df = pd.DataFrame(self.records)
            df.to_csv(self.temp_file.name, index=False)
        self.success = True
        super().__exit__(exc_type, exc_value, traceback)
