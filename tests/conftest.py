import os
import pytest
import tempfile
import zipfile
from pathlib import Path

@pytest.fixture
def test_data_dir():
    """Return path to test data directory"""
    return Path(os.environ.get('TEST_DATA_DIR', '/data/test_data'))

@pytest.fixture
def temp_dir():
    """Provide a temporary directory that's cleaned up after the test"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

@pytest.fixture
def valid_am_data(test_data_dir):
    """Return path to valid AM test data file"""
    return test_data_dir / 'valid_am.dat'

@pytest.fixture
def valid_en_data(test_data_dir):
    """Return path to valid EN test data file"""
    return test_data_dir / 'valid_en.dat'

@pytest.fixture
def test_zip(temp_dir, valid_am_data, valid_en_data):
    """Create a test zip file containing sample data files"""
    zip_path = temp_dir / 'test.zip'
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.write(valid_am_data, 'AM.dat')
        zf.write(valid_en_data, 'EN.dat')
    return zip_path

@pytest.fixture
def output_dir(temp_dir):
    """Create and return path to output directory"""
    output = temp_dir / 'output'
    output.mkdir(exist_ok=True)
    return output
