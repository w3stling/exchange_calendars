import os
import pytest


@pytest.fixture
def test_tzdata_path():
    assert 'PYTHONTZPATH' in os.environ, "PYTHONTZPATH is not set"
    print(f"PYTHONTZPATH: {os.environ['PYTHONTZPATH']}")
