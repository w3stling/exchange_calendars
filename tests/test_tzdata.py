import os
import pytest

def test_tzdata_path():
    assert 'PYTHONTZPATH' in os.environ, "PYTHONTZPATH is not set"
    print(f"PYTHONTZPATH: {os.environ['PYTHONTZPATH']}")
