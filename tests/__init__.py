import os
import tzdata

# Make tests use the tzdata from the Python module instead of the operating system.
# Set the PYTHONTZPATH environment variable to the tzdata zoneinfo directory
tzdata_path = os.path.join(os.path.dirname(tzdata.__file__), 'zoneinfo')
os.environ['PYTHONTZPATH'] = tzdata_path

print(f"1 PYTHONTZPATH set to: {os.environ['PYTHONTZPATH']}")
print(f"1 tzdata_path: {tzdata_path}")