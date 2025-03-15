import os
import tzdata

# Make tests use the tzdata from the Python module instead of the operating system.
# Set the PYTHONTZPATH environment variable to the tzdata zoneinfo directory
tzdata_path = os.path.join(os.path.dirname(tzdata.__file__), 'zoneinfo')
os.environ['PYTHONTZPATH'] = tzdata_path