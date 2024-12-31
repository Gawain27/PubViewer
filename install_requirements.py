import subprocess
import sys
import os

# Path to the requirements.txt file
requirements_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')

# Check if the file exists
if os.path.exists(requirements_file):
    # Run the pip command to install the dependencies
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', '--break-system-packages', '-r', requirements_file])
else:
    print("requirements.txt not found in the current directory.")
