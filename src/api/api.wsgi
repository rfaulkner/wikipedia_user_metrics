
PROJECT_PATH = ''

import sys
sys.stdout = sys.stderr     # replace the stdout stream
sys.path.insert(0, PROJECT_PATH)
from src.api import app as application