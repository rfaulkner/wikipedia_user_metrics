
import sys
sys.stdout = sys.stderr     # replace the stdout stream
from src.api.run import app as application