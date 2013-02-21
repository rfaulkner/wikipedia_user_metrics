
import sys
sys.stdout = sys.stderr     # replace the stdout stream
from user_metrics.api.run import app as application