
# Module path initialization - define the query module based on config

import sys
from user_metrics.config import settings
QUERY_MOD_PATH = 'user_metrics/metrics/query/'
sys.path.append(settings.__project_home__ + QUERY_MOD_PATH)

query_mod = __import__(settings.__query_module__)
