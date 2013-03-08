
# Module path initialization - define the query module based on config

from user_metrics.config import settings
query_mod = __import__(settings.__query_module__)
