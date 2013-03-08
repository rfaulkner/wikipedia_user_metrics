
# Module path initialization - define the query module based on config

from user_metrics.config import settings
QUERY_MOD_PATH = 'user_metrics.query.'
query_mod = __import__(QUERY_MOD_PATH + settings.__query_module__)
