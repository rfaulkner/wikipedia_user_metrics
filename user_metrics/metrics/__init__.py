
# Module path initialization - define the query module based on config

from user_metrics.utils import nested_import
from user_metrics.config import settings

query_mod = nested_import(settings.__query_module__)
