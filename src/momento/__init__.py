import logging

from .simple_cache_client import SimpleCacheClient
from .simple_cache_client_async import SimpleCacheClientAsync

logging.getLogger("momentosdk").addHandler(logging.NullHandler())
