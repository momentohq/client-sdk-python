import logging

from momento import logs

from .auth import CredentialProvider
from .config import Configurations
from .simple_cache_client import SimpleCacheClient
from .simple_cache_client_async import SimpleCacheClientAsync

logging.getLogger("momentosdk").addHandler(logging.NullHandler())
logs.initialize_momento_logging()

__all__ = ["CredentialProvider", "Configurations", "SimpleCacheClient", "SimpleCacheClientAsync"]
