"""Momento client library.

Instantiate a client with `CacheClient` or `CacheClientAsync` (for asyncio).
Use `CredentialProvider` to read credentials for the client.
Use `Configurations` for pre-built network configurations.
"""

import logging

from momento import logs

from .auth import CredentialProvider
from .cache_client import CacheClient
from .cache_client_async import CacheClientAsync
from .config import Configurations

logging.getLogger("momentosdk").addHandler(logging.NullHandler())
logs.initialize_momento_logging()

__all__ = ["CredentialProvider", "Configurations", "CacheClient", "CacheClientAsync"]
