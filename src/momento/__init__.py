"""Momento client library.

Instantiate a client with `CacheClient` or `CacheClientAsync` (for asyncio).
Use `CredentialProvider` to read credentials for the client.
Use `Configurations` for pre-built network configurations.
"""

import logging

from momento import logs

from .auth import CredentialProvider
from .auth_client import AuthClient
from .auth_client_async import AuthClientAsync
from .cache_client import CacheClient
from .cache_client_async import CacheClientAsync
from .config import Configurations, TopicConfigurations
from .topic_client import TopicClient
from .topic_client_async import TopicClientAsync

__version__ = "1.26.0"  # x-release-please-version

logging.getLogger("momentosdk").addHandler(logging.NullHandler())
logs.initialize_momento_logging()

__all__ = [
    "CredentialProvider",
    "Configurations",
    "TopicConfigurations",
    "CacheClient",
    "CacheClientAsync",
    "TopicClient",
    "TopicClientAsync",
    "AuthClient",
    "AuthClientAsync",
    "__version__",
]
