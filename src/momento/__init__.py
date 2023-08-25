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
from .config import Configurations, TopicConfigurations, VectorIndexConfigurations
from .topic_client import TopicClient
from .topic_client_async import TopicClientAsync
from .vector_index_client import PreviewVectorIndexClient
from .vector_index_client_async import PreviewVectorIndexClientAsync

logging.getLogger("momentosdk").addHandler(logging.NullHandler())
logs.initialize_momento_logging()

__all__ = [
    "CredentialProvider",
    "Configurations",
    "TopicConfigurations",
    "VectorIndexConfigurations",
    "CacheClient",
    "CacheClientAsync",
    "TopicClient",
    "TopicClientAsync",
    "PreviewVectorIndexClient",
    "PreviewVectorIndexClientAsync",
]
