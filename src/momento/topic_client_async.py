from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import TopicConfiguration
from momento.internal.aio._scs_pubsub_client import _ScsPubsubClient
from momento.responses import TopicPublishResponse, TopicSubscribeResponse


class TopicClientAsync:
    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        self._logger = logs.logger
        self._cache_endpoint = credential_provider.cache_endpoint
        self._pubsub_client = _ScsPubsubClient(configuration, credential_provider)

    async def __aenter__(self) -> TopicClientAsync:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self._pubsub_client.close()

    async def publish(self, cache_name: str, topic_name: str, value: str | bytes) -> TopicPublishResponse:
        return await self._pubsub_client.publish(cache_name, topic_name, value)

    async def subscribe(self, cache_name: str, topic_name: str) -> TopicSubscribeResponse:
        return await self._pubsub_client.subscribe(cache_name, topic_name)
