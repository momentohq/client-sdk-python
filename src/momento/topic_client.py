from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import TopicConfiguration
from momento.internal.synchronous._scs_pubsub_client import _ScsPubsubClient
from momento.responses import TopicPublishResponse, TopicSubscribeResponse


class TopicClient:
    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        self._logger = logs.logger
        self._cache_endpoint = credential_provider.cache_endpoint
        self._pubsub_client = _ScsPubsubClient(configuration, credential_provider)

    def __enter__(self) -> TopicClient:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._pubsub_client.close()

    def publish(self, cache_name: str, topic_name: str, value: str | bytes) -> TopicPublishResponse:
        return self._pubsub_client.publish(cache_name, topic_name, value)

    def subscribe(self, cache_name: str, topic_name: str) -> TopicSubscribeResponse:
        return self._pubsub_client.subscribe(cache_name, topic_name)
