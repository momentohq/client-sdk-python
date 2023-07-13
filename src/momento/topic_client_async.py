from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import TopicConfiguration
from momento.internal.aio._scs_pubsub_client import _ScsPubsubClient
from momento.responses import TopicPublishResponse, TopicSubscribeResponse


class TopicClientAsync:
    """Asynchronous Topic Client.

    Publish and subscribe methods return a response object unique to each request.
    The response object is resolved to a type-safe object of one of several
    sub-types. See the documentation for each response type for details.

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+ if you're subscribing to a topic::

        response = client_async.subscribe(cache_name, topic_name)
        match response:
            case TopicSubscribe.SubscriptionAsync():
                ...the subscription was successful...
            case TopicSubscribe.Error():
                ...there was an error trying to subscribe...

    or equivalently in earlier versions of python::

        response = client_async.subscribe(cache_name, topic_name)
        if isinstance(response, TopicSubscribe.SubscriptionAsync):
            ...
        elif isinstance(response, TopicSubscribe.Error):
            ...
        else:
            raise Exception("This should never happen")
    """

    def __init__(self, configuration: TopicConfiguration, credential_provider: CredentialProvider):
        """Instantiate a client.

        Args:
            configuration (TopicConfiguration): An object holding configuration settings for communication
                with the server.
            credential_provider: (CredentialProvider): An object holding the auth token and endpoint information.

        Example:
            from momento import CredentialProvider, TopicClientAsync, TopicConfigurations

            configuration = TopicConfigurations.Default.v1()
            credential_provider = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
            client = TopicClientAsync(configuration, credential_provider)
        """
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
        """Publishes a message to a topic.

        Args:
            cache_name (str): Name of the cache to publish to.
            topic_name (str): Name of the topic to publish to.
            value (str|bytes): The value to publish

        Returns:
            TopicPublishResponse
        """
        return await self._pubsub_client.publish(cache_name, topic_name, value)

    async def subscribe(self, cache_name: str, topic_name: str) -> TopicSubscribeResponse:
        """Subscribes to a topic.

        Args:
            cache_name (str): The cache to subscribe to.
            topic_name (str): The topic to subscribe to.

        Returns:
            TopicSubscribeResponse
        """
        return await self._pubsub_client.subscribe(cache_name, topic_name)

    async def close(self) -> None:
        await self._pubsub_client.close()
