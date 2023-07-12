from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from momento import logs
from momento.auth import CredentialProvider
from momento.config import TopicConfiguration
from momento.internal.synchronous._scs_pubsub_client import _ScsPubsubClient
from momento.responses import TopicPublishResponse, TopicSubscribeResponse


class TopicClient:
    """Synchronous Topic Client.

    Publish and subscribe methods return a response object unique to each request.
    The response object is resolved to a type-safe object of one of several
    sub-types. See the documentation for each response type for details.

    Pattern matching can be used to operate on the appropriate subtype.
    For example, in python 3.10+ if you're subscribing to a topic::

        response = client.subscribe(cache_name, topic_name)
        match response:
            case TopicSubscribe.Subscription():
                ...the subscription was successful...
            case TopicSubscribe.Error():
                ...there was an error trying to subscribe...

    or equivalently in earlier versions of python::

        response = client.subscribe(cache_name, topic_name)
        if isinstance(response, TopicSubscribe.Subscription):
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
            from momento import CredentialProvider, TopicClient, TopicConfigurations

            configuration = TopicConfigurations.Default.v1()
            credential_provider = CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN")
            client = TopicClient(configuration, credential_provider)
        """
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
        """Publishes a message to a topic.

        Args:
            cache_name (str): Name of the cache to publish to.
            topic_name (str): Name of the topic to publish to.
            value (str|bytes): The value to publish

        Returns:
            TopicPublishResponse
        """
        return self._pubsub_client.publish(cache_name, topic_name, value)

    def subscribe(self, cache_name: str, topic_name: str) -> TopicSubscribeResponse:
        """Subscribes to a topic.

        Args:
            cache_name (str): The cache to subscribe to.
            topic_name (str): The topic to subscribe to.

        Returns:
            TopicSubscribeResponse
        """
        return self._pubsub_client.subscribe(cache_name, topic_name)

    def close(self) -> None:
        self._pubsub_client.close()
