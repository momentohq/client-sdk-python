from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional

from momento.config.transport.topic_grpc_configuration import TopicGrpcConfiguration
from momento.internal._utilities import _validate_request_timeout


class TopicTransportStrategy(ABC):
    """Configures the network options for communicating with the Momento pubsub service."""

    @abstractmethod
    def get_grpc_configuration(self) -> TopicGrpcConfiguration:
        """Access the gRPC configuration.

        Returns:
            TopicGrpcConfiguration: low-level gRPC settings for the Momento client's communication.
        """
        pass

    @abstractmethod
    def with_grpc_configuration(self, grpc_configuration: TopicGrpcConfiguration) -> TopicTransportStrategy:
        """Copy constructor for overriding the gRPC configuration.

        Args:
            grpc_configuration (TopicGrpcConfiguration): the new gRPC configuration.

        Returns:
            TopicTransportStrategy: a new TopicTransportStrategy with the specified gRPC config.
        """
        pass

    @abstractmethod
    def with_client_timeout(self, client_timeout: timedelta) -> TopicTransportStrategy:
        """Copies the TopicTransportStrategy and updates the copy's client-side timeout.

        Args:
            client_timeout (timedelta): the new client-side timeout.

        Returns:
            TopicTransportStrategy: the new TopicTransportStrategy.
        """
        pass


class StaticTopicGrpcConfiguration(TopicGrpcConfiguration):
    def __init__(
        self,
        deadline: timedelta,
        max_send_message_length: Optional[int] = None,
        max_receive_message_length: Optional[int] = None,
        keepalive_permit_without_calls: Optional[bool] = True,
        keepalive_time: Optional[timedelta] = timedelta(milliseconds=5000),
        keepalive_timeout: Optional[timedelta] = timedelta(milliseconds=1000),
    ):
        self._deadline = deadline
        self._max_send_message_length = max_send_message_length
        self._max_receive_message_length = max_receive_message_length
        self._keepalive_permit_without_calls = keepalive_permit_without_calls
        self._keepalive_time = keepalive_time
        self._keepalive_timeout = keepalive_timeout

    def get_deadline(self) -> timedelta:
        return self._deadline

    def with_deadline(self, deadline: timedelta) -> TopicGrpcConfiguration:
        _validate_request_timeout(deadline)
        return StaticTopicGrpcConfiguration(deadline)

    def get_max_send_message_length(self) -> Optional[int]:
        return self._max_send_message_length

    def get_max_receive_message_length(self) -> Optional[int]:
        return self._max_receive_message_length

    def get_keepalive_permit_without_calls(self) -> Optional[int]:
        return int(self._keepalive_permit_without_calls) if self._keepalive_permit_without_calls is not None else None

    def get_keepalive_time(self) -> Optional[timedelta]:
        return self._keepalive_time

    def get_keepalive_timeout(self) -> Optional[timedelta]:
        return self._keepalive_timeout


class StaticTopicTransportStrategy(TopicTransportStrategy):
    def __init__(self, grpc_configuration: TopicGrpcConfiguration):
        self._grpc_configuration = grpc_configuration

    def get_grpc_configuration(self) -> TopicGrpcConfiguration:
        return self._grpc_configuration

    def with_grpc_configuration(self, grpc_configuration: TopicGrpcConfiguration) -> TopicTransportStrategy:
        return StaticTopicTransportStrategy(grpc_configuration)

    def with_client_timeout(self, client_timeout: timedelta) -> TopicTransportStrategy:
        return self.with_grpc_configuration(self._grpc_configuration.with_deadline(client_timeout))
