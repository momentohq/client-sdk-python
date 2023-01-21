from typing import List

import grpc
import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.controlclient_pb2_grpc as control_client
import pkg_resources

from momento.auth import CredentialProvider
from momento.internal.synchronous._add_header_client_interceptor import (
    AddHeaderClientInterceptor,
    Header,
)
from momento.internal.synchronous._retry_interceptor import (
    get_retry_interceptor_if_enabled,
)


class _ControlGrpcManager:
    """Momento Internal."""

    version = pkg_resources.get_distribution("momento").version

    def __init__(self, credential_provider: CredentialProvider):
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.get_control_endpoint(), credentials=grpc.ssl_channel_credentials()
        )
        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_interceptors(credential_provider.get_auth_token())
        )
        self._stub = control_client.ScsControlStub(intercept_channel)

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> control_client.ScsControlStub:
        return self._stub


class _DataGrpcManager:
    """Momento Internal."""

    version = pkg_resources.get_distribution("momento").version

    def __init__(self, credential_provider: CredentialProvider):
        self._secure_channel = grpc.secure_channel(
            target=credential_provider.get_cache_endpoint(),
            credentials=grpc.ssl_channel_credentials(),
        )
        intercept_channel = grpc.intercept_channel(
            self._secure_channel, *_interceptors(credential_provider.get_auth_token())
        )
        self._stub = cache_client.ScsStub(intercept_channel)

    def close(self) -> None:
        self._secure_channel.close()

    def stub(self) -> cache_client.ScsStub:
        return self._stub


def _interceptors(auth_token: str) -> List[grpc.UnaryUnaryClientInterceptor]:
    headers = [Header("authorization", auth_token), Header("agent", f"python:{_ControlGrpcManager.version}")]
    return [AddHeaderClientInterceptor(headers), *get_retry_interceptor_if_enabled()]
