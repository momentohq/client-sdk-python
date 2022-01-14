import grpc

import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.controlclient_pb2_grpc as control_client

from . import _authorization_interceptor


class _ControlGrpcManager:
    """Momento Internal."""
    def __init__(self, auth_token, endpoint) -> None:
        self._secure_channel = grpc.secure_channel(
            endpoint, grpc.ssl_channel_credentials())
        intercept_channel = grpc.intercept_channel(
            self._secure_channel,
            _authorization_interceptor.get_authorization_interceptor(
                auth_token))
        self._stub = control_client.ScsControlStub(intercept_channel)

    def close(self):
        self._secure_channel.close()

    def stub(self):
        return self._stub


class _DataGrpcManager:
    """Momento Internal."""
    def __init__(self, auth_token, endpoint):
        self._secure_channel = grpc.secure_channel(
            endpoint, grpc.ssl_channel_credentials())
        intercept_channel = grpc.intercept_channel(
            self._secure_channel,
            _authorization_interceptor.get_authorization_interceptor(
                auth_token))
        self._stub = cache_client.ScsStub(intercept_channel)

    def close(self):
        self._secure_channel.close()

    def stub(self):
        return self._stub
