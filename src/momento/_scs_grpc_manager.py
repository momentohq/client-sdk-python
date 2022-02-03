import grpc

import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.controlclient_pb2_grpc as control_client

from . import _authorization_interceptor
from . import _client_timeout_interceptor


_DEFAULT_CONTROL_CLIENT_DEADLINE_SECONDS = 60.0 # 1 minute
_DEFAULT_DATA_CLIENT_DEADLINE_SECONDS = 5.0 # 5 seconds

class _ControlGrpcManager:
    """Momento Internal."""
    def __init__(self, auth_token, endpoint) -> None:
        self._secure_channel = grpc.secure_channel(
            endpoint, grpc.ssl_channel_credentials())
        authorization_interceptor = _authorization_interceptor.get_authorization_interceptor(auth_token)
        timeout_interceptor = _client_timeout_interceptor.get(_DEFAULT_CONTROL_CLIENT_DEADLINE_SECONDS)
        intercept_channel = grpc.intercept_channel(
            self._secure_channel,
            authorization_interceptor,
            timeout_interceptor)
        self._stub = control_client.ScsControlStub(intercept_channel)

    def close(self):
        self._secure_channel.close()

    def stub(self):
        return self._stub


class _DataGrpcManager:
    """Momento Internal."""
    def __init__(self, auth_token, endpoint, request_timeout_ms):
        timeout_seconds =  _DEFAULT_DATA_CLIENT_DEADLINE_SECONDS if not request_timeout_ms else request_timeout_ms/1000.0
        self._secure_channel = grpc.secure_channel(
            endpoint, grpc.ssl_channel_credentials())
        authorization_interceptor = _authorization_interceptor.get_authorization_interceptor(auth_token)
        timeout_interceptor = _client_timeout_interceptor.get(timeout_seconds)
        intercept_channel = grpc.intercept_channel(
            self._secure_channel,
            authorization_interceptor,
            timeout_interceptor)
        self._stub = cache_client.ScsStub(intercept_channel)

    def close(self):
        self._secure_channel.close()

    def stub(self):
        return self._stub
