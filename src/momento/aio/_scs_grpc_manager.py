import grpc
import pkg_resources

import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.controlclient_pb2_grpc as control_client

from ._add_header_client_interceptor import AddHeaderClientInterceptor


class _ControlGrpcManager:
    """Momento Internal."""
    is_user_agent_sent = False

    def __init__(self, auth_token: str, endpoint: str):
        headers = [
            {"authorization", auth_token}
        ]
        if _ControlGrpcManager.is_user_agent_sent == False:
            version = pkg_resources.get_distribution('momento').version
            headers.append({"agent", f"python:{version}"})
            _ControlGrpcManager.is_user_agent_sent = True
        self._secure_channel = grpc.aio.secure_channel(
            target=endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=[AddHeaderClientInterceptor(headers)],
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> control_client.ScsControlStub:
        return control_client.ScsControlStub(self._secure_channel)


class _DataGrpcManager:
    """Momento Internal."""
    is_user_agent_sent = False

    def __init__(self, auth_token: str, endpoint: str):
        headers = [
            {"authorization", auth_token}
        ]
        if _ControlGrpcManager.is_user_agent_sent == False:
            version = pkg_resources.get_distribution('momento').version
            headers.append({"agent", f"python:{version}"})
            _ControlGrpcManager.is_user_agent_sent = True
        self._secure_channel = grpc.aio.secure_channel(
            target=endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=[AddHeaderClientInterceptor(headers)],
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> cache_client.ScsStub:
        return cache_client.ScsStub(self._secure_channel)
