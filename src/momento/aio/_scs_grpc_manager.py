from typing import List

import grpc
import pkg_resources

import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.controlclient_pb2_grpc as control_client
from grpc import experimental

from ._add_header_client_interceptor import AddHeaderClientInterceptor
from ._add_header_client_interceptor import Header
from ._retry_interceptor import get_retry_interceptor_if_enabled


class _ControlGrpcManager:
    """Momento Internal."""

    version = pkg_resources.get_distribution("momento").version

    def __init__(self, auth_token: str, endpoint: str):
        self._secure_channel = grpc.aio.secure_channel(
            target=endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=_interceptors(auth_token),
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> control_client.ScsControlStub:
        return control_client.ScsControlStub(self._secure_channel)


class _DataGrpcManager:
    """Momento Internal."""

    version = pkg_resources.get_distribution("momento").version

    def __init__(self, auth_token: str, endpoint: str):
        self._secure_channel = grpc.aio.secure_channel(
            target=endpoint,
            credentials=grpc.ssl_channel_credentials(),
            interceptors=_interceptors(auth_token),
            # Here is where you would pass override configuration to the underlying C gRPC layer.
            # However, I have tried several different tuning options here and did not see any
            # performance improvements, so sticking with the defaults for now.
            # For more info on the performance investigations:
            # https://github.com/momentohq/client-sdk-python/issues/120
            # For more info on available gRPC config options:
            # https://grpc.github.io/grpc/python/grpc.html
            # https://grpc.github.io/grpc/python/glossary.html#term-channel_arguments
            # https://github.com/grpc/grpc/blob/v1.46.x/include/grpc/impl/codegen/grpc_types.h#L140
            options=[
                # ('grpc.max_concurrent_streams', 1000),
                # ('grpc.use_local_subchannel_pool', 1),
                # (experimental.ChannelOptions.SingleThreadedUnaryStream, 1)
            ],
        )

    async def close(self) -> None:
        await self._secure_channel.close()

    def async_stub(self) -> cache_client.ScsStub:
        return cache_client.ScsStub(self._secure_channel)


def _interceptors(auth_token: str) -> List[grpc.aio.ClientInterceptor]:
    headers = [
        Header("authorization", auth_token),
        Header("agent", f"python:{_ControlGrpcManager.version}"),
    ]
    return [
        AddHeaderClientInterceptor(headers),
        *get_retry_interceptor_if_enabled(),
    ]
