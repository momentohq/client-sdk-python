from __future__ import annotations

import grpc

from momento.config.transport.grpc_configuration import GrpcConfiguration
from momento.internal._utilities import _timedelta_to_ms

DEFAULT_MAX_MESSAGE_SIZE = 5_243_000  # bytes


def grpc_channel_options_from_grpc_config(grpc_config: GrpcConfiguration) -> grpc.aio.ChannelArgumentType:
    """Create gRPC channel options from a GrpcConfiguration.

    Args:
        grpc_config (GrpcConfiguration): the gRPC configuration.

    Returns:
        grpc.aio.ChannelArgumentType: a list of gRPC channel options as key-value tuples.
    """
    channel_options = []

    max_send_length = grpc_config.get_max_send_message_length()
    channel_options.append(
        ("grpc.max_send_message_length", max_send_length if max_send_length is not None else DEFAULT_MAX_MESSAGE_SIZE)
    )

    max_receive_length = grpc_config.get_max_receive_message_length()
    channel_options.append(
        (
            "grpc.max_receive_message_length",
            max_receive_length if max_receive_length is not None else DEFAULT_MAX_MESSAGE_SIZE,
        )
    )

    keepalive_permit = grpc_config.get_keepalive_permit_without_calls()
    if keepalive_permit is not None:
        channel_options.append(("grpc.keepalive_permit_without_calls", keepalive_permit))

    keepalive_time = grpc_config.get_keepalive_time()
    if keepalive_time is not None:
        channel_options.append(("grpc.keepalive_time_ms", _timedelta_to_ms(keepalive_time)))

    keepalive_timeout = grpc_config.get_keepalive_timeout()
    if keepalive_timeout is not None:
        channel_options.append(("grpc.keepalive_timeout_ms", _timedelta_to_ms(keepalive_timeout)))

    return channel_options
