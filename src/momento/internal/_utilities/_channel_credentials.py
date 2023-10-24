"""Helper functions for creating gRPC channel credentials.

Note that this isn't in the _utilities init to avoid a circular import.
"""
from __future__ import annotations

import grpc

from momento.config import Configuration, VectorIndexConfiguration


def channel_credentials_from_root_certs_or_default(
    config: Configuration | VectorIndexConfiguration,
) -> grpc.ChannelCredentials:
    """Create gRPC channel credentials from the root certificates or the default credentials.

    Args:
        config (Configuration): the configuration to use.

    Returns:
        grpc.ChannelCredentials: the gRPC channel credentials.
    """
    root_certificates = config.get_transport_strategy().get_grpc_configuration().get_root_certificates_pem()
    return grpc.ssl_channel_credentials(root_certificates=root_certificates)  # type: ignore[misc]
