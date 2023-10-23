"""Helper functions for creating gRPC channel credentials.

Note that this isn't in the _utilities init to avoid a circular import.
"""

import grpc

from momento.config import VectorIndexConfiguration


def vector_credentials_from_root_certs_or_default(config: VectorIndexConfiguration) -> grpc.ChannelCredentials:
    """Create gRPC channel credentials from the root certificates or the default credentials.

    Args:
        config (VectorIndexConfiguration): the configuration to use.

    Returns:
        grpc.ChannelCredentials: the gRPC channel credentials.
    """
    root_certificates = config.get_transport_strategy().get_grpc_configuration().get_root_certificates_pem()
    return grpc.ssl_channel_credentials(root_certificates=root_certificates)  # type: ignore[misc]
