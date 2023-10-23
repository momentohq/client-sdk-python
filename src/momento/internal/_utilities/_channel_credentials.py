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
    root_certificates_pem = config.get_transport_strategy().get_grpc_configuration().get_root_certificates_pem()
    if root_certificates_pem is None:
        return grpc.ssl_channel_credentials()  # type: ignore[misc]
    else:
        return grpc.ssl_channel_credentials(root_certificates_pem)  # type: ignore[misc]
