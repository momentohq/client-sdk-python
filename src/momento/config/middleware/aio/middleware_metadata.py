from typing import Optional

from grpc.aio import Metadata


class MiddlewareMetadata:
    """Wrapper for gRPC metadata."""

    def __init__(self, metadata: Optional[Metadata]):
        self.grpc_metadata = metadata

    def get_grpc_metadata(self) -> Optional[Metadata]:
        """Get the underlying gRPC metadata."""
        return self.grpc_metadata
