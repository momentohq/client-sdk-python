from typing import Optional

from grpc._typing import MetadataType


class MiddlewareMetadata:
    """Wrapper for gRPC metadata."""

    def __init__(self, metadata: Optional[MetadataType]):
        self.grpc_metadata = metadata

    def get_grpc_metadata(self) -> Optional[MetadataType]:
        """Get the underlying gRPC metadata."""
        return self.grpc_metadata
