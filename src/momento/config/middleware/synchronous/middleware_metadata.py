from dataclasses import dataclass
from typing import Optional

from grpc._typing import MetadataType


@dataclass
class MiddlewareMetadata:
    """Wrapper for gRPC metadata."""

    grpc_metadata: Optional[MetadataType]
