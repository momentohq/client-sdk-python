from dataclasses import dataclass
from typing import Optional

from grpc.aio import Metadata


@dataclass
class MiddlewareMetadata:
    """Wrapper for gRPC metadata."""

    grpc_metadata: Optional[Metadata]
