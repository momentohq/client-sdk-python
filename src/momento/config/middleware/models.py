from dataclasses import dataclass
from typing import Dict

import grpc
from google.protobuf.message import Message

CONNECTION_ID_KEY = "connectionID"


@dataclass
class MiddlewareMessage:
    """Wrapper for a gRPC protobuf message."""

    grpc_message: Message

    @property
    def message_length(self) -> int:
        """Length of the message in bytes."""
        return len(self.grpc_message.SerializeToString())

    @property
    def constructor_name(self) -> str:
        """The class name of the message."""
        return str(self.grpc_message.__class__.__name__)


@dataclass
class MiddlewareStatus:
    """Wrapper for gRPC status."""

    grpc_status: grpc.StatusCode


@dataclass
class MiddlewareRequestHandlerContext:
    """Context for middleware request handlers."""

    context: Dict[str, str]
