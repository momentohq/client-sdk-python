from typing import Dict

import grpc
from google.protobuf.message import Message

CONNECTION_ID_KEY = "connectionID"


class MiddlewareMessage:
    """Wrapper for a gRPC protobuf message."""

    def __init__(self, message: Message):
        self.grpc_message = message

    def get_message_length(self) -> int:
        """Get the length of the message in bytes."""
        return len(self.grpc_message.SerializeToString())

    def get_constructor_name(self) -> str:
        """Get the class name of the message."""
        return str(self.grpc_message.__class__.__name__)

    def get_message(self) -> Message:
        """Get the underlying gRPC message."""
        return self.grpc_message


class MiddlewareStatus:
    """Wrapper for gRPC status."""

    def __init__(self, status: grpc.StatusCode):
        self.grpc_status = status

    def get_code(self) -> grpc.StatusCode:
        """Get the status code."""
        return self.grpc_status


class MiddlewareRequestHandlerContext:
    """Context for middleware request handlers."""

    def __init__(self, context: Dict[str, str]):
        self.context = context

    def get_context(self) -> Dict[str, str]:
        return self.context
