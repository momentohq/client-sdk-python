import enum
from dataclasses import dataclass
from typing import Optional

import grpc


@enum.unique
class MomentoErrorCode(enum.Enum):
    """A list of all available Momento error codes.

    These can be used to check for specific types of errors on a failure response.
    """

    INVALID_ARGUMENT_ERROR = (1,)
    """Invalid argument passed to Momento client"""
    UNKNOWN_SERVICE_ERROR = (2,)
    """Service returned an unknown response"""
    ALREADY_EXISTS_ERROR = (3,)
    """Cache with specified name already exists"""
    NOT_FOUND_ERROR = (4,)
    """Cache with specified name doesn't exist"""
    INTERNAL_SERVER_ERROR = (5,)
    """An unexpected error occurred while trying to fulfill the request"""
    PERMISSION_ERROR = (6,)
    """Insufficient permissions to perform operation"""
    AUTHENTICATION_ERROR = (7,)
    """Invalid authentication credentials to connect to cache service"""
    CANCELLED_ERROR = (8,)
    """Request was cancelled by the server"""
    LIMIT_EXCEEDED_ERROR = (9,)
    """Request rate exceeded the limits for the account"""
    BAD_REQUEST_ERROR = (10,)
    """Request was invalid"""
    TIMEOUT_ERROR = (11,)
    """Client's configured timeout was exceeded"""
    SERVER_UNAVAILABLE = (12,)
    """Server was unable to handle the request"""
    CLIENT_RESOURCE_EXHAUSTED = (13,)
    """A client resource most likely memory was exhausted"""
    FAILED_PRECONDITION_ERROR = (14,)
    """System is not in a state required for the operation's execution"""
    UNKNOWN_ERROR = 15
    """Unknown error has occurred"""


@dataclass
class MomentoGrpcErrorDetails:
    """Captures low-level information about an error, at the gRPC level.

    Hopefully this is only needed in rare cases, by Momento engineers, for debugging.
    """

    code: grpc.StatusCode
    """The gRPC status code of the error repsonse"""
    details: str
    """Detailed information about the error"""
    metadata: Optional[grpc.aio.Metadata] = None
    """Headers and other information about the error response"""
    # TODO need to reconcile synchronous metadata (above) with grpc.aio.Metadata


@dataclass
class MomentoErrorTransportDetails:
    """Container for low-level error information, including details from the transport layer."""

    grpc: MomentoGrpcErrorDetails
    """Low-level gRPC error details"""
