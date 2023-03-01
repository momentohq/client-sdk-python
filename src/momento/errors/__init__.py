"""Momento client error handling.

The SDK error base class is `SdkException`, which is a subclass of `Exception`.
Should a command raise an exception, the client will capture it return an `Error` response
containing the exception details.
"""

from .error_details import (
    MomentoErrorCode,
    MomentoErrorTransportDetails,
    MomentoGrpcErrorDetails,
)
from .exceptions import (
    AlreadyExistsException,
    AuthenticationException,
    BadRequestException,
    CancelledException,
    FailedPreconditionException,
    InternalServerException,
    InvalidArgumentException,
    LimitExceededException,
    NotFoundException,
    PermissionDeniedException,
    SdkException,
    ServerUnavailableException,
    TimeoutException,
    UnknownException,
    UnknownServiceException,
)

# NB: since this module imports from sibling modules, it must be at the bottom
# to avoid circular imports
from .error_converter import convert_error

__all__ = [
    "MomentoErrorCode",
    "MomentoErrorTransportDetails",
    "MomentoGrpcErrorDetails",
    "AlreadyExistsException",
    "AuthenticationException",
    "BadRequestException",
    "CancelledException",
    "FailedPreconditionException",
    "InternalServerException",
    "InvalidArgumentException",
    "LimitExceededException",
    "NotFoundException",
    "PermissionDeniedException",
    "SdkException",
    "ServerUnavailableException",
    "TimeoutException",
    "UnknownException",
    "UnknownServiceException",
    "convert_error",
]
