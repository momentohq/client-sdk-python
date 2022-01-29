class SdkError(Exception):
    """Base exception for all errors raised by Sdk"""
    def __init__(self, message):
        super().__init__(message)


class ClientSdkError(SdkError):
    """For all errors raised by the client.

       Indicates that the request failed on the SDK. The request either did not
       make it to the service or if it did the response from the service could
       not be parsed successfully.
    """
    def __init__(self, message):
        super().__init__(message)


class InvalidInputError(ClientSdkError):
    """Error raised when provided input values to the SDK are invalid

       Some examples - missing required parameters, incorrect parameter
       types, malformed input.
    """
    def __init__(self, message):
        super().__init__(message)


class MomentoServiceError(SdkError):
    """Errors raised when Momento Service returned an error code"""
    def __init__(self, message):
        super().__init__(message)


class CacheServiceError(MomentoServiceError):
    """Base class for all errors raised by the Caching Service"""
    def __init__(self, message):
        super().__init__(message)


class CacheNotFoundError(CacheServiceError):
    """Error raised for operations performed on non-existent cache"""
    def __init__(self, message):
        super().__init__(message)


class CacheExistsError(CacheServiceError):
    """Error raised when attempting to create a cache with same name"""
    def __init__(self, message):
        super().__init__(message)


class InvalidArgumentError(CacheServiceError):
    """Error raised when service validation fails for provided values"""
    def __init__(self, message):
        super().__init__(message)


class PermissionError(CacheServiceError):
    """Error when authentication with Cache Service fails"""
    def __init__(self, message):
        super().__init__(message)


class InternalServerError(CacheServiceError):
    """Operation failed on the server with an unknown error"""
    def __init__(self, message):
        super().__init__(message)
