import grpc
from . import errors
from . import _momento_logger

__rpc_to_error = {
    grpc.StatusCode.ALREADY_EXISTS: errors.CacheExistsError,
    grpc.StatusCode.INVALID_ARGUMENT: errors.CacheValueError,
    grpc.StatusCode.NOT_FOUND: errors.CacheNotFoundError,
    grpc.StatusCode.PERMISSION_DENIED: errors.PermissionError,
}


def convert(exception):
    if (isinstance(exception, errors.SdkError)):
        return exception

    if (isinstance(exception, grpc.RpcError)):
        if exception.code() in __rpc_to_error:
            return __rpc_to_error[exception.code()](exception.details())
        else:
            return errors.InternalServerError(
                'CacheService failed with an internal error')

    return errors.ClientSdkError('Operation failed with error: ' +
                                 str(exception))


def convert_ecache_result(ecache_result, message, operation_name):
    _momento_logger.debug(
        f'Converting ECacheResult: {ecache_result} to error.')
    return errors.InternalServerError(
        f'CacheService returned an unexpected result: {ecache_result}' +
        f' for operation: {operation_name} with message: {message}')
