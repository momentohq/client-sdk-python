import grpc
from . import errors
from momento_wire_types import cacheclient_pb2 as cache_client_types

__rpc_to_error = {
    grpc.StatusCode.ALREADY_EXISTS: errors.CacheExistsError,
    grpc.StatusCode.INVALID_ARGUMENT: errors.CacheValueError,
    grpc.StatusCode.NOT_FOUND: errors.CacheNotFoundError,
    grpc.StatusCode.PERMISSION_DENIED: errors.PermissionError,
}

# Till the time MR2 stops returning errors in Enums
__ecache_result_to_error = {
    cache_client_types.Bad_Request: errors.CacheValueError,
    cache_client_types.Internal_Server_Error: errors.InternalServerError,
    cache_client_types.Service_Unavailable: errors.InternalServerError,
    cache_client_types.Unauthorized: errors.PermissionError,
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


def convert_ecache_result(ecache_result, message):
    if (ecache_result in __ecache_result_to_error):
        return __ecache_result_to_error[ecache_result](message)
    return errors.InternalServerError(
        'CacheService failed with an internal error')
