from enum import Enum
from momento_wire_types import cacheclient_pb2 as cache_client_types
from . import _cache_service_errors_converter as error_converter
from . import _momento_logger


class CacheResult(Enum):
    HIT = 1
    MISS = 2


class CacheSetResponse:
    def __init__(self, grpc_set_response, value):
        """Initializes CacheSetResponse to handle gRPC set response.

        Args:
            grpc_set_response: Protobuf based response returned by Scs.
            value (string or bytes): The value to be used to store item in the cache

        Raises:
            InternalServerError: If server encountered an unknown error while trying to store the item.
        """
        self._value = value

    def str_utf8(self):
        """Decodes string value set in cache to a utf-8 string."""
        return self._value.decode('utf-8')

    def bytes(self):
        """Returns byte value set in cache."""
        return self._value


class CacheGetResponse:
    def __init__(self, grpc_get_response):
        """Initializes CacheGetResponse to handle gRPC get response.

        Args:
            grpc_get_response: Protobuf based response returned by Scs.

        Raises:
            InternalServerError: If server encountered an unknown error while trying to retrieve the item.
        """
        self._value = grpc_get_response.cache_body

        if (grpc_get_response.result == cache_client_types.Hit):
            self._result = CacheResult.HIT
        elif (grpc_get_response.result == cache_client_types.Miss):
            self._result = CacheResult.MISS
        else:
            _momento_logger.debug(
                f'Get received unsupported ECacheResult: {grpc_get_response.result}'
            )
            raise error_converter.convert_ecache_result(
                grpc_get_response.result, grpc_get_response.message, 'GET')

    def str_utf8(self):
        """Returns value stored in cache as utf-8 string if there was Hit. Returns None otherwise."""
        if (self._result == CacheResult.HIT):
            return self._value.decode('utf-8')
        return None

    def bytes(self):
        """Returns value stored in cache as bytes if there was Hit. Returns None otherwise."""
        if (self._result == CacheResult.HIT):
            return self._value
        return None

    def result(self):
        """Returns get operation result such as HIT or MISS."""
        return self._result


class CreateCacheResponse:
    def __init__(self, grpc_create_cache_response):
        pass


class DeleteCacheResponse:
    def __init__(self, grpc_delete_cache_response):
        pass


class ListCachesResponse:
    def __init__(self, grpc_list_cache_response):
        """Initializes ListCacheResponse to handle list cache response.

        Args:
            grpc_list_cache_response: Protobuf based response returned by Scs.
        """
        self._next_token = grpc_list_cache_response.next_token if grpc_list_cache_response.next_token != '' else None
        self._caches = []
        for cache in grpc_list_cache_response.cache:
            self._caches.append(CacheInfo(cache))

    def next_token(self):
        """Returns next token."""
        return self._next_token

    def caches(self):
        """Returns all caches."""
        return self._caches


class CacheInfo:
    def __init__(self, grpc_listed_cache):
        """Initializes CacheInfo to handle caches returned from list cache operation.

        Args:
            grpc_listed_cache: Protobuf based response returned by Scs.
        """
        self._name = grpc_listed_cache.cache_name

    def name(self):
        """Returns all cache's name."""
        return self._name
