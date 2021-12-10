from enum import Enum
from momento_wire_types import cacheclient_pb2 as cache_client_types
from . import _cache_service_errors_converter as error_converter


class CacheResult(Enum):
    HIT = 1
    MISS = 2


class CacheSetResponse:
    def __init__(self, grpc_set_response, value):
        self._value = value
        if (grpc_set_response.result is not cache_client_types.Ok):
            raise error_converter.convert_ecache_result(
                grpc_set_response.result, grpc_set_response.message)

    def str_utf8(self):
        return self._value.decode('utf-8')

    def bytes(self):
        return self._value


class CacheGetResponse:
    def __init__(self, grpc_get_response):
        self._value = grpc_get_response.cache_body

        if (grpc_get_response.result is not cache_client_types.Hit
                and grpc_get_response.result is not cache_client_types.Miss):
            raise error_converter.convert_ecache_result(
                grpc_get_response.result, grpc_get_response.message)

        if (grpc_get_response.result == cache_client_types.Hit):
            self._result = CacheResult.HIT
        if (grpc_get_response.result == cache_client_types.Miss):
            self._result = CacheResult.MISS

    def str_utf8(self):
        if (self._result == CacheResult.HIT):
            return self._value.decode('utf-8')
        return None

    def bytes(self):
        if (self._result == CacheResult.HIT):
            return self._value
        return None

    def result(self):
        return self._result


class CreateCacheResponse:
    def __init__(self, grpc_create_cache_response):
        pass


class DeleteCacheResponse:
    def __init__(self, grpc_delete_cache_response):
        pass


class ListCachesResponse:
    def __init__(self, grpc_list_cache_response):
        self._next_token = grpc_list_cache_response.next_token
        self._caches = []
        for cache in grpc_list_cache_response.cache:
            self._caches.append(CacheInfo(cache))

    def next_token(self):
        return self._next_token

    def caches(self):
        return self._caches


class CacheInfo:
    def __init__(self, grpc_listed_caches):
        self._name = grpc_listed_caches.cache_name

    def name(self):
        return self._name
