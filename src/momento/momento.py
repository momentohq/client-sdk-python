import grpc
import momento_wire_types.controlclient_pb2_grpc as control_client

from momento_wire_types.controlclient_pb2 import CreateCacheRequest, ListCachesRequest
from momento_wire_types.controlclient_pb2 import DeleteCacheRequest
from . import _cache_service_errors_converter
from . import errors
from .cache import Cache
from . import _authorization_interceptor
from . import _momento_endpoint_resolver
from .cache_operation_responses import CreateCacheResponse
from .cache_operation_responses import DeleteCacheResponse
from .cache_operation_responses import ListCachesResponse
from . import _momento_logger


class Momento:
    def __init__(self, auth_token, endpoint_override=None):
        """Inits Momento to setup SCS client.

        Args: 
            auth_token: Momento JWT
            endpoint_override: String of optional endpoint ovveride to be used when given an explicit endpoint by the Momneto team
        """
        endpoints = _momento_endpoint_resolver.resolve(auth_token,
                                                       endpoint_override)
        self._auth_token = auth_token
        self._control_endpoint = endpoints.control_endpoint
        self._cache_endpoint = endpoints.cache_endpoint
        self._secure_channel = grpc.secure_channel(
            self._control_endpoint, grpc.ssl_channel_credentials())
        intercept_channel = grpc.intercept_channel(
            self._secure_channel,
            _authorization_interceptor.get_authorization_interceptor(
                auth_token))
        self._client = control_client.ScsControlStub(intercept_channel)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._secure_channel.close()

    def create_cache(self, cache_name):
        """Creates a new cache in your Momento account.

        Args: 
            cache_name: String used to create cache.

        Returns:
            CreateCacheResponse

        Raises:
            Exception to notify either sdk, grpc, or operation error.
        """
        try:
            _momento_logger.debug(f'Creating cache with name: {cache_name}')
            request = CreateCacheRequest()
            request.cache_name = cache_name
            return CreateCacheResponse(self._client.CreateCache(request))
        except Exception as e:
            _momento_logger.debug(f'Failed to create cache: {cache_name} with exception:{e}')
            raise _cache_service_errors_converter.convert(e) from None

    def delete_cache(self, cache_name):
        """Deletes a cache and all of the items within it.

        Args: 
            cache_name: String cache name to delete.

        Returns:
            DeleteCacheResponse

        Raises:
            Exception to notify either sdk, grpc, or operation error.
        """
        try:
            _momento_logger.debug(f'Deleting cache with name: {cache_name}')
            request = DeleteCacheRequest()
            request.cache_name = cache_name
            return DeleteCacheResponse(self._client.DeleteCache(request))
        except Exception as e:
            _momento_logger.debug(f'Failed to delete cache: {cache_name} with exception:{e}')
            raise _cache_service_errors_converter.convert(e) from None

    def get_cache(self, cache_name, ttl_seconds, create_if_absent=False):
        """Gets a MomentoCache to perform gets and sets on.

        Args: 
            cache_name: String cache name
            ttl_seconds: Time to live if object insdie of cache in seconds.
            create_if_absent: Boolean value to decide if cahce should be created if it does not exist.

        Returns:
            Cache

        Raises:
            CacheNotFoundError
        """
        cache = Cache(self._auth_token, cache_name, self._cache_endpoint,
                     ttl_seconds)
        try:
            return cache._connect()
        except errors.CacheNotFoundError as e:
            if (not create_if_absent):
                raise e

        _momento_logger.debug(f'create_if_absent={create_if_absent}')
        self.create_cache(cache_name)
        return cache._connect()

    def list_caches(self, next_token=None):
        """Lists ll caches.

        Args: 
            next_token: Token to continue paginating through the list. It's ised to hnadle large paginated lists.

        Returns:
            ListCachesResponse

        Raises:
            Exception to notify either sdk, grpc, or operation error.
        """
        try:
            list_caches_request = ListCachesRequest()
            list_caches_request.next_token = next_token if next_token is not None else ''
            return ListCachesResponse(self._client.ListCaches(list_caches_request))
        except Exception as e:
            raise _cache_service_errors_converter.convert(e)


def init(auth_token):
    return Momento(auth_token)
