import grpc
import momento_wire_types.controlclient_pb2_grpc as control_client

from momento_wire_types.controlclient_pb2 import CreateCacheRequest
from momento_wire_types.controlclient_pb2 import DeleteCacheRequest
from . import _cache_service_errors_converter
from . import errors
from .cache import Cache
from . import _authorization_interceptor
from . import _momento_endpoint_resolver
from .cache_operation_responses import CreateCacheResponse
from .cache_operation_responses import DeleteCacheResponse


class Momento:
    def __init__(self, auth_token, endpoint_override=None):
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
        try:
            request = CreateCacheRequest()
            request.cache_name = cache_name
            return CreateCacheResponse(self._client.CreateCache(request))
        except Exception as e:
            raise _cache_service_errors_converter.convert(e) from None

    def delete_cache(self, cache_name):
        try:
            request = DeleteCacheRequest()
            request.cache_name = cache_name
            return DeleteCacheResponse(self._client.DeleteCache(request))
        except Exception as e:
            raise _cache_service_errors_converter.convert(e) from None

    def get_cache(self, cache_name, ttl_seconds, create_if_absent=False):
        if (create_if_absent):
            try:
                self.create_cache(cache_name)
            except errors.CacheExistsError:
                # Cache already exists so nothing to do
                pass
        return Cache(self._auth_token, cache_name, self._cache_endpoint,
                     ttl_seconds)


def init(auth_token):
    return Momento(auth_token)
