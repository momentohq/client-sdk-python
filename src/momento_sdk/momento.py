from momento_wire_types.controlclient_pb2 import CreateCacheRequest, DeleteCacheRequest
import momento_wire_types.controlclient_pb2_grpc as control_client
import grpc

from momento_sdk.cache import Cache
from . import authorization_interceptor, momento_endpoint_resolver

class Momento:
    def __init__(self, auth_token, endpoint_override=None):
        endpoints = momento_endpoint_resolver._resolve(auth_token, endpoint_override)
        self._auth_token = auth_token
        self._control_endpoint = endpoints.control_endpoint
        self._cache_endpoint = endpoints.cache_endpoint
        self._secure_channel = grpc.secure_channel(self._control_endpoint, grpc.ssl_channel_credentials())
        intercept_channel = grpc.intercept_channel(self._secure_channel, authorization_interceptor.get_authorization_interceptor(auth_token))
        self._client = control_client.ScsControlStub(intercept_channel)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._secure_channel.close()

    def create_cache(self, cache_name):
        request = CreateCacheRequest()
        request.cache_name = cache_name
        self._client.CreateCache(request)

    def delete_cache(self, cache_name):
        request = DeleteCacheRequest()
        request.cache_name = cache_name
        self._client.DeleteCache(request)

    def get_cache(self, cache_name, ttl_seconds, options=None) :
        # TODO: Do create if exists
        return Cache(self._auth_token, cache_name, self._cache_endpoint, ttl_seconds)

def init(auth_token):
    return Momento(auth_token=auth_token)
