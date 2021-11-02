import grpc
from . import authorization_interceptor
from . import cache_name_interceptor
import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.cacheclient_pb2 as cache_client_types


class Cache:
    def __init__(self, auth_token, cache_name, endpoint, default_ttlSeconds):
        self._default_ttlSeconds = default_ttlSeconds
        self._secure_channel = grpc.secure_channel(
            endpoint, grpc.ssl_channel_credentials())
        auth_interceptor = authorization_interceptor.get_authorization_interceptor(
            auth_token)
        cache_interceptor = cache_name_interceptor.get_cache_name_interceptor(
            cache_name)
        intercept_channel = grpc.intercept_channel(self._secure_channel,
                                                   auth_interceptor,
                                                   cache_interceptor)
        self._client = cache_client.ScsStub(intercept_channel)
        # self._wait_till_ready()

    # Temporary hack
    # TODO: Make this time bound
    def _wait_till_ready(self):
        while (True):
            try:
                self._client.Get('b\0x01')
                break
            except:
                True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._secure_channel.close()

    def set(self, key, value, ttl_seconds=None):
        item_ttl_seconds = self._default_ttlSeconds if ttl_seconds is None else ttl_seconds
        set_request = cache_client_types.SetRequest()
        set_request.cache_key = self._asBytes(key,
                                              "Unsupported type for key: ")
        set_request.cache_body = self._asBytes(value,
                                               "Unsupported type for value: ")
        set_request.ttl_milliseconds = item_ttl_seconds * 1000
        self._client.Set(set_request)

    def get(self, key):
        get_request = cache_client_types.GetRequest()
        get_request.cache_key = self._asBytes(key,
                                              "Unsupported type for key: ")
        response = self._client.Get(get_request)
        print(response)

    def _asBytes(self, data, errorMessage):
        if (isinstance(data, str)):
            return data.encode('utf-8')
        if (isinstance(data, bytes)):
            return data
        # TODO: Add conversions
        raise TypeError(errorMessage + str(type(data)))
