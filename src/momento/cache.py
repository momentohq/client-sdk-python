import grpc
import time
import uuid
import momento_wire_types.cacheclient_pb2_grpc as cache_client
import momento_wire_types.cacheclient_pb2 as cache_client_types

from . import _cache_service_errors_converter
from . import _authorization_interceptor
from . import _cache_name_interceptor
from . import errors
from . import cache_operation_responses as cache_sdk_resp


class Cache:
    def __init__(self, auth_token, cache_name, endpoint, default_ttlSeconds):
        self._validate_ttl(default_ttlSeconds)
        self._default_ttlSeconds = default_ttlSeconds
        self._secure_channel = grpc.secure_channel(
            endpoint, grpc.ssl_channel_credentials())
        auth_interceptor = _authorization_interceptor.get_authorization_interceptor(
            auth_token)
        cache_interceptor = _cache_name_interceptor.get_cache_name_interceptor(
            cache_name)
        intercept_channel = grpc.intercept_channel(self._secure_channel,
                                                   auth_interceptor,
                                                   cache_interceptor)
        self._client = cache_client.ScsStub(intercept_channel)

    def _connect(self) :
        try:
            self.get(uuid.uuid1().bytes)
            return self
        except Exception as e:
            raise _cache_service_errors_converter.convert(e) from None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._secure_channel.close()

    def set(self, key, value, ttl_seconds=None):
        try:
            item_ttl_seconds = self._default_ttlSeconds if ttl_seconds is None else ttl_seconds
            self._validate_ttl(item_ttl_seconds)
            set_request = cache_client_types.SetRequest()
            set_request.cache_key = self._asBytes(
                key, 'Unsupported type for key: ')
            set_request.cache_body = self._asBytes(
                value, 'Unsupported type for value: ')
            set_request.ttl_milliseconds = item_ttl_seconds * 1000
            response = self._client.Set(set_request)
            return cache_sdk_resp.CacheSetResponse(response,
                                                   set_request.cache_body)
        except Exception as e:
            raise _cache_service_errors_converter.convert(e)

    def get(self, key):
        try:
            get_request = cache_client_types.GetRequest()
            get_request.cache_key = self._asBytes(
                key, 'Unsupported type for key: ')
            response = self._client.Get(get_request)
            return cache_sdk_resp.CacheGetResponse(response)
        except Exception as e:
            raise _cache_service_errors_converter.convert(e)

    def _asBytes(self, data, errorMessage):
        if (isinstance(data, str)):
            return data.encode('utf-8')
        if (isinstance(data, bytes)):
            return data
        raise errors.InvalidInputError(errorMessage + str(type(data)))

    def _validate_ttl(self, ttl_seconds):
        if (not isinstance(ttl_seconds, int) or ttl_seconds <= 0):
            raise errors.InvalidInputError(
                'TTL Seconds must be a non-zero positive integer.')
