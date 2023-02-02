from datetime import timedelta
from typing import Callable

from momento import SimpleCacheClient
from momento.auth import EnvMomentoTokenProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import CacheResponse
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import TCacheName, TScalarKey
from tests.utils import uuid_str

TCacheNameValidator = Callable[[TCacheName], CacheResponse]


def a_cache_name_validator() -> None:
    def with_non_existent_cache_name_it_throws_not_found(cache_name_validator: TCacheNameValidator) -> None:
        cache_name = uuid_str()
        response = cache_name_validator(cache_name)
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR

    def with_null_cache_name_it_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = cache_name_validator(None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must be a string"

    def with_empty_cache_name_it_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = cache_name_validator("")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must not be empty"

    def with_bad_cache_name_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = cache_name_validator(1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must be a string"


TKeyValidator = Callable[[str, TScalarKey], CacheResponse]


def a_key_validator() -> None:
    def with_null_key_throws_exception(cache_name: str, key_validator: TKeyValidator) -> None:
        response = key_validator(cache_name, None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    def with_bad_key_throws_exception(cache_name: str, key_validator: TKeyValidator) -> None:
        response = key_validator(cache_name, 1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Unsupported type for key: <class 'int'>"


TConnectionValidator = Callable[[SimpleCacheClient, str], CacheResponse]


def a_connection_validator() -> None:
    def throws_authentication_exception_for_bad_token(
        bad_token_credential_provider: EnvMomentoTokenProvider,
        configuration: Configuration,
        cache_name: str,
        default_ttl_seconds: timedelta,
        connection_validator: TConnectionValidator,
    ) -> None:
        with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
            response = connection_validator(client, cache_name)
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR

    def throws_timeout_error_for_short_request_timeout(
        configuration: Configuration,
        credential_provider: EnvMomentoTokenProvider,
        cache_name: str,
        default_ttl_seconds: timedelta,
        connection_validator: TConnectionValidator,
    ) -> None:
        configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
        with SimpleCacheClient(configuration, credential_provider, default_ttl_seconds) as client:
            response = connection_validator(client, cache_name)
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.TIMEOUT_ERROR
