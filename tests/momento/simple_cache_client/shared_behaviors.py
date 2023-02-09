from datetime import timedelta

from typing_extensions import Protocol

from momento import SimpleCacheClient
from momento.auth import CredentialProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import CacheResponse
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import TCacheName, TScalarKey
from tests.utils import uuid_str


class TCacheNameValidator(Protocol):
    def __call__(self, cache_name: TCacheName) -> CacheResponse:
        ...


def a_cache_name_validator() -> None:
    def with_non_existent_cache_name_it_throws_not_found(cache_name_validator: TCacheNameValidator) -> None:
        cache_name = uuid_str()
        response = cache_name_validator(cache_name=cache_name)
        if isinstance(response, ErrorResponseMixin):
            assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR
        else:
            assert False

    def with_null_cache_name_it_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = cache_name_validator(cache_name=None)  # type: ignore
        if isinstance(response, ErrorResponseMixin):
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert response.inner_exception.message == "Cache name must be a string"
        else:
            assert False

    def with_empty_cache_name_it_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = cache_name_validator(cache_name="")
        if isinstance(response, ErrorResponseMixin):
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert response.inner_exception.message == "Cache name must not be empty"
        else:
            assert False

    def with_bad_cache_name_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = cache_name_validator(cache_name=1)  # type: ignore
        if isinstance(response, ErrorResponseMixin):
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert response.inner_exception.message == "Cache name must be a string"
        else:
            assert False


class TKeyValidator(Protocol):
    def __call__(self, key: TScalarKey) -> CacheResponse:
        ...


def a_key_validator() -> None:
    def with_null_key_throws_exception(cache_name: str, key_validator: TKeyValidator) -> None:
        response = key_validator(key=None)  # type: ignore
        if isinstance(response, ErrorResponseMixin):
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        else:
            assert False

    def with_bad_key_throws_exception(cache_name: str, key_validator: TKeyValidator) -> None:
        response = key_validator(key=1)  # type: ignore
        if isinstance(response, ErrorResponseMixin):
            assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
            assert response.inner_exception.message == "Unsupported type for key: <class 'int'>"
        else:
            assert False


class TConnectionValidator(Protocol):
    def __call__(self, client: SimpleCacheClient) -> CacheResponse:
        ...


def a_connection_validator() -> None:
    def throws_authentication_exception_for_bad_token(
        bad_token_credential_provider: CredentialProvider,
        configuration: Configuration,
        default_ttl_seconds: timedelta,
        connection_validator: TConnectionValidator,
    ) -> None:
        with SimpleCacheClient(configuration, bad_token_credential_provider, default_ttl_seconds) as client:
            response = connection_validator(client)
            if isinstance(response, ErrorResponseMixin):
                assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR
            else:
                assert False

    def throws_timeout_error_for_short_request_timeout(
        configuration: Configuration,
        credential_provider: CredentialProvider,
        default_ttl_seconds: timedelta,
        connection_validator: TConnectionValidator,
    ) -> None:
        configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
        with SimpleCacheClient(configuration, credential_provider, default_ttl_seconds) as client:
            response = connection_validator(client)
            if isinstance(response, ErrorResponseMixin):
                assert response.error_code == MomentoErrorCode.TIMEOUT_ERROR
            else:
                assert False
