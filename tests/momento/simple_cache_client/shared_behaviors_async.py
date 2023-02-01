from datetime import timedelta
from typing import Awaitable, Callable, Union

from momento import SimpleCacheClientAsync
from momento.auth import EnvMomentoTokenProvider
from momento.config import Configuration
from momento.errors import MomentoErrorCode
from momento.responses import CacheResponse
from momento.responses.mixins import ErrorResponseMixin
from momento.typing import TScalarKey
from tests.utils import uuid_str

TCacheNameValidator = Callable[[str], Awaitable[CacheResponse]]


def a_cache_name_validator() -> None:
    async def with_non_existent_cache_name_it_throws_not_found(cache_name_validator: TCacheNameValidator) -> None:
        cache_name = uuid_str()
        response = await cache_name_validator(cache_name)
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.NOT_FOUND_ERROR

    async def with_null_cache_name_it_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = await cache_name_validator(None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must be a non-empty string"

    async def with_empty_cache_name_it_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = await cache_name_validator("")
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache header is empty"

    async def with_bad_cache_name_throws_exception(cache_name_validator: TCacheNameValidator) -> None:
        response = await cache_name_validator(1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Cache name must be a non-empty string"


TKeyValidator = Callable[[str, TScalarKey], Awaitable[CacheResponse]]


def a_key_validator() -> None:
    async def with_null_key_throws_exception(cache_name: str, key_validator: TKeyValidator) -> None:
        response = await key_validator(cache_name, None)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR

    async def with_bad_key_throws_exception(cache_name: str, key_validator: TKeyValidator) -> None:
        response = await key_validator(cache_name, 1)  # type: ignore
        assert isinstance(response, ErrorResponseMixin)
        assert response.error_code == MomentoErrorCode.INVALID_ARGUMENT_ERROR
        assert response.inner_exception.message == "Unsupported type for key: <class 'int'>"


TConnectionValidator = Callable[[SimpleCacheClientAsync, str], Awaitable[CacheResponse]]


def a_connection_validator() -> None:
    async def throws_authentication_exception_for_bad_token(
        bad_token_credential_provider: EnvMomentoTokenProvider,
        configuration: Configuration,
        cache_name: str,
        default_ttl_seconds: timedelta,
        connection_validator: TConnectionValidator,
    ) -> None:
        async with SimpleCacheClientAsync(
            configuration, bad_token_credential_provider, default_ttl_seconds
        ) as client_async:
            response = await connection_validator(client_async, cache_name)
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.AUTHENTICATION_ERROR

    async def throws_timeout_error_for_short_request_timeout(
        configuration: Configuration,
        credential_provider: EnvMomentoTokenProvider,
        cache_name: str,
        default_ttl_seconds: timedelta,
        connection_validator: TConnectionValidator,
    ) -> None:
        configuration = configuration.with_client_timeout(timedelta(milliseconds=1))
        async with SimpleCacheClientAsync(configuration, credential_provider, default_ttl_seconds) as client_async:
            response = await connection_validator(client_async, cache_name)
            assert isinstance(response, ErrorResponseMixin)
            assert response.error_code == MomentoErrorCode.TIMEOUT_ERROR
