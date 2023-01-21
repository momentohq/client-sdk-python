import os
from datetime import timedelta

import pytest

from momento import SimpleCacheClientAsync
from momento.auth.credential_provider import CredentialProvider, EnvMomentoTokenProvider
from momento.config.configuration import Configuration
from momento.errors import InvalidArgumentException


async def test_init_throws_exception_when_client_uses_negative_default_ttl(
    configuration: Configuration, credential_provider: CredentialProvider
) -> None:
    with pytest.raises(InvalidArgumentException, match="TTL timedelta must be a non-negative integer"):
        SimpleCacheClientAsync(configuration, credential_provider, timedelta(seconds=-1))


async def test_init_throws_exception_for_non_jwt_token(
    configuration: Configuration, default_ttl_seconds: timedelta
) -> None:
    with pytest.raises(InvalidArgumentException, match="Invalid Auth token."):
        os.environ["BAD_AUTH_TOKEN"] = "notanauthtoken"
        credential_provider = EnvMomentoTokenProvider("BAD_AUTH_TOKEN")
        SimpleCacheClientAsync(configuration, credential_provider, default_ttl_seconds)


async def test_init_throws_exception_when_client_uses_integer_request_timeout_ms(
    configuration: Configuration, credential_provider: CredentialProvider, default_ttl_seconds: int
) -> None:
    with pytest.raises(
        InvalidArgumentException, match="Request timeout must be a timedelta with a value greater " "than zero."
    ):
        configuration.with_client_timeout(-1)


async def test_init_throws_exception_when_client_uses_negative_request_timeout_ms(
    configuration: Configuration, credential_provider: CredentialProvider, default_ttl_seconds: timedelta
) -> None:
    with pytest.raises(
        InvalidArgumentException, match="Request timeout must be a timedelta with a value greater than zero."
    ):
        configuration = configuration.with_client_timeout(timedelta(seconds=-1))
        SimpleCacheClientAsync(configuration, credential_provider, default_ttl_seconds)


async def test_init_throws_exception_when_client_uses_zero_request_timeout_ms(
    configuration: Configuration, credential_provider: CredentialProvider, default_ttl_seconds: timedelta
) -> None:
    with pytest.raises(
        InvalidArgumentException, match="Request timeout must be a timedelta with a value greater than zero."
    ):
        configuration = configuration.with_client_timeout(timedelta(seconds=0))
        SimpleCacheClientAsync(configuration, credential_provider, default_ttl_seconds)
