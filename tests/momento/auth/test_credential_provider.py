import os

import jwt
import pytest

from momento.auth.credential_provider import CredentialProvider
from tests.utils import uuid_str

test_email = "user@test.com"
test_control_endpoint = "control.test.com"
test_cache_endpoint = "cache.test.com"
test_message = {"sub": test_email, "cp": test_control_endpoint, "c": test_cache_endpoint}
test_token = jwt.encode(test_message, "secret", algorithm="HS512")
test_env_var_name = uuid_str()
os.environ[test_env_var_name] = test_token


@pytest.mark.parametrize(
    "provider, auth_token, control_endpoint, cache_endpoint",
    [
        (CredentialProvider.from_string(auth_token=test_token), test_token, test_control_endpoint, test_cache_endpoint),
        (
            CredentialProvider.from_string(
                auth_token=test_token,
                control_endpoint="give.me.control.test.com",
                cache_endpoint="secret.cache.test.com",
            ),
            test_token,
            "give.me.control.test.com",
            "secret.cache.test.com",
        ),
        (
            CredentialProvider.from_environment_variable(env_var_name=test_env_var_name),
            test_token,
            test_control_endpoint,
            test_cache_endpoint,
        ),
        (
            CredentialProvider.from_environment_variable(
                env_var_name=test_env_var_name,
                control_endpoint="give.me.control.test.com",
                cache_endpoint="secret.cache.test.com",
            ),
            test_token,
            "give.me.control.test.com",
            "secret.cache.test.com",
        ),
    ],
)
def test_endpoints(provider, auth_token, control_endpoint, cache_endpoint) -> None:
    assert provider.auth_token == auth_token
    assert provider.control_endpoint == control_endpoint
    assert provider.cache_endpoint == cache_endpoint


def test_env_token_raises_if_not_exists() -> None:
    with pytest.raises(RuntimeError, match=r"Missing required environment variable"):
        CredentialProvider.from_environment_variable(env_var_name=uuid_str())
