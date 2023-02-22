import re

from momento.auth import CredentialProvider


def describe_credential_provider() -> None:
    def it_obscures_the_auth_token(bad_token_credential_provider: CredentialProvider) -> None:
        cp = bad_token_credential_provider
        assert not re.search(r"{cp.auth_token}", cp.__repr__())
        assert not re.search(r"{cp.auth_token}", str(cp))
