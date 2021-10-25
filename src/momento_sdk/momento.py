import momento_wire_types.controlclient_pb2 as control_client

class Momento:
    def __init__(self, auth_token, endpoint_override=None):
        self._auth_token__ = auth_token
        self._endpoint_override = endpoint_override

    def create_cache(self, cache_name):
        print("hello")


def init(auth_token):
    return Momento(auth_token=auth_token)
