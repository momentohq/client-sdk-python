from grpc.aio import Metadata


def make_metadata(cache_name: str) -> Metadata:
    return Metadata(("cache", cache_name))
