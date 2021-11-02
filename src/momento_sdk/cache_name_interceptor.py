from . import header_client_interceptor


def get_cache_name_interceptor(cache_name):
    return header_client_interceptor.header_adder_interceptor(
        'cache', cache_name)
