import os
import random
from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

BAD_AUTH_TOKEN: str = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy"

def do_work():
    os.environ["BAD_AUTH_TOKEN"] = BAD_AUTH_TOKEN
    print("Creating client")
    bad_client = CacheClient.create(Configurations.Laptop.latest(), CredentialProvider.from_environment_variable("BAD_AUTH_TOKEN"),
                       timedelta(seconds=60))
    print(bad_client.list_caches())
    clients = []
    for i in range(0, 10):
        clients.append(CacheClient.create(Configurations.Laptop.latest(), CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
                         timedelta(seconds=60)))
        print("Created client " + str(i))

    for i in range(0, 1000):
        client = clients[i % 10]
        print(client.create_cache(str(i)))
        print(client.list_caches())
        print(client.delete_cache(str(i)))


do_work()