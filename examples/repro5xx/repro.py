from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

def do_work():
    clients = []
    for i in range(0, 5):
        clients.append(CacheClient.create(Configurations.Laptop.latest(), CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
                         timedelta(seconds=60)))
        print("Created client " + str(i))

    for i in range(0, 1000):
        client = clients[i % 5]
        create = client.create_cache(str(i))
        print(client.list_caches())
        if isinstance(create, CreateCache.Success):
            print(client.delete_cache(str(i)))


do_work()