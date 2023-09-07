from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

def do_work():
    clients = []
    for i in range(0, 2):
        clients.append(CacheClient(Configurations.Laptop.latest(), CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
                         timedelta(seconds=60)))
        print("Created client " + str(i))

    for i in range(0, 1000):

        client = clients[i % 2]
        set = client.set("cache", "key", "val")
        get = client.get("cache", "key")
        if (isinstance(get, CacheGet.Hit)):
            print("Hit!")


do_work()
