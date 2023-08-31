import asyncio
from datetime import timedelta

from momento import CacheClientAsync, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

async def do_work():
    clients = []
    for i in range(0, 2):
        clients.append(CacheClientAsync(Configurations.Laptop.latest(), CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
                                          timedelta(seconds=60)))
        print("Created client " + str(i))

    for i in range(0, 1000):

        client = clients[i % 2]

        create = print("Client " + str(i%2) + " : " + str(await client.create_cache(str(i))))
        print("Client " + str(i%2) + " : " + str(await client.list_caches()))
        print("Client " + str(i%2) + " : " + str(await client.delete_cache(str(i))))


if __name__ == "__main__":
    asyncio.run(do_work())