from datetime import timedelta

from momento import CacheClient, Configurations, CredentialProvider

def do_work():
    client = CacheClient.create(Configurations.Laptop.latest(), CredentialProvider.from_environment_variable("MOMENTO_AUTH_TOKEN"),
                                          timedelta(seconds=60))
    create = client.create_cache("repro-simple")
    print("created cache repro-simple")

do_work()