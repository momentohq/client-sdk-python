from momento import __version__ as momento_version


def test_momento_version() -> None:
    assert momento_version != ""
