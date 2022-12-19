import setuptools
import os

version = os.getenv("MOMENTO_SDK_VERSION")

if(version is None):
    version = '0.0.0+localdev0'

# version is the only dynamic configuration
setuptools.setup(
    version=version,
    long_description="# Momento SDK\n Check out our SDK example [here](https://github.com/momentohq/client-sdk-examples/tree/main/python)!",
    long_description_content_type="text/markdown",
    project_urls={
        'Source': 'https://github.com/momentohq/client-sdk-python',
    },
)
