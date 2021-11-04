import setuptools
import os

version = os.getenv("MOMENTO_SDK_VERSION")

if [version == None]:
    version = '0.0.dev'
    print('version:' + version)

# version is the only dynamic configuration
setuptools.setup(
    version=version,
)
