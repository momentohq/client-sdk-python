<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

# Momento Python Client Examples

_Read this in other languages_: [日本語](README.ja.md)
<br>

## Prereqs

- [Python 3.7 or above is required](https://www.python.org/downloads/)
- To get started with Momento you will need a Momento API key. You can get one from the [Momento Console](https://console.gomomento.com).
- Developer libraries (gcc/python dev headers) installed on machine you intend to run on

**Amazon Linux**

```bash
sudo yum groupinstall "Development Tools"
sudo yum install python3-devel
```

**Ubuntu**

```bash
sudo apt install build-essential
sudo apt-get install python3-dev
```

**OSX**

```bash
xcode-select --install
```

## Running the Example Using Poetry (Recommended)

- We use [poetry](https://python-poetry.org/docs/) to manage dependencies and packaging. This allows us to cleanly separate release vs development dependencies, as well as streamline deployment. See the [poetry docs](https://python-poetry.org/docs/#installation) for installation instructions.

To set up the `poetry` environment for this project:

```bash
poetry install
```

To run the python version 3.10+ examples:

```bash
MOMENTO_API_KEY=<YOUR_API_KEY> poetry run python -m py310.example
MOMENTO_API_KEY=<YOUR_API_KEY> poetry run python -m py310.example_async
```

To run the examples with SDK debug logging enabled:

```bash
DEBUG=true MOMENTO_API_KEY=<YOUR_API_KEY> poetry run python -m py310.example
DEBUG=true MOMENTO_API_KEY=<YOUR_API_KEY> poetry run python -m py310.example_async
```

To run the python version <3.10 examples:

```bash
MOMENTO_API_KEY=<YOUR_API_KEY> poetry run python -m prepy310.example
MOMENTO_API_KEY=<YOUR_API_KEY> poetry run python -m prepy310.example_async
```

To run the examples with SDK debug logging enabled:

```bash
DEBUG=true MOMENTO_API_KEY=<YOUR_API_KEY> poetry run python -m prepy310.example
DEBUG=true MOMENTO_API_KEY=<YOUR_API_KEY> poetry run python -m prepy310.example_async
```

## Running the Example Using pip

Install the prerequisites:

```bash
pip install -r requirements.txt
```

To run the python version 3.10+ examples:

```bash
MOMENTO_API_KEY=<YOUR_API_KEY> python -m py310.example
MOMENTO_API_KEY=<YOUR_API_KEY> python -m py310.example_async
```

To run the examples with SDK debug logging enabled:

```bash
DEBUG=true MOMENTO_API_KEY=<YOUR_API_KEY> python -m py310.example
DEBUG=true MOMENTO_API_KEY=<YOUR_API_KEY> python -m py310.example_async
```

To run the python version <3.10 examples:

```bash
MOMENTO_API_KEY=<YOUR_API_KEY> python -m prepy310.example
MOMENTO_API_KEY=<YOUR_API_KEY> python -m prepy310.example_async
```

To run the examples with SDK debug logging enabled:

```bash
DEBUG=true MOMENTO_API_KEY=<YOUR_API_KEY> python -m prepy310.example
DEBUG=true MOMENTO_API_KEY=<YOUR_API_KEY> python -m prepy310.example_async
```

## Running the load generator example

This repo includes a very basic load generator, to allow you to experiment
with performance in your environment based on different configurations. It's
very simplistic, and only intended to give you a quick way to explore the
performance of the Momento client running on a single python process.

Note that because python has a global interpreter lock, user code runs on
a single thread and cannot take advantage of multiple CPU cores. Thus, the
limiting factor in request throughput will often be CPU. Keep an eye on your CPU
consumption while running the load generator, and if you reach 100%
of a CPU core then you most likely won't be able to improve throughput further
without running additional python processes.

CPU will also impact your client-side latency; as you increase the number of
concurrent requests, if they are competing for CPU time then the observed
latency will increase.

Also, since performance will be impacted by network latency, you'll get the best
results if you run on a cloud VM in the same region as your Momento cache.

Check out the configuration settings at the bottom of the 'example_load_gen.py' to
see how different configurations impact performance.

If you have questions or need help experimenting further, please reach out to us!

To run the load generator:

```bash
# Run example load generator
MOMENTO_API_KEY=<YOUR AUTH TOKEN> poetry run python -m py310.example_load_gen
```

You can check out the example code in [example_load_gen.py](py310/example_load_gen.py). The configurable
settings are at the bottom of the file.
