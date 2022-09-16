# Python Client SDK

_Read this in other languages_: [日本語](README.ja.md)
<br>

## Prereqs

- [Python 3.7 or above is required](https://www.python.org/downloads/)
- A Momento Auth Token is required, you can generate one using the [Momento CLI](https://github.com/momentohq/momento-cli)
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

## Running the Example Using Pipenv

- This project uses [`pipenv`](https://packaging.python.org/en/latest/tutorials/managing-dependencies/) to manage dependencies.  This keeps your project dependencies separate from your system python packages.

To install `pipenv` if you don't already have it:

```bash
python3 -m pip install --user pipenv
```

To set up the `pipenv` environment for this project:

```bash
pipenv install
```

To run the examples:

```bash
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> pipenv run python example.py
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> pipenv run python example_async.py
```

To run the examples with SDK debug logging enabled:

```bash
DEBUG=true MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> pipenv run python example.py
DEBUG=true MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> pipenv run python example_async.py
```

## Running the Example Using pip
Install the prerequisites:

```bash
pip install -r requirements.txt
```

To run the examples:

```bash
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> python example.py
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> python example_async.py
```

To run the examples with SDK debug logging enabled:

```bash
DEBUG=true MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> python example.py
DEBUG=true MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> python example_async.py
```

## Using SDK in your project

via `pipenv`:

```bash
pipenv install momento==0.14.0
```

via `pip` and `requirements.txt`:

Add `momento==0.14.0` to `requirements.txt` or any other dependency management framework used by your project.

To install directly to your system:

```bash
pip install --user momento==0.14.0
```

## Running the load generator example

This repo includes a very basic load generator, to allow you to experiment
with performance in your environment based on different configurations.  It's
very simplistic, and only intended to give you a quick way to explore the
performance of the Momento client running on a single python process.

Note that because python has a global interpreter lock, user code runs on
a single thread and cannot take advantage of multiple CPU cores.  Thus, the
limiting factor in request throughput will often be CPU.  Keep an eye on your CPU
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
MOMENTO_AUTH_TOKEN=<YOUR AUTH TOKEN> pipenv run python example_load_gen.py
```

You can check out the example code in [example_load_gen.py](example_load_gen.py).  The configurable
settings are at the bottom of the file.
