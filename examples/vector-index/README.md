<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

# Momento Python Vector Index Client Examples

## Prereqs

- [Python 3.7 or above is required](https://www.python.org/downloads/)
- A Momento Auth Token is required, you can generate one from the [Momento Console](https://console.gomomento.com).
- Developer libraries (gcc/python dev headers) installed on machine you intend to run on

## Running the Example Using Poetry (Recommended)

- We use [poetry](https://python-poetry.org/docs/) to manage dependencies and packaging. This allows us to cleanly separate release vs development dependencies, as well as streamline deployment. See the [poetry docs](https://python-poetry.org/docs/#installation) for installation instructions.

To set up the `poetry` environment for this project:

```bash
poetry install
```

To run the basic Momento Vector Index example

```bash
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> poetry run python -m example
```