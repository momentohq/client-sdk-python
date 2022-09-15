.PHONY: all
## Generate sync unit tests, format, lint, and test
all: precommit

.PHONY: install-poetry
## Bootstrap installing poetry
install-poetry:
	curl -sSL https://install.python-poetry.org | python3 -

.PHONY: install
## Install project and dependencies
install:
	@poetry install

.PHONY: format
## Format the code using black and isort
format:
	@poetry run black src tests
	@poetry run isort .

.PHONY: lint
## Lint the code using mypy and flake8
lint:
	@poetry run mypy src
	@poetry run flake8 src

.PHONY: gen-test
## Generate synchronous test scripts from async test scripts. 
gen-test:
	@bash tests/scripts/sync_from_async.sh

.PHONY: test
## Run unit and integration tests with pytest
test:
	@poetry run pytest

.PHONY: precommit
## Run format, lint, and test as a step before committing.
precommit: gen-test format lint test
	@echo.

.PHONY: clean
## Remove intermediate files
clean:
	@rm -rf dist .mypy_cache .pytest_cache
	@find -name "*__pycache__*" | xargs rm -rf


# See <https://gist.github.com/klmr/575726c7e05d8780505a> for explanation.
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)";echo;sed -ne"/^## /{h;s/.*//;:d" -e"H;n;s/^## //;td" -e"s/:.*//;G;s/\\n## /---/;s/\\n/ /g;p;}" ${MAKEFILE_LIST}|LC_ALL='C' sort -f|awk -F --- -v n=$$(tput cols) -v i=19 -v a="$$(tput setaf 6)" -v z="$$(tput sgr0)" '{printf"%s%*s%s ",a,-i,$$1,z;m=split($$2,w," ");l=n-i;for(j=1;j<=m;j++){l-=length(w[j])+1;if(l<= 0){l=n-i-length(w[j])-1;printf"\n%*s ",-i," ";}printf"%s ",w[j];}printf"\n";}'|more $(shell test $(shell uname) == Darwin && echo '-Xr')
