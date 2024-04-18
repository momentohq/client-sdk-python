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
## Format the code using ruff
format:
	@poetry run ruff format src tests

.PHONY: lint
## Lint the code using ruff and mypy
lint:
	@poetry run ruff check src tests
	@poetry run mypy src tests

.PHONY: do-gen-sync
do-gen-sync:
	@poetry run python src/momento/internal/codegen.py src/momento/internal/aio/_scs_control_client.py src/momento/internal/synchronous/_scs_control_client.py
	@poetry run python src/momento/internal/codegen.py src/momento/internal/aio/_scs_data_client.py src/momento/internal/synchronous/_scs_data_client.py
	@poetry run python src/momento/internal/codegen.py src/momento/cache_client_async.py src/momento/cache_client.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/shared_behaviors_async.py tests/momento/cache_client/shared_behaviors.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/test_init_async.py tests/momento/cache_client/test_init.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/test_control_async.py tests/momento/cache_client/test_control.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/test_dictionary_async.py tests/momento/cache_client/test_dictionary.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/test_list_async.py tests/momento/cache_client/test_list.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/test_scalar_async.py tests/momento/cache_client/test_scalar.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/test_set_async.py tests/momento/cache_client/test_set.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/test_sorted_set_async.py tests/momento/cache_client/test_sorted_set.py
	@poetry run python src/momento/internal/codegen.py tests/momento/cache_client/test_sorted_set_simple_async.py tests/momento/cache_client/test_sorted_set_simple.py

.PHONY: gen-sync
## Generate synchronous code and tests from asynchronous code.
gen-sync: do-gen-sync format lint

.PHONY: test
## Run unit and integration tests with pytest
test:
	@poetry run pytest

.PHONY: precommit
## Run format, lint, and test as a step before committing.
precommit: gen-sync format lint test


.PHONY: clean
## Remove intermediate files
clean:
	@rm -rf dist .mypy_cache .pytest_cache
	@find . -name "*__pycache__*" | xargs rm -rf


# See <https://gist.github.com/klmr/575726c7e05d8780505a> for explanation.
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)";echo;sed -ne"/^## /{h;s/.*//;:d" -e"H;n;s/^## //;td" -e"s/:.*//;G;s/\\n## /---/;s/\\n/ /g;p;}" ${MAKEFILE_LIST}|LC_ALL='C' sort -f|awk -F --- -v n=$$(tput cols) -v i=19 -v a="$$(tput setaf 6)" -v z="$$(tput sgr0)" '{printf"%s%*s%s ",a,-i,$$1,z;m=split($$2,w," ");l=n-i;for(j=1;j<=m;j++){l-=length(w[j])+1;if(l<= 0){l=n-i-length(w[j])-1;printf"\n%*s ",-i," ";}printf"%s ",w[j];}printf"\n";}'|more $(shell test $(shell uname) == Darwin && echo '-Xr')
