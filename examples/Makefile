.PHONY: export
export:
	@poetry export -f requirements.txt --output requirements.txt

.PHONY: format
format:
	@poetry run ruff format prepy310 py310

.PHONY: lint
lint:
	@poetry run mypy prepy310 py310
	@poetry run ruff check prepy310 py310
