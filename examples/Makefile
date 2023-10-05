.PHONY: export
export:
	@poetry export -f requirements.txt --output requirements.txt

.PHONY: format
format:
	@poetry run black prepy310 py310
	@poetry run isort prepy310 py310


.PHONY: lint
lint:
	@poetry run mypy prepy310 py310
	@poetry run flake8 prepy310 py310
