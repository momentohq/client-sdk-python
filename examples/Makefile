.PHONY: export
export:
	@poetry export -f requirements.txt --output requirements.txt

.PHONY: format
format:
	@poetry run black prepy310 py310
	@poetry run isort prepy310 py310

