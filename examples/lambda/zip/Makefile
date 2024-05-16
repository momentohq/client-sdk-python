.PHONY: setup
setup:
	python -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

.PHONY: dist
dist:
	rm -rf dist dist.zip
# Some important notes:
# 1. The `--platform` option is used to specify the target platform. In this case, we are targeting the manylinux2014_x86_64 platform for AWS Lambda.
# 2. The `--target` option is used to specify the target directory where the dependencies will be installed.
# 3. The `--implementation` option is used to specify the Python implementation. In this case, we are using the CPython implementation.
# 4. The `--python-version` option is used to specify the Python version. In this case, we are using Python 3.8. You can change this to match the Python version used by AWS Lambda.
# 5. The `--only-binary` option is used to specify that only binary distributions should be used. This is important because AWS Lambda does not support building from source.
# 6. The `--upgrade` option is used to ensure that the dependencies are up-to-date.
# 7. The `-r requirements.txt` option is used to specify the requirements file that contains the dependencies to install.
	.venv/bin/pip install \
		--platform manylinux2014_x86_64 \
		--target=dist \
		--implementation cp \
		--python-version 3.8 \
		--only-binary=:all: \
		--upgrade \
		-r requirements.txt
	cp index.py dist
	cd dist && zip -9r ../dist.zip .
