.PHONY: test
test:
	uv run --group test pytest tests/ -v

.PHONY: lint
lint:
	uvx ruff@0.15 check src/
	uvx ruff@0.15 format --check src/

.PHONY: format
format:
	uvx ruff@0.15 format src/

.PHONY: build
build:
	uv build --wheel

.PHONY: vendor
vendor:
	mkdir -p dist/deps
	uvx pip download openpyxl -d dist/deps

.PHONY: validate
validate:
	databricks bundle validate

.PHONY: deploy-dev
deploy-dev: lint test build vendor
	databricks bundle deploy -t dev -p BYDELSFAKTA_DEV

.PHONY: deploy-prod
deploy-prod: lint is-git-clean test build vendor
	databricks bundle deploy -t prod -p BYDELSFAKTA_PROD

.PHONY: is-git-clean
is-git-clean:
	@status=$$(git fetch origin && git status -s -b) ;\
	if test "$${status}" != "## main...origin/main"; then \
		echo; \
		echo Git working directory is dirty, aborting >&2; \
		false; \
	fi
