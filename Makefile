.PHONY: test
test:
	uv run --group test pytest tests/ -v

.PHONY: lint
lint:
	ruff check src/
	ruff format --check src/

.PHONY: format
format:
	ruff format src/

.PHONY: build
build:
	uv build --wheel

.PHONY: validate
validate:
	databricks bundle validate

.PHONY: deploy-dev
deploy-dev: lint test build
	databricks bundle deploy -t dev

.PHONY: deploy-prod
deploy-prod: lint is-git-clean test build
	databricks bundle deploy -t prod

.PHONY: is-git-clean
is-git-clean:
	@status=$$(git fetch origin && git status -s -b) ;\
	if test "$${status}" != "## main...origin/main"; then \
		echo; \
		echo Git working directory is dirty, aborting >&2; \
		false; \
	fi
