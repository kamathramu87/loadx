.PHONY: install
install: ## Install the uv environment.
	@echo "🚀 Creating virtual environment using uv"
	@uv sync

.PHONY: check
check: ## Run code quality tools.
	@echo "🚀 Checking uv lock file consistency with 'pyproject.toml': Running uv lock --check"
	@uv lock --locked
	@echo "🚀 Linting code: Running pre-commit"
	@uv run pre-commit run -a
	@echo "🚀 Static type checking: Running mypy"
	@uv run mypy .
	@echo "🚀 Checking for dependency issues: Running deptry"
	@uv run deptry .

.PHONY: test
test: ## Test the code with pytest.
	@echo "🚀 Testing code: Running pytest"
	@uv run pytest --cov --cov-config=pyproject.toml --cov-report=xml

.PHONY: build
build: clean-build ## Build wheel and sdist files using uv.
	@echo "🚀 Creating wheel and sdist files"
	@uv build

.PHONY: clean-build
clean-build: ## clean build artifacts
	@rm -rf dist

.PHONY: build-check
build-check: build ## Build and validate distributions without uploading.
	@echo "🚀 Validating wheel and sdist metadata"
	@uv tool run --from 'twine>=7,<8' twine check --strict dist/*

.PHONY: publish
publish: build-check ## Build, validate, and publish to PyPI using UV_PUBLISH_TOKEN.
	@echo "🚀 Publishing."
	@uv publish

.PHONY: build-and-publish
build-and-publish: publish ## Alias for publish (build, validate, then upload).

.PHONY: docs-test
docs-test: ## Test if documentation can be built without warnings or errors.
	@uv run mkdocs build -s

.PHONY: docs
docs: ## Build and serve the documentation.
	@uv run mkdocs serve

.PHONY: help
help: ## Show help for the commands.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
