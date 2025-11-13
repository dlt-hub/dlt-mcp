# List recipes
default:
    @just --list

# Setup Python environment
setup:
    uv sync --dev --extra search

# Run pre-commit hooks
pre-commit:
    uv run prek run --all-files

# this should correspond to pre-commit config but more verbose
# Lint all files
lint:
    uv run ruff check

# this should correspond to pre-commit config but more verbose
# Format all files
format:
    uv run ruff format

# this should correspond to pre-commit config but more verbose
# Type check all files.
type-check:
    uv run ty check --output-format full

# Run test suite
test:
    uv run pytest tests/
