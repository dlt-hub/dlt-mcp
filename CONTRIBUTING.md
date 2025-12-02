# Contributing to dlt-mcp

We are excited to have you contribute to the dlt-mcp project! To get started, please follow the instructions below.

## Development Environment Setup

To set up your local development environment, you can use the following command with `just`:

```bash
just setup
```

Alternatively, you can run:
```bash
uv sync --dev --extra search --extra duckdb
```

This command will sync the development environment using `uv`, including the necessary packages for search and DuckDB.

## Setup up prek (a pre-commit alternative)

This will make sure the type checks and formatting are good.

```bash
uv run prek install
```

## Submitting Changes

When you're ready to contribute, follow these steps:
    - Create an issue describing the feature, bug fix, or improvement you'd like to make.
    - Create a new branch in your forked repository for your changes.
    - Write your code and tests.
    - If you’ve added, removed, or updated dependencies in pyproject.toml, make sure uv.lock is up to date by running uv lock.
    - Create a pull request targeting the devel branch of the main repository. Please link the ticket that describes what you are doing in the PR, or write a PR comment that makes it clear to us and other users without prior knowledge what you are doing here.

## Branch Naming Rules

To ensure that our git history clearly explains what was changed by which branch or PR, we use the following naming convention (all lowercase, with dashes, no underscores):

{category}/{ticket-id}-description-of-the-branch
#### example:
feat/4922-add-schema-diff-tool

Branch Categories:
    - feat: A new feature (ticket required).
    - fix: A bug fix (ticket required).
    - exp: An experiment (ticket encouraged). May later become a feat.
    - test: Related to tests (ticket encouraged).
    - docs: Documentation changes (ticket optional).
    - keep: Branches we want to keep and revisit later (ticket encouraged).
