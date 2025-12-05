# Contributing to `dlt-mcp`

## Developer setup

1. Install the package manager [uv](https://docs.astral.sh/uv/getting-started/installation/)
2. Install the Python development dependencies (includes the `just` command)

    ```shell
    uv sync --dev
    ```
3. Finish development setup. This will install Python extras, pre-commit hooks, and more

    ```shell
    just setup
    ```