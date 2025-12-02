<h1 align="center">
    <strong>data load tool (dlt) — MCP Server</strong>
</h1>
<p align="center">
Connecting your LLM to the best practices and tools to enable exploration, creation and debugging of your data load pipelines.
</p>

<h3 align="center">
🚀 Join our thriving community of likeminded developers and build the future together!
</h3>

<div align="center">
  <a target="_blank" href="https://dlthub.com/community" style="background:none">
    <img src="https://img.shields.io/badge/slack-join-dlt.svg?labelColor=191937&color=6F6FF7&logo=slack" style="width: 260px;"  />
  </a>
</div>

## Installation

DLT runs using uv, you will need to [setup uv](https://docs.astral.sh/uv/getting-started/installation/) first.

To your MCP server config add,

```json
{
  "name": "DLT mcp server",
  "command": "uv",
  "args": [
    "run",
    "--with",
    "dlt-mcp[search]",
    "python",
    "-m",
    "dlt_mcp"
  ],
  "env": {}
}
```

## Tools

The goal of the tools is to allow LLMs to compose different queries allowing you to explore, create or debug pipelines. The following tools are available in the dlt-mcp module:

- **list_pipelines**: Lists all available dlt pipelines. Each pipeline consists of several tables.
- **list_tables**: Retrieves a list of all tables in the specified pipeline.
- **get_table_schemas**: Returns the schema of the specified tables.
- **execute_sql_query**: Executes a SELECT SQL statement for simple data analysis.
- **get_load_table**: Retrieves metadata about data loaded with dlt.
- **get_pipeline_local_state**: Fetches the state information of the pipeline, including incremental dates, resource state, and source state.
- **get_table_schema_diff**: Compares the current schema of a table with another version and provides a diff.
- **search_docs**: Searches over the `dlt` documentation using different modes (hybrid, full_text, or vector) to verify features and identify recommended patterns.
- **search_code**: Searches the source code for the specified query and optional file path, providing insights into internal code structures and patterns.


## Get Involved

The dlt-mcp project is quickly growing, and we're excited to have you join our community! Here's how you can get involved:

- **Connect with the Community**: Join other dlt users and contributors on our [Slack](https://dlthub.com/community)
- **Report issues and suggest features**: Please use the [GitHub Issues](https://github.com/dlt-hub/dlt-mcp/issues) to report bugs or suggest new features. Before creating a new issue, make sure to search the tracker for possible duplicates and add a comment if you find one.
- **Improve documentation**: Help us enhance the dlt documentation.

## Contribute code
Please read [CONTRIBUTING](CONTRIBUTING.md) before you make a PR.
