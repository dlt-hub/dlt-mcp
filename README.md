<h1 align="center">
    <strong>data load tool (dlt) — MCP Server</strong>
</h1>
<p align="center">
  🚀 Follow <a href="https://dlthub.com/docs/dlt-ecosystem/llm-tooling/llm-native-workflow">this guide</a> to create a dlt pipeline in 10mins with AI
</p>


## How is it useful?

Large language models (LLMs) know a lot about the world, but nothing about your specific code and data. 

The [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server allows the LLM to retrieve **up-to-date** and **correct** information about your [dlt](https://github.com/dlt-hub/dlt) pipelines, datasets, schema, etc. This significantly improves the development experience in AI-enabled IDEs (Copilot, Cursor, Continue, Claude Code, etc.)

## Installation

The package manager [uv](https://docs.astral.sh/uv/getting-started/installation/) is required to launch the MCP server.

Add this section to your MCP configuration file inside your IDE.

```json
{
  "name": "dlt",
  "command": "uv",
  "args": [
    "run",
    "--with",
    "dlt-mcp[search]",
    "python",
    "-m",
    "dlt_mcp"
  ],
}
```

>[!NOTE]
>The configuration file format varies slightly across IDEs

## Features
### Tools

The dlt MCP server provides [tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools) that allows the LLM to take actions:

- **list_pipelines**: Lists all available dlt pipelines. Each pipeline consists of several tables.
- **list_tables**: Retrieves a list of all tables in the specified pipeline.
- **get_table_schemas**: Returns the schema of the specified tables.
- **execute_sql_query**: Executes a SELECT SQL statement for simple data analysis.
- **get_load_table**: Retrieves metadata about data loaded with dlt.
- **get_pipeline_local_state**: Fetches the state information of the pipeline, including incremental dates, resource state, and source state.
- **get_table_schema_diff**: Compares the current schema of a table with another version and provides a diff.
- **search_docs**: Searches over the `dlt` documentation using different modes (hybrid, full_text, or vector) to verify features and identify recommended patterns.
- **search_code**: Searches the source code for the specified query and optional file path, providing insights into internal code structures and patterns.
