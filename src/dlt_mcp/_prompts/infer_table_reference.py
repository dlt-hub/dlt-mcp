from typing import Optional


def infer_table_reference(pipeline_name: Optional[str] = None):
    """Generates guidelines to infer table references for a given pipeline"""
    if pipeline_name is None:
        prompt = (
            "You are tasked with analyzing table relationships across dlt pipelines.\n\n"
            "First, list all available pipelines by running: `dlt pipeline list`\n"
            "Then, ask the user which pipeline they want to investigate.\n\n"
            "Once a pipeline is selected, follow this workflow:\n\n"
            "1. **Fetch all tables**: Run `dlt pipeline <pipeline_name> tables` to get all tables in the pipeline\n"
            "2. **Get table schemas**: For each table, run `dlt pipeline <pipeline_name> schema <table_name>` to fetch schema details\n"
            "3. **Analyze relationships**: Based on column names, data types, descriptions, and table names:\n"
            "   - Look for common column names across tables (e.g., 'user_id', 'customer_id')\n"
            "   - Identify foreign key patterns (e.g., 'parent_id', 'foreign_key')\n"
            "   - Check for date/time columns that might indicate relationships\n"
            "   - Examine table descriptions for hints about relationships\n"
            "   - Look for auto-incrementing IDs that might reference other tables\n\n"
            "4. **Document findings**: List possible combinations of columns that might be related to each other across tables\n"
            "5. **Validate assumptions**: Suggest ways to confirm these relationships (e.g., sample data inspection, referential integrity checks)\n\n"
            "**Note**: You are currently in development mode. If any tools are missing or not available, please indicate which ones are needed.\n"
        )
    else:
        prompt = (
            f"You are tasked with analyzing table relationships for pipeline: {pipeline_name}\n\n"
            "Follow this workflow:\n\n"
            "1. **Fetch all tables**: Run `dlt pipeline {pipeline_name} tables` to get all tables in the pipeline\n"
            "2. **Get table schemas**: For each table, run `dlt pipeline {pipeline_name} schema <table_name>` to fetch schema details\n"
            "3. **Analyze relationships**: Based on column names, data types, descriptions, and table names:\n"
            "   - Look for common column names across tables (e.g., 'user_id', 'customer_id')\n"
            "   - Identify foreign key patterns (e.g., 'parent_id', 'foreign_key')\n"
            "   - Check for date/time columns that might indicate relationships\n"
            "   - Examine table descriptions for hints about relationships\n"
            "   - Look for auto-incrementing IDs that might reference other tables\n\n"
            "4. **Document findings**: List possible combinations of columns that might be related to each other across tables\n"
            "5. **Validate assumptions**: Suggest ways to confirm these relationships (e.g., sample data inspection, referential integrity checks)\n\n"
            "**Note**: You are currently in development mode. If any tools are missing or not available, please indicate which ones are needed.\n"
        )
    return prompt
