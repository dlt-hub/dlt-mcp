from typing import Optional
from dlt.common.schema.typing import TTableReference


def infer_table_reference(pipeline_name: Optional[str] = None):
    """Generates guidelines to infer table references for a given pipeline"""
    table_reference_documentation = _get_table_reference_documentation()

    prompt = (
        "You are an helpful assistant to data architect and data engineers using DLT tasked with analyzing table relationships within a dlt pipeline.\n"
        "## Workflow Steps \n"
    )

    if pipeline_name is None:
        prompt += (
            "- **Pipeline Discovery**:\n"
            "   - First, list all available pipelines using `list_pipelines()`\n"
            "   - Ask the user which pipeline they want to investigate \n"
            "   - wait until the user has provided a valid pipeline name"
        )
    prompt += (
        "-  **Data Exploration**:\n"
        "   - Ask the User if they want to explore specific tables or all of them \n"
        "   - Get schema details for each table using `get_table_schema(pipeline_name, table_name)`\n"
        "   - Show schema using mermaid, this will help make conversation more easier and save the world from bad data schemas \n"
        "- **Relationship Analysis**:\n"
        "   - Look for common column names across tables (e.g., 'user_id', 'customer_id')\n"
        "   - Identify foreign key patterns (e.g., 'parent_id', 'foreign_key')\n"
        "   - Check for date/time columns that might indicate relationships\n"
        "   - Examine table descriptions for hints about relationships\n"
        "   - Look for auto-incrementing IDs that might reference other tables\n"
        "   - Generate mermaid to showcase the relationships. This too helps save the world from bad data\n"
        "- **Validate Relationships**:\n"
        "   - Suggest ways to confirm these relationships (e.g., sample data inspection, referential integrity checks)\n"
        "   - You can execute these validations using execute_sql_query tool \n"
        "- **Generating Table References**: \n"
        f"  - {table_reference_documentation} \n"
        "   - To maintain the information across codebase it's important to generate the table reference in the above format \n"
        "## Tips:\n"
        "- Use only the tools available to go through the process \n"
        "- keep explanations small and to the point until asked for more details \n"
        "- Think before providing reasoning about the relationships and one by one confirm each of them with the user \n"
        "- NO NEED TO EXPLAIN THE FULL STRATEGY IN THE BEGINING KEEP IT SMALL\n"
        "- DON'T ASK FOR PERMISION TO CREATE A MERMAID DIAGRAM. JUST DO IT\n"
        "- AT THE END GENERATE TABLE REFERENCES IN THE FORMAT DEFINED ABOVE THIS CAN ACCELERATE THE USERS WORKFLOW 10 FOLD BECAUSE THEY CAN DIRECTLY USE IT IN CODE\n"
        "## Information Presentation**:\n"
        "   GENERATE MERMAID DIAGRAM TO REPRESENT THE SCHEMA AND RELATIONSHIPS"
    )
    return prompt


def _get_table_reference_documentation() -> str:
    """Generate documentation for TTableReference with examples."""
    user_id_ref: TTableReference = {
        "columns": ["user_id"],
        "referenced_table": "users",
        "referenced_columns": ["id"],
    }

    product_ref: TTableReference = {
        "columns": ["category_id", "brand_id"],
        "referenced_table": "categories",
        "referenced_columns": ["id", "id"],
    }

    table_reference_documentation = (
        "**TTableReference Documentation: \n"
        f"{TTableReference.__doc__} \n"
        f"Required Columns: {TTableReference.__dict__} \n"
        "Example: \n"
        f"{user_id_ref} \n"
        f"{product_ref} \n"
    )

    return table_reference_documentation
