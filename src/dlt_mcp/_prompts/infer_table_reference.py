def infer_table_reference(pipeline_name: str):
    """Generates guidelines to infer table references for a given pipeline"""
    prompt = (
        f"For the pipeline: {pipeline_name} \n"
        + "1. Fetch the all the tables \n"
        + "2. For each table fetch the schema \n"
        + "3. Based on the Column names, descriptions & respective table name and descriptions \n"
        + "List down possible combinations of columns that might be related to each other across tables"
    )
    return prompt
