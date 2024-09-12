from pyspark.sql import SparkSession, DataFrame
from typing import List

def return_schema_evolution(full_table_names: List[str]) -> DataFrame:
    """
    Returns the schema evolution of a list of tables passed.

    Args:
        full_table_names (List[str]): A list of full table names, in format:
                                      'catalog.schema.table'

    Returns:
        df (DataFrame): A DataFrame containing schema evolution for passed tables.
    """
    spark = SparkSession.getActiveSession()
    sql_to_run = """
    WITH CTE_HIST
    AS (
        """ + ' UNION ALL '.join([f"""SELECT '{table}' AS table_name,
        * FROM (DESCRIBE HISTORY {table})""" for table in full_table_names]) + """
    ),
    CTE_STRUCT
    AS (
        SELECT *
        ,      explode(from_json(operationParameters.columns,
                            'array<struct<column:struct<name:string>>>'
                            ).column.name) AS column
        FROM CTE_HIST
        WHERE operation = 'ADD COLUMNS'
        UNION ALL
        SELECT *
        ,      explode(from_json(operationParameters.columns,
                            'array<string>'
                            )) AS column
        FROM CTE_HIST
        WHERE operation = 'DROP COLUMNS'
    )
    SELECT table_name AS table_name 
    ,      column AS column_name
    ,      IF(operation = 'ADD COLUMNS', 'ADDED', 'REMOVED') AS added_or_removed
    ,      timestamp AS schema_evolution_date_time
    ,      version AS changed_by_delta_version
    ,      userName AS changed_by_user
    ,      job.jobId AS changed_by_job_id
    ,      job.jobName AS changed_by_job_name
    FROM CTE_STRUCT
    """
    df = spark.sql(sql_to_run)

    return df
