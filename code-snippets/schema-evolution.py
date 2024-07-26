from pyspark.sql import SparkSession
from typing import str


def evolve_schema_with_column(
    catalog: str,
    schema: str,
    table: str,
    new_column: str,
    new_column_type: str,
    after_column: str,
) -> None:
    """
    Evolves a schema of a table by adding a new column
    in the right order. 

    Args:
        catalog (str): The catalog of the table.
        schema (str): The schema of the table.
        table (str): The table name.
        new_column (str): The column to add.
        new_column_type (str): The data type of the column to add
        after_column (str): The predecessing column for the right order.
    """
    spark = SparkSession.getActiveSession()

    column_exists = spark.sql(f"""
    SELECT COUNT(1) AS boolean
    FROM {catalog}.INFORMATION_SCHEMA.columns 
    WHERE table_catalog = '{catalog}'
    AND table_schema = '{schema}'
    AND table_name = '{table}'
    AND column_name = '{new_column}'
    """).collect()[0][0]

    if not column_exists:
        spark.sql(f"""
        ALTER TABLE {catalog}.{schema}.{table} 
        ADD COLUMN {new_column} {new_column_type} AFTER {after_column}
        """)
