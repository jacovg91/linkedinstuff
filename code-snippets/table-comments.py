def set_table_and_column_comments(
    table_dict: dict, table_catalog: str, table_schema: str, table_name: str
) -> None:
    """
    Extracts comments for a table and its columns from a given
    dictionary and sets the comments on a Unity Catalog enabled table.

    Args:
        table_dict (dict): Dictionary containing table metadata with comments. 
        table_catalog (str): Catalog name for the table.
        table_schema (str): Schema name for the table.
        table_name (str): Name of the table to set comments on.
    """
    spark = SparkSession.getActiveSession()
    full_table_name = f"{table_catalog}.{table_schema}.{table_name}"

    tables = table_dict.get("table", {})
    if tables.get("name") == table_name:
        table_comment = tables.get("description")
        spark.sql(
            f"""COMMENT ON TABLE {full_table_name} IS '{table_comment}'"""
        )

        for column in tables.get("columns", []):
            column_name = column.get("column_name")
            column_comment = column.get("description")
            spark.sql(  # 'COMMENT ON COLUMN' doesn't work on DBR 15.4 LTS, therefore ALTER.
                f"""ALTER TABLE {full_table_name}
                    ALTER COLUMN {column_name} COMMENT '{column_comment}'"""
            )
        return
