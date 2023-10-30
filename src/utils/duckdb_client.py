import os

import duckdb
from polars import DataFrame


class DuckbClient:
    def __init__(self, db_path: str = os.path.join("data", "clean", "clean.db")):
        self.con = duckdb.connect(db_path)

    def parquets_to_table(self, table_name: str, parquet_files: list):
        self.con.execute(f"""DROP TABLE IF EXISTS {table_name}""")
        self.con.execute(
            f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                AS
                  SELECT *
                  FROM parquet_scan('{parquet_files[0]}')
                  LIMIT 0
                ;
                """
        )
        for file in parquet_files:
            query = f"INSERT INTO {table_name} SELECT * FROM parquet_scan('{file}');"
            self.con.execute(query)

    def df_to_table(self, table_name: str, df: DataFrame):
        self.con.execute(f"""DROP TABLE IF EXISTS {table_name}""")
        self.con.execute(
            f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                AS
                  SELECT *
                  FROM df
                ;
                """
        )

    def select_all_from_table(self, table_name: str):
        return self.con.execute(f"""SELECT * FROM {table_name}""")

    def select_columns_from_table(self, table_name: str, columns: list):
        return self.con.execute(f"SELECT {', '.join(columns)} FROM {table_name}")

    def execute_sql_string(self, string: str):
        return self.con.execute(string)
