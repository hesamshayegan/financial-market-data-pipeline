import duckdb
import pandas as pd

DUCKDB_PATH = "/Users/hesam/Desktop/codes/financial-market-project/airflow/dags/db/stocks_database.duckdb"

try:

    duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)
    tablename = "stocks_history"

    initial_count_query = f"SELECT COUNT(*) AS count FROM cleaned_{tablename};"
    initial_rows = duckdb_conn.sql(initial_count_query).fetchone()[0]

    remove_duplicates = f"""
                WITH ranked_rows AS (
                    SELECT "ID", ROW_NUMBER() OVER (
                        PARTITION BY "Date", "Stock"
                        ORDER BY "ID"
                    ) AS row_num
                    FROM cleaned_{tablename}
                )
                DELETE FROM cleaned_{tablename}
                WHERE "ID" IN (
                    SELECT "ID"
                    FROM ranked_rows
                    WHERE row_num > 1
                )
                """
    
    duckdb_conn.execute(remove_duplicates)

    final_count_query = f"SELECT COUNT(*) AS count FROM cleaned_{tablename};"
    final_rows = duckdb_conn.sql(final_count_query).fetchone()[0]

    print(f"Original rows: {initial_rows}, Final rows: {final_rows}")

    duckdb_conn.close()

except Exception as e:
    print(f"{e}")


