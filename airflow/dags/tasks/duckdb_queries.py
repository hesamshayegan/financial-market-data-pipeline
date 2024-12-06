import duckdb
from sqlalchemy import create_engine
import pandas as pd
import os

DB_URL = 'postgresql+psycopg2://postgres@host.docker.internal:5432/docker_db'
engine = create_engine(DB_URL)

# get the directory of the current file
current_dir = os.path.dirname(os.path.abspath(__file__))

# get the parent directory
parent_dir = os.path.dirname(current_dir)

# construct the input file's path
file_path = os.path.join(parent_dir, 'db')
os.makedirs(file_path, exist_ok=True) 
DUCKDB_PATH = f"{file_path}/stocks_database.duckdb"

# # duckdb_conn = duckdb.connect(database="s3://my-bucket/my_database.duckdb", read_only=False)


def create_formatted_date(tablenames):

    for tablename in tablenames:
        try:
            with engine.begin() as connection:
                duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)
                
                rds_table_query = f"SELECT * FROM {tablename}"

                df_rds_table = pd.read_sql(sql=rds_table_query, con=connection)

                drop_table = f"DROP TABLE IF EXISTS cleaned_{tablename};"
                duckdb_conn.execute(drop_table)

                create_table = f"CREATE TABLE cleaned_{tablename} AS SELECT * FROM df_rds_table;"
                duckdb_conn.execute(create_table)

                add_column = f"ALTER TABLE cleaned_{tablename} ADD COLUMN IF NOT EXISTS formatted_date DATE;"
                duckdb_conn.execute(add_column)

                update_table = f"""
                                UPDATE cleaned_{tablename}
                                SET "formatted_date" = CAST("Date" AS DATE);
                                """
                duckdb_conn.execute(update_table)
            

                print(f"Created cleaned_{tablename} in DuckDB.")

                duckdb_conn.close()


        except Exception as e:
                    print(f"An error occured: {e}")



def remove_null_values(tablenames):
      
    for tablename in tablenames:
        try:
            duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)

            initial_count_query = f"SELECT COUNT(*) AS count FROM cleaned_{tablename};"
            initial_rows = duckdb_conn.sql(initial_count_query).fetchone()[0]

            delete_null_values_query = f"""
                    DELETE FROM cleaned_{tablename} 
                    WHERE COALESCE("Gross Profit", "Total Revenue", "Diluted EPS") IS NULL;
                    """
            duckdb_conn.execute(delete_null_values_query)

            print(f"Null values removed from cleaned_{tablename}.")

            final_count_query = f"SELECT COUNT(*) AS count FROM cleaned_{tablename};"
            final_rows = duckdb_conn.sql(final_count_query).fetchone()[0]

            print(f"Original rows: {initial_rows}, Final rows: {final_rows}")

            duckdb_conn.close()

        except Exception as e:
                    print(f"An error occured: {e}")


def remove_duplicates(tablenames):
     
    for tablename in tablenames:
        try:
            duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)

            initial_count_query = f"SELECT COUNT(*) AS count FROM cleaned_{tablename};"
            initial_rows = duckdb_conn.sql(initial_count_query).fetchone()[0]

            remove_duplicates_query = f"""
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
    
            duckdb_conn.execute(remove_duplicates_query)

            print(f"Duplicate values removed from cleaned_{tablename}")

            final_count_query = f"SELECT COUNT(*) AS count FROM cleaned_{tablename};"
            final_rows = duckdb_conn.sql(final_count_query).fetchone()[0]

            print(f"Original rows: {initial_rows}, Final rows: {final_rows}")

        except Exception as e:
            print(f"An error occured: {e}")    