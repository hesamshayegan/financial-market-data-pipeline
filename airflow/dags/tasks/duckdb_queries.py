from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import duckdb
from datetime import datetime


POSTGRES_CONN_ID = 'postgres_conn'
DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'stocks_database.duckdb')
OUTPUT_PATH_BASE = "s3://financial-s3-bucket-ed39d568"
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)


print(f"DuckDB Path: {DUCKDB_PATH}")


def create_formatted_date(tablenames):

    for tablename in tablenames:
        try:
            
            postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            with postgres_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    
                    rds_table_query = f"SELECT * FROM {tablename};"
                    df_rds_table = pd.read_sql(sql=rds_table_query, con=conn)

                    duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)

                    initial_count_query = f"SELECT COUNT(*) AS count FROM cleaned_{tablename};"
                    initial_rows = duckdb_conn.sql(initial_count_query).fetchone()[0]

                    drop_table = f"DROP TABLE IF EXISTS cleaned_{tablename};"
                    duckdb_conn.execute(drop_table)

                    create_table = f"CREATE TABLE cleaned_{tablename} AS SELECT * FROM df_rds_table;"
                    duckdb_conn.execute(create_table)

                    add_column = f"ALTER TABLE cleaned_{tablename} ADD COLUMN IF NOT EXISTS formatted_date DATE;"
                    duckdb_conn.execute(add_column)

                    update_table = f"""
                        UPDATE cleaned_{tablename}
                        SET formatted_date = CAST(Date AS DATE);
                    """
                    duckdb_conn.execute(update_table)

                    print(f"Created cleaned_{tablename} in DuckDB.")

                    final_count_query = f"SELECT COUNT(*) AS count FROM cleaned_{tablename};"
                    final_rows = duckdb_conn.sql(final_count_query).fetchone()[0]

                    print(f"Original rows: {initial_rows}, Final rows: {final_rows}")

                    duckdb_conn.close()

        except Exception as e:
            print(f"An error occurred with table {tablename}: {e}")



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


def create_pe_ratios_csv():
    
    try:
        duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)

        drop_table = f"DROP TABLE IF EXISTS pe_ratios;"
        duckdb_conn.execute(drop_table)

        query = f"""
                CREATE TABLE pe_ratios AS
                SELECT * FROM (
                WITH csyi AS (
                    SELECT
                        *,
                        EXTRACT(YEAR FROM "formatted_date") As year,
                        EXTRACT(MONTH FROM "formatted_date") As month
                    FROM cleaned_stocks_yearly_income
                    )
                    SELECT
                        csh."Stock",
                        csh."High",
                        csh."Low",
                        (csh."High" + csh."Low") / 2 AS average_value,
                        csyi."Diluted EPS",
                        ((csh."High" + csh."Low") / 2) / csyi."Diluted EPS" AS pe_ratio,
                        csh."formatted_date"
                    FROM cleaned_stocks_history AS csh
                    INNER JOIN csyi ON csyi."Stock" = csh."Stock"
                    WHERE csh."formatted_date" = (
                        SELECT MAX("formatted_date")
                        FROM cleaned_stocks_history
                        WHERE
                            EXTRACT(YEAR FROM "formatted_date") = csyi.year AND
                            EXTRACT(MONTH FROM "formatted_date") = csyi.month 
                    )
                    ORDER BY csh."Stock", csh."formatted_date"
                    );
                """
        
        duckdb_conn.execute(query)

        print("pe_ratios table was created successfully")
        
        # install s3 extension
        duckdb_conn.execute("INSTALL s3;")
        duckdb_conn.execute("LOAD s3;")

        now = datetime.now()
        dt_string = now.strftime("%m%d%Y%H%M%S")
        result_to_csv = f"COPY pe_ratios TO '{OUTPUT_PATH_BASE}/pe_ratios_table_{dt_string}.csv' (HEADER, DELIMITER ',');"
        
        duckdb_conn.execute(result_to_csv)

        print(f"Table saved to {OUTPUT_PATH_BASE}/pe_ratios_table_{dt_string}.csv")

        duckdb_conn.close()

    except Exception as e:
        print(f"{e}")


def create_stock_history_csv():
     
    try:
        duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)

        query = query = f"""
                SELECT "Stock", "Open", "High", "Low", "Close", "formatted_date"
                FROM cleaned_stocks_history
                """

        # install s3 extension
        duckdb_conn.execute("INSTALL s3;")
        duckdb_conn.execute("LOAD s3;")

        now = datetime.now()
        dt_string = now.strftime("%m%d%Y%H%M%S")
        result_to_csv = (f"COPY ({query}) TO '{OUTPUT_PATH_BASE}/stocks_price_history_table_{dt_string}.csv' (HEADER, DELIMITER ',');")

        duckdb_conn.execute(result_to_csv)
        
        print(f"Table saved to {OUTPUT_PATH_BASE}/stocks_price_history_table_{dt_string}.csv")

        duckdb_conn.close()
        
    except Exception as e:
        print(f"{e}")


def create_current_pe_ratios_csv():
     
    try:

        duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)

        query = f"""
                SELECT *
                FROM cleaned_stocks_data_web
                """
        # install s3 extension
        duckdb_conn.execute("INSTALL s3;")
        duckdb_conn.execute("LOAD s3;")

        now = datetime.now()
        dt_string = now.strftime("%m%d%Y%H%M%S")
        result_to_csv = (f"COPY ({query}) TO '{OUTPUT_PATH_BASE}/stocks_current_pe_ratio_table_{dt_string}.csv' (HEADER, DELIMITER ',');")

        duckdb_conn.execute(result_to_csv)
        
        print(f"Query results saved to {OUTPUT_PATH_BASE}/stocks_current_pe_ratio_table_{dt_string}.csv")

        duckdb_conn.close()
        
    except Exception as e:
        print(f"{e}")


def create_stocks_yearly_income_csv():
     
    try:
        duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)

        query = f"""
                SELECT *
                FROM cleaned_stocks_yearly_income
                """

        # install s3 extension
        duckdb_conn.execute("INSTALL s3;")
        duckdb_conn.execute("LOAD s3;")

        now = datetime.now()
        dt_string = now.strftime("%m%d%Y%H%M%S")
        result_to_csv = (f"COPY ({query}) TO '{OUTPUT_PATH_BASE}/stocks_yearly_income_table_{dt_string}.csv' (HEADER, DELIMITER ',');")

        duckdb_conn.execute(result_to_csv)

        print(f"Query results saved to {OUTPUT_PATH_BASE}/stocks_yearly_income_table_{dt_string}.csv")

        duckdb_conn.close()
        
    except Exception as e:
        print(f"{e}")


def create_stocks_quarterly_income_csv():
     
    try:
        duckdb_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)

        query = f"""
                SELECT *
                FROM cleaned_stocks_quarterly_income
                """
        
        # install s3 extension
        duckdb_conn.execute("INSTALL s3;")
        duckdb_conn.execute("LOAD s3;")

        now = datetime.now()
        dt_string = now.strftime("%m%d%Y%H%M%S")
        result_to_csv = (f"COPY ({query}) TO '{OUTPUT_PATH_BASE}/stocks_quarterly_income_table_{dt_string}.csv' (HEADER, DELIMITER ',');")

        duckdb_conn.execute(result_to_csv)


        print(f"Query results saved to {OUTPUT_PATH_BASE}/stocks_quarterly_income_table_{dt_string}.csv")


        duckdb_conn.close()
        
    except Exception as e:
        print(f"{e}")