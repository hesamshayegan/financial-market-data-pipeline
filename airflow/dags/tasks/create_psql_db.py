from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


def load_history_data(df, table_name):

    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f'''
            DROP TABLE IF EXISTS {table_name}
        ''')
        conn.commit()
    
        cursor.execute(f'''
            CREATE TABLE {table_name} (
                "ID" NUMERIC,
                "Date" DATE,
                "Open" FLOAT,
                "High" FLOAT,
                "Low" FLOAT,
                "Close" FLOAT,
                "Volume" FLOAT,
                "Stock" VARCHAR(10)
            );
        ''')
        conn.commit()

        
        insert_query = f'''
            INSERT INTO {table_name} ("ID", "Date", "Open", "High", "Low", "Close", "Volume", "Stock")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
        
        conn.commit()
        print(f"Data uploaded successfully to table: {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()


def load_income_data(df, table_name):

    
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f'''
            DROP TABLE IF EXISTS {table_name}
        ''')
        conn.commit()

        # Create the table if it doesn't exist
        cursor.execute(f'''
            CREATE TABLE {table_name} (
                "ID" NUMERIC,
                "Date" DATE,
                "Gross Profit" FLOAT,
                "Total Revenue" FLOAT,
                "Diluted EPS" FLOAT,
                "Stock" VARCHAR (10)
            );
        ''')
        conn.commit()

        
        insert_query = f'''
            INSERT INTO {table_name} ("ID", "Date", "Gross Profit", "Total Revenue", "Diluted EPS", "Stock")
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
        
        conn.commit()
        print(f"Data uploaded successfully to table: {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()

def load_web_data(df, table_name):

    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f'''
            DROP TABLE IF EXISTS {table_name}
        ''')
        conn.commit()

        cursor.execute(f'''
            CREATE TABLE {table_name} (
                "ID" NUMERIC,
                "Date" DATE,
                "Market Cap" VARCHAR (20),
                "PEratio" FLOAT,
                "Dividend" VARCHAR (10),
                "Stock" VARCHAR (10)
            );
        ''')
        conn.commit()

        
        insert_query = f'''
            INSERT INTO {table_name} ("ID", "Date", "Market Cap", "PEratio", "Dividend", "Stock")
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
        
        conn.commit()
        print(f"Data uploaded successfully to table: {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()