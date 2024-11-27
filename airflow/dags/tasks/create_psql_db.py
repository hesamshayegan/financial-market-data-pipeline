from airflow.providers.postgres.operators.postgres import PostgresOperator


create_table_1 = PostgresOperator(
    task_id='tsk_create_table_1',
    postgres_conn_id="postgres_conn",
    sql='''
        CREATE TABLE IF NOT EXISTS stocks_history (
            "Date" DATE,
            "Open" FLOAT,
            "High" FLOAT,
            "Low" FLOAT,
            "Close" FLOAT,
            "Volume" FLOAT,
            "Stock" VARCHAR (10)
        );
    ''',
    database='docker_db'
    )


create_table_2 = PostgresOperator(
task_id='tsk_create_table_2',
postgres_conn_id="postgres_conn",
sql='''
    CREATE TABLE IF NOT EXISTS stocks_yearly_income (
        "Date" DATE,
        "Gross Profit" FLOAT,
        "Total Revenue" FLOAT,
        "Diluted EPS" FLOAT,
        "Stock" VARCHAR (10)
    );
''',
database='docker_db'
)


create_table_3 = PostgresOperator(
task_id='tsk_create_table_3',
postgres_conn_id="postgres_conn",
sql='''
    CREATE TABLE IF NOT EXISTS stocks_quarterly_income (
        "Date" DATE,
        "Gross Profit" FLOAT,
        "Total Revenue" FLOAT,
        "Stock" VARCHAR (10)
    );
''',
database='docker_db'
)


create_table_4 = PostgresOperator(
task_id='tsk_create_table_4',
postgres_conn_id="postgres_conn",
sql='''
    CREATE TABLE IF NOT EXISTS stocks_data_web (
        "Date" DATE,
        "Market Cap Profit" FLOAT,
        "PEratio Revenue" FLOAT,
        "Dividend" FLOAT,
        "Stock" VARCHAR (10)
    );
''',
database='docker_db'
)