from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from tasks.nasdaq_webscraping import get_stock_statistics
from tasks.yfinance_api import get_stock_history, get_yearly_income, get_quarterly_income, stocks
from tasks.create_psql_db import create_table_1, create_table_2, create_table_3, create_table_4
from tasks.duckdb_queries import(
    create_formatted_date, 
    remove_null_values,
    remove_duplicates,
)


@dag(
    start_date=datetime.today(),
    schedule_interval="@daily",
    tags=["extract"],
    catchup=False
)

def get_stocks_data():

    start_pipeline = DummyOperator(
            task_id = 'tsk_start_pipeline'
    )
    
    end_pipeline = DummyOperator(
                task_id = 'task_end_pipeline'
    )
    
    # get stocks history
    @task
    def get_stock_history_from_api():
        
        stock_history_df = get_stock_history()

        return stock_history_df
        
    # get yearly income
    @task
    def get_yearly_income_from_api():
        
        yearly_income_df = get_yearly_income()

        return yearly_income_df
    
    # get quartely income 
    @task
    def get_quarterly_income_from_api():
        quarterly_income_df = get_quarterly_income()

        return quarterly_income_df

    @task
    def get_stock_data_from_web(stocks):
        statistics_df = get_stock_statistics(stocks)

        return statistics_df
    
    @task
    def load_stock_data_to_psqldb(df, table_name):
        db_url = 'postgresql+psycopg2://postgres@host.docker.internal:5432/docker_db'
        engine = create_engine(db_url)

        try:
            with engine.begin() as connections:
                df.to_sql(name=table_name, con=connections, index=False, if_exists='replace')
            print(f"Data uploaded successfully to table: {', '.join(table_name)}")
        except Exception as e:
                print(f"An error occured: {e}")

    # duckdb stage
    @task
    def format_dates():
        tablenames = ['stocks_history', 'stocks_yearly_income', 'stocks_quarterly_income', 'stocks_data_web']
        result = create_formatted_date(tablenames)
    
    @task
    def clean_null_values():
        tablenames = ['stocks_yearly_income', 'stocks_quarterly_income']
        result = remove_null_values(tablenames)

    @task
    def clean_duplicates():
        tablenames = ['stocks_history', 'stocks_yearly_income', 'stocks_quarterly_income', 'stocks_data_web']
        result = remove_duplicates(tablenames)

    with TaskGroup(group_id = 'group_a', tooltip= "Extract_from_yfinanceAPI") as group_API:
        stock_history = get_stock_history_from_api()
        stocks_yearly_income = get_yearly_income_from_api()
        stocks_quarterly_income = get_quarterly_income_from_api()
    
    with TaskGroup(group_id = 'group_b', tooltip= "Extract_from_web") as group_webscraping:
        stock_data_from_web = get_stock_data_from_web(stocks)    

    with TaskGroup(group_id='group_staging', tooltip="staging_area") as group_staging:
        
        load_stock_history = load_stock_data_to_psqldb(stock_history, 'stocks_history')
        load_yearly_income = load_stock_data_to_psqldb(stocks_yearly_income, 'stocks_yearly_income')
        load_quarterly_income = load_stock_data_to_psqldb(stocks_quarterly_income, 'stocks_quarterly_income')
        load_data_from_web = load_stock_data_to_psqldb(stock_data_from_web, 'stocks_data_web')

        # dependencies within group_db
        create_table_1 >> load_stock_history
        create_table_2 >> load_yearly_income
        create_table_3 >> load_quarterly_income
        create_table_4 >> load_data_from_web

    
    with TaskGroup(group_id='group_duckdb', tooltip="duckdb_stage") as group_duckdb:
        formatted_date = format_dates()
        removed_null_values = clean_null_values()
        removed_duplicates = clean_duplicates()
        
        formatted_date >> removed_null_values >> removed_duplicates

    
    # pipeline's overall structure
    start_pipeline >> group_API >> group_staging >> group_duckdb >> end_pipeline
    start_pipeline >> group_webscraping >> group_staging >> group_duckdb >> end_pipeline
    

get_stocks_data()