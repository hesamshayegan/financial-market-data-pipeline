from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from tasks.nasdaq_webscraping import get_stock_statistics
from tasks.yfinance_api import get_stock_history, get_yearly_income, get_quarterly_income, stocks
from tasks.create_psql_db import (
load_history_data,
load_income_data,
load_web_data
)

from tasks.duckdb_queries import(
    create_formatted_date, 
    remove_null_values,
    remove_duplicates,
    create_pe_ratios_csv,
    create_stock_history_csv,
    create_current_pe_ratios_csv,
    create_stocks_yearly_income_csv,
    create_stocks_quarterly_income_csv
)


@dag(
    start_date=datetime.today(),
    schedule_interval="@daily",
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
    def load_stock_history_to_db(df, table_name):
        result = load_history_data(df, table_name)

    @task
    def load_stock_income_to_db(df, table_name):
        result = load_income_data(df, table_name)

    @task
    def load_stock_web_to_db(df, table_name):
        result = load_web_data(df, table_name)
    

    #duckdb stage
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

    @task
    def create_petable_output():
        result = create_pe_ratios_csv()

    @task
    def create_stock_history_output():
        result = create_stock_history_csv()

    @task
    def create_current_pe_ratios_table():
        result = create_current_pe_ratios_csv()

    @task
    def create_stocks_yearly_income_table():
        result = create_stocks_yearly_income_csv()

    @task
    def create_stocks_quarterly_income_table():
        result = create_stocks_quarterly_income_csv()


    with TaskGroup(group_id = 'group_a', tooltip= "Extract_from_yfinanceAPI") as group_API:
        stock_history = get_stock_history_from_api()
        stocks_yearly_income = get_yearly_income_from_api()
        stocks_quarterly_income = get_quarterly_income_from_api()
    
    with TaskGroup(group_id = 'group_b', tooltip= "Extract_from_web") as group_webscraping:
        stock_data_from_web = get_stock_data_from_web(stocks)    

    with TaskGroup(group_id='group_staging', tooltip="staging_area") as group_staging:
        
        load_stock_history_to_db(stock_history, 'stocks_history')
        load_stock_income_to_db(stocks_yearly_income, 'stocks_yearly_income')
        load_stock_income_to_db(stocks_quarterly_income, 'stocks_quarterly_income')
        load_stock_web_to_db(stock_data_from_web, 'stocks_data_web')


    with TaskGroup(group_id='group_duckdb', tooltip="duckdb_stage") as group_duckdb:
        formatted_date = format_dates()
        removed_null_values = clean_null_values()
        removed_duplicates = clean_duplicates()
        
        formatted_date >> removed_null_values >> removed_duplicates

    with TaskGroup(group_id='group_output', tooltip="output_stage") as group_output:
        petable_results = create_petable_output()
        stocks_history_results = create_stock_history_output()
        current_pe_results = create_current_pe_ratios_table()
        stocks_yearly_income_results = create_stocks_yearly_income_table()
        stocks_quarterly_income_results = create_stocks_quarterly_income_table()

        stocks_history_results >> petable_results >> current_pe_results >> stocks_yearly_income_results >> stocks_quarterly_income_results


    #pipeline's overall structure
    start_pipeline >> group_API >> group_staging >> group_duckdb >> group_output >> end_pipeline
    start_pipeline >> group_webscraping >> group_staging >> group_duckdb >> group_output >> end_pipeline
    

get_stocks_data()