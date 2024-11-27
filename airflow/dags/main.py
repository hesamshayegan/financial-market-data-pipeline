from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
import os
from tasks.nasdaq_webscraping import get_stock_statistics
from tasks.yfinance_api import get_stock_history, get_yearly_income, get_quarterly_income, stocks
from tasks.create_psql_db import create_table_1, create_table_2, create_table_3, create_table_4



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

    

    # pipeline's overall structure
    start_pipeline >> group_API >> group_staging >> end_pipeline
    start_pipeline >> group_webscraping >> group_staging >> end_pipeline
    

get_stocks_data()


# stocks = ['AAPL', 'AMZN', 'MSFT']

# print(get_stock_statistics(stocks))
# def test():
#     all_data = []
#     for stock_syb in stocks:
#         stock = yf.Ticker(stock_syb)
#         df = stock.income_stmt.transpose()
#         df['Stock'] = stock_syb

#         all_data.append(df)

#     combined = pd.concat(all_data, ignore_index=True)
#     return combined[['Gross Profit', 'Total Revenue', 'Diluted EPS', 'Stock']]


# result = test()
# print(result)

