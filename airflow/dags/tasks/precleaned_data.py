from airflow.providers.postgres.operators.postgres import PostgresOperator


def create_date_format_task(tablenames):
    tasks = []
    for tablename in tablenames:

        task = PostgresOperator(
            task_id = f"tsk_format_dates_{tablename}",
            postgres_conn_id = "postgres_conn",
            sql = f"""
                ALTER TABLE {tablename}
                ADD COLUMN IF NOT EXISTS formatted_date DATE;

                UPDATE {tablename}
                SET "formatted_date" = CAST("Date" AS DATE);
            """
        )
        tasks.append(task)

    return tasks

def remove_null_values(tablenames):
    tasks = []
    for tablename in tablenames:

        task = PostgresOperator(
            task_id = f"tsk_remove_null_values_{tablename}",
            postgres_conn_id = "postgres_conn",
            sql = f"""
                DELETE FROM {tablename}
                WHERE COALESCE("Gross Profit", "Total Revenue", "Diluted EPS") IS NULL;
            """

        )
        tasks.append(task)

    return tasks


def remove_data_web_duplicates(tablename):

    task = PostgresOperator(
        task_id = f"tsk_remove_duplicates_{tablename}",
        postgres_conn_id = "postgres_conn",
        sql = f"""
            DELETE FROM {tablename} 
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM {tablename}
                GROUP BY "Stock"
            );
        """
    )

    return task


def remove_stocks_api_duplicates(tablenames):
    tasks = []
    for tablename in tablenames:

        task = PostgresOperator(
            task_id = f"tsk_remove_duplicates_{tablename}",
            postgres_conn_id = "postgres_conn",
            sql = f"""
                DELETE FROM {tablename} 
                WHERE ctid NOT IN (
                    SELECT MIN(ctid)
                    FROM {tablename}
                    GROUP BY "Date", "Stock"
                );
            """
        )
        tasks.append(task)

    return tasks