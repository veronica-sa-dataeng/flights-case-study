import os
from datetime import datetime
from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from importlib import import_module
from airflow.operators.dummy_operator import DummyOperator
import logging

sf_queries = import_module("data-eng-dag-etl.queries")
m = import_module("data-eng-dag-etl.main")
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
        "owner": "Veronica Sa",
        "start_date": dates.days_ago(1),
        "email": ["veronica.s.dataeng@gmail.com"],
        "email_on_failure": True,
        "catchup": False,
        "email_on_retry": False,
        "max_active_runs": 1
    }

def creating_ddl():

    input_str = m.run_snowflake_queries(sf_queries.infer_schema_column_list,'get_records')
    column_name = input_str[0][0].split('|')
    logger.info('Column name in list without pipes')
    logger.info(column_name)

    def infer_column_type(column_name):
        if "DATE" in column_name or "TIME" in column_name:
            return "TIMESTAMP"
        elif "CANCELLED" in column_name or "DIVERTED" in column_name:
            return "BOOLEAN"
        else:
            return "VARCHAR"

    ddl = "CREATE OR REPLACE TABLE flights (\n"
    for col in column_name:
        column_type = infer_column_type(col)
        ddl += f"    {col} {column_type},\n"

    ddl = ddl.rstrip(",\n") + "\n);"
    m.run_snowflake_queries(ddl,'run')
    return 

def load_file_to_table():
    
    m.run_snowflake_queries(sf_queries.insert_csv_stage_table,'run')
    return 

def dim_tables():

# DIM AIRLINE
    count_before = m.run_snowflake_queries(sf_queries.dim_airline_count,'get_count')
    m.run_snowflake_queries(sf_queries.insert_dim_airline_table,'run')
    count_after = m.run_snowflake_queries(sf_queries.dim_airline_count,'get_count')
    logger.info(f"Row count for DIM_AIRLINE - Before: {count_before}, After: {count_after}")

# DIM AIRPORT
    count_before = m.run_snowflake_queries(sf_queries.dim_airport_count,'get_count')
    m.run_snowflake_queries(sf_queries.insert_dim_airport_table,'run')
    count_after = m.run_snowflake_queries(sf_queries.dim_airport_count,'get_count')
    logger.info(f"Row count for DIM_AIRLINE - Before: {count_before}, After: {count_after}")
    return 

def fact_table():
    count_before = m.run_snowflake_queries(sf_queries.fact_count,'get_count')
    m.run_snowflake_queries(sf_queries.insert_dim_fact_table,'run')
    count_after = m.run_snowflake_queries(sf_queries.fact_count,'get_count')
    logger.info(f"Row count for DIM_AIRLINE - Before: {count_before}, After: {count_after}")
    return 

dag = DAG(dag_id="data_eng_dag_etl", default_args=DEFAULT_ARGS, 
          schedule_interval= None,
            default_view='graph', 
            catchup=False,
          )
start = DummyOperator(
        task_id='start',
        dag=dag,
    )
creating_ddl_task = PythonOperator(
    task_id='creating_ddl_task',
    python_callable=creating_ddl,
    dag=dag,
)

load_file_to_table_task = PythonOperator(
    task_id='load_file_to_table_task',
    python_callable=load_file_to_table,
    dag=dag,
)
dim_tables_task = PythonOperator(
    task_id='dim_tables_task',
    python_callable=dim_tables,
    dag=dag,
)
fact_table_task = PythonOperator(
    task_id='fact_table_task',
    python_callable=fact_table,
    dag=dag,
)
end = DummyOperator(
        task_id='end',
        dag=dag,
    )
start >> creating_ddl_task >> load_file_to_table_task >> dim_tables_task>> fact_table_task>> end 
