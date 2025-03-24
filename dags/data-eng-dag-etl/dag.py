import os
from datetime import datetime
from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from importlib import import_module
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import zipfile
import pandas as pd
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
        "owner": "Veronica Sa",
        "start_date": dates.days_ago(1),
        "email": ["celulardaveve@gmail.com"],
        "email_on_failure": True,
        "catchup": False,
        "email_on_retry": False,
        "max_active_runs": 1
    }

def snowflake_connection(command):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    result = hook.get_first(command)
    return result 

def creating_ddl():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    query = """
    SELECT *
    FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz'
      , FILE_FORMAT=>'infer_schema_format'
      )
    );
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    input_str = hook.get_records(query)
    logger.info('String with all columns')
    logger.info(input_str)

    if not input_str:
        raise ValueError("❌ No files found")
    
    column_name = input_str[0][0].split('|')
    logger.info('Column name in list without pipes')
    logger.info(column_name)

    def infer_column_type(column_name):
        if "DATE" in column_name or "TIME" in column_name:
            return "DATE"
        elif "CANCELLED" in column_name or "DIVERTED" in column_name:
            return "BOOLEAN"
        else:
            return "VARCHAR"

    ddl = "CREATE OR REPLACE TABLE flights (\n"
    for col in column_name:
        column_type = infer_column_type(col)
        ddl += f"    {col} {column_type},\n"

    ddl = ddl.rstrip(",\n") + "\n);"

    logger.info('Final DDL:')
    logger.info(ddl)
    hook.run(ddl)
    return ddl 

def load_file_to_table():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    
    copy_sql = """
        COPY INTO CANDIDATE_00184.FLIGHTS
        FROM @PUBLIC.S3_FOLDER/flights.gz
        FILE_FORMAT = csv_format;
    """
    
    hook.run(copy_sql)


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

end = DummyOperator(
        task_id='end',
        dag=dag,
    )
start >> creating_ddl_task >> load_file_to_table_task >>  end 