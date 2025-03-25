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
hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

DEFAULT_ARGS = {
        "owner": "Veronica Sa",
        "start_date": dates.days_ago(1),
        "email": ["veronica.s.dataeng@gmail.com"],
        "email_on_failure": True,
        "catchup": False,
        "email_on_retry": False,
        "max_active_runs": 1
    }

def snowflake_connection(command):
    result = hook.get_first(command)
    return result 

def creating_ddl():

    query = """
    SELECT *
    FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz'
      , FILE_FORMAT=>'infer_schema_format'
      )
    );
    """
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

    logger.info('Final DDL:')
    logger.info(ddl)
    hook.run(ddl)
    return ddl 

def load_file_to_table():
    
    copy_sql = """
        COPY INTO CANDIDATE_00184.FLIGHTS
        FROM @PUBLIC.S3_FOLDER/flights.gz
        FILE_FORMAT = csv_format;
    """
    
    hook.run(copy_sql)

def dim_tables():

    count_before = hook.run("SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE")[0][0]
    query = """
    INSERT INTO RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE 
    SELECT  
        ROW_NUMBER() OVER (ORDER BY "AIRLINECODE") AS "AIRLINE_ID",
        "AIRLINECODE", 
        "AIRLINENAME"      
    FROM 
        (SELECT DISTINCT 
            "AIRLINECODE", 
            SUBSTRING("AIRLINENAME", 1, POSITION(':' IN "AIRLINENAME") - 1) AS "AIRLINENAME"
        FROM RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS
        ) AS DISTINCT_FLIGHTS;
    
    """
    hook.run(query)
    count_after = hook.run("SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE")[0][0]
    logger.info(f"Row count for DIM_AIRLINE: {count_after - count_before}")

    count_before = hook.run("SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT")[0][0]
    query = """
    INSERT INTO RECRUITMENT_DB.CANDIDATE_00184."DIM_AIRPORT"
    SELECT 
        ROW_NUMBER() OVER (ORDER BY "AIRPORTNAME") AS "AIRPORT_ID",
        "AIRPORTCODE",
        SUBSTRING("AIRPORTNAME", 1, LENGTH("AIRPORTNAME") - 2) AS "AIRPORTNAME",
        "CITYNAME",
        CASE 
            WHEN "STATECODE" IS NULL 
                THEN  RIGHT("AIRPORTNAME",2)
            ELSE 
                "STATECODE"
        END AS "STATECODE",
        CASE 
            WHEN "STATENAME" IS NULL AND RIGHT("AIRPORTNAME",2) = 'KS' THEN 'Kansas'
            WHEN "STATENAME" IS NULL AND RIGHT("AIRPORTNAME",2) = 'OK' THEN 'Oklahoma'
            ELSE "STATENAME"
        END AS "STATENAME"
    FROM (
        SELECT 
            SUBSTRING(ORIGAIRPORTNAME, 1, POSITION(':' IN ORIGAIRPORTNAME) - 1) AS "AIRPORTNAME",
            ORIGINAIRPORTCODE AS "AIRPORTCODE",
            ORIGINCITYNAME AS "CITYNAME",
            ORIGINSTATE AS "STATECODE",
            ORIGINSTATENAME AS "STATENAME"
        FROM RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS
        UNION ALL
        SELECT  
            SUBSTRING(DESTAIRPORTNAME, 1, POSITION(':' IN DESTAIRPORTNAME) - 1) AS "AIRPORTNAME",
            DESTAIRPORTCODE AS "AIRPORTCODE",
            DESTCITYNAME AS "CITYNAME",
            DESTSTATE AS "STATECODE",
            DESTSTATENAME AS "STATENAME"
        FROM RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS
        ) as TEMP 
        GROUP BY "AIRPORTNAME","AIRPORTCODE","CITYNAME","STATECODE","STATENAME"
    """
    hook.run(query)
    count_after = hook.run("SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT")[0][0]
    logger.info(f"Row count for DIM_AIRPORT: {count_after - count_before}")
    
    return 

def fact_table():

    count_before = hook.run("SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.FACT_FLIGHTS")[0][0]
    query = """
    INSERT INTO "RECRUITMENT_DB"."CANDIDATE_00184"."FACT_FLIGHTS" 
    SELECT 
        ROW_NUMBER() OVER (ORDER BY "TRANSACTIONID") AS "FLIGHT_ID",
        flatfile."TRANSACTIONID",
        flatfile."FLIGHTDATE",
        flatfile."TAILNUM",
        flatfile."FLIGHTNUM",
        flatfile."CANCELLED",
        flatfile."DEPDELAY",
        flatfile."ARRDELAY",
        flatfile."CRSDEPTIME",
        flatfile."DEPTIME",
        flatfile."CRSARRTIME",
        flatfile."ARRTIME",
        flatfile."WHEELSOFF",
        flatfile."WHEELSON",
        orig."AIRPORT_ID" AS "ORIGAIRPORTID",
        dest."AIRPORT_ID" AS "DESTAIRPORTID",
        airline."AIRLINE_ID",
        flatfile."TAXIOUT",
        flatfile."TAXIIN",
        flatfile."CRSELAPSEDTIME",
        flatfile."ACTUALELAPSEDTIME",
        flatfile."DIVERTED",
        flatfile."DISTANCE",
        CASE
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 0 AND 100 THEN '0-100 miles'
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 101 AND 200 THEN '101-200 miles'
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 201 AND 300 THEN '201-300 miles'
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 301 AND 400 THEN '301-400 miles'
            WHEN CAST(SUBSTRING("DISTANCE", 1, LENGTH("DISTANCE") - 6) AS INTEGER) BETWEEN 401 AND 500 THEN '401-500 miles'
        ELSE '500+ miles'
        END as "DISTANCEGROUP",
        CASE
            WHEN DATE("ARRTIME") > DATE("DEPTIME") THEN 1
        ELSE 0 
        END AS "NEXTDAYARR",
        CASE
            WHEN CAST("DEPDELAY" AS INTEGER) > 15 THEN 1
        ELSE 0 
        END AS "DEPDELAYGT15"
    FROM RECRUITMENT_DB.CANDIDATE_00184.FLIGHTS flatfile
    INNER JOIN RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRLINE airline 
    on flatfile.airlinecode = airline."AIRLINECODE"
    LEFT JOIN RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT orig 
    ON flatfile.ORIGINAIRPORTCODE = orig.airportcode
    LEFT JOIN RECRUITMENT_DB.CANDIDATE_00184.DIM_AIRPORT dest 
    ON flatfile.DESTAIRPORTCODE = dest.airportcode
    """
    hook.run(query)
    count_after = hook.run("SELECT COUNT(*) FROM RECRUITMENT_DB.CANDIDATE_00184.FACT_FLIGHTS")[0][0]
    logger.info(f"Row count for FACT_FLIGHTS: {count_after - count_before}")
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
