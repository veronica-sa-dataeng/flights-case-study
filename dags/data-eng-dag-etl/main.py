import logging
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

logger = logging.getLogger(__name__)

hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

def snowflake_connection(command):
    result = hook.get_first(command)
    return result 

def run_snowflake_queries(query,type):
    if type == 'get_records':
        input_str = hook.get_records(query)
        logger.info(input_str)
        return input_str  
    elif type == 'run':
        hook.run(query)
        logger.info(query)
        return None  
    elif type == 'get_count':
        count = hook.get_records(query)[0][0]
        logger.info(count)
        return count 