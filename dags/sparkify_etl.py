import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator,
                                PostgresOperator)

import sql_insert_statements
import sql_chunks

s3_bucket = 'udacity-dend'

default_args = {
    'owner': 'nate',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
}

dag = DAG('simply_sparkify_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # run once a day, in line with the data frequency
          schedule_interval='0 0 * * *',
          # we are deleting data after each run in the staging table, so can't run the next ones until one has finished
          max_active_runs=1
        )

log_s3_key = 'log_data/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json'
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket=s3_bucket,
    s3_key=log_s3_key,
    table='staging_events',
    json_config='s3://udacity-dend/log_json_path.json',
    execution_date='{{execution_date}}'
)

# the way the fact/dimension table should be done is with simple SQL and
# PostgresOperator.  There is no reason to overcomplicate it.

load_songplays_table = PostgresOperator(
    task_id="load_songplays_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_insert_statements.songplay_table_insert
)

load_user_dimension_table = PostgresOperator(
    task_id="load_users_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_insert_statements.user_table_insert
)

load_time_dimension_table = PostgresOperator(
    task_id="load_time_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_insert_statements.time_table_insert
)

stage_events_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_user_dimension_table
stage_events_to_redshift >> load_time_dimension_table
