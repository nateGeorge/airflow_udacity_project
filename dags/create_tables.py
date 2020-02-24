import logging
from datetime import datetime

from airflow import DAG
from airflow.operators import PythonOperator, StageToRedshiftOperator, PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

import sql_insert_statements

# kwargs dict for python callables
op_kwargs = {'redshift_conn_id': 'redshift'}

default_args = {
    'owner': 'nate'
}

dag = DAG('simply_create_all_tables',
          default_args=default_args,
          description='Creates all Sparkify tables in Redshift with Airflow',
          start_date=datetime(2020, 2, 20),
          schedule_interval=None
        )


def create_staging_events_table(redshift_conn_id='redshift'):
    redshift_hook = PostgresHook(redshift_conn_id)

    table = 'staging_events'

    logging.info(f'dropping {table} table')
    redshift_hook.run(f'DROP TABLE IF EXISTS public.{table};')

    logging.info(f'creating {table} table')

    sql = f"""CREATE TABLE IF NOT EXISTS {table}
        (artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT);
    """
    redshift_hook.run(sql)


def create_staging_songs_table(redshift_conn_id='redshift'):
    redshift_hook = PostgresHook(redshift_conn_id)

    table = 'staging_songs'

    logging.info(f'dropping {table} table')
    redshift_hook.run(f'DROP TABLE IF EXISTS public.{table};')

    logging.info(f'creating {table} table')

    sql = f"""CREATE TABLE IF NOT EXISTS {table}
        (num_songs INT,
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year INT);
    """

    redshift_hook.run(sql)


def create_artists_table(redshift_conn_id='redshift'):
    redshift_hook = PostgresHook(redshift_conn_id)
    table = 'artists'

    logging.info(f'dropping {table} table')
    redshift_hook.run(f'DROP TABLE IF EXISTS public.{table};')

    logging.info(f'creating {table} table')

    sql = f"""CREATE TABLE IF NOT EXISTS {table}
        (artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC);
    """

    redshift_hook.run(sql)


def create_songs_table(redshift_conn_id='redshift'):
    redshift_hook = PostgresHook(redshift_conn_id)

    table = 'songs'

    logging.info(f'dropping {table} table')
    redshift_hook.run(f'DROP TABLE IF EXISTS public.{table};')

    logging.info(f'creating {table} table')

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table}
    (song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration NUMERIC);
    """

    redshift_hook.run(sql)


def create_users_table(redshift_conn_id='redshift'):
    redshift_hook = PostgresHook(redshift_conn_id)

    table = 'users'

    logging.info(f'dropping {table} table')
    redshift_hook.run(f'DROP TABLE IF EXISTS public.{table};')

    logging.info(f'creating {table} table')

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table}
    (user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR);
    """

    redshift_hook.run(sql)


def create_time_table(redshift_conn_id='redshift'):
    redshift_hook = PostgresHook(redshift_conn_id)

    table = 'time'

    logging.info(f'dropping {table} table')
    redshift_hook.run(f'DROP TABLE IF EXISTS public.{table};')

    logging.info(f'creating {table} table')

    sql = f"""CREATE TABLE IF NOT EXISTS {table}
        (time_id INT IDENTITY(0, 1) PRIMARY KEY,
        start_time TIMESTAMP,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT);
    """

    redshift_hook.run(sql)


def create_songplays_table(redshift_conn_id='redshift'):
    redshift_hook = PostgresHook(redshift_conn_id)

    table = 'songplays'

    logging.info(f'dropping {table} table')
    redshift_hook.run(f'DROP TABLE IF EXISTS public.{table};')

    logging.info(f'creating {table} table')

    sql = f"""CREATE TABLE IF NOT EXISTS {table}
        (songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR);
    """

    redshift_hook.run(sql)


create_staging_events_task = PythonOperator(
    task_id='create_staging_events_table',
    dag=dag,
    python_callable=create_staging_events_table,
    op_kwargs=op_kwargs
)

create_staging_songs_task = PythonOperator(
    task_id='create_staging_songs_table',
    dag=dag,
    python_callable=create_staging_songs_table,
    op_kwargs=op_kwargs
)

create_artists_task = PythonOperator(
    task_id='create_artists_table',
    dag=dag,
    python_callable=create_artists_table,
    op_kwargs=op_kwargs
)

create_songs_task = PythonOperator(
    task_id='create_songs_table',
    dag=dag,
    python_callable=create_songs_table,
    op_kwargs=op_kwargs
)

create_users_task = PythonOperator(
    task_id='create_users_table',
    dag=dag,
    python_callable=create_users_table,
    op_kwargs=op_kwargs
)

create_time_task = PythonOperator(
    task_id='create_time_table',
    dag=dag,
    python_callable=create_time_table,
    op_kwargs=op_kwargs
)

create_songplays_task = PythonOperator(
    task_id='create_songplays_table',
    dag=dag,
    python_callable=create_songplays_table,
    op_kwargs=op_kwargs
)

# Note: This can be done with PostgresOperators like so.
# You may put the SQL in another .py file and import it, too.
# from airflow.operators import PostgrosOperator
# sp_table = 'songplays'
# drop_table_sql = f'DROP TABLE IF EXISTS public.{};'
#
# create_sp_table_sql = f"""CREATE TABLE public.{} (
# playid varchar(32) NOT NULL,
# start_time timestamp NOT NULL,
# userid int4 NOT NULL,
# "level" varchar(256),
# songid varchar(256),
# artistid varchar(256),
# sessionid int4,
# location varchar(256),
# user_agent varchar(256),
# CONSTRAINT songplays_pkey PRIMARY KEY (playid)
# );
# """
#
# drop_songplays = PostgresOperator(
#     task_id="drop_songplays_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=drop_table_sql.format(sp_table),
# )
#
#
# drop_songplays = PostgresOperator(
#     task_id="create_songplays_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=create_sp_table_sql.format(sp_table),
# )

# The songs don't change and take a long time to load (~5m), so only load them once.
s3_bucket = 'udacity-dend'
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket=s3_bucket,
    s3_key='song_data',
    table='staging_songs',
    execution_date='{execution_date}'
)

load_song_dimension_table = PostgresOperator(
    task_id="load_song_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_insert_statements.song_table_insert
)

load_artist_dimension_table = PostgresOperator(
    task_id="load_artist_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_insert_statements.artist_table_insert
)


# can't load into tables before they're created
create_staging_songs_task >> stage_songs_to_redshift
create_songs_task >> load_song_dimension_table
create_artists_task >> load_artist_dimension_table

# can't load dim tables without the data
stage_songs_to_redshift >> load_song_dimension_table
stage_songs_to_redshift >> load_artist_dimension_table
