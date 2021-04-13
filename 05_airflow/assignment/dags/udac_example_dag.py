from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner' : 'pathairs',
    'start_date' : datetime(2021, 4, 10, 6, 0, 0),
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry' : False
}

dag = DAG('sparkify_data_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly"
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    iam_role = 'arn:aws:iam::257533661337:role/myRedshiftRole',
    region = 'us-west-2',
    table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    json_path = 's3://udacity-dend/log_json_path.json',
    create_stmt = SqlQueries.staging_events_create
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    iam_role = 'arn:aws:iam::257533661337:role/myRedshiftRole',
    region = 'us-west-2',
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    create_stmt = SqlQueries.staging_songs_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    create_stmt = SqlQueries.songplay_table_create,
    insert_stmt = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'users',
    create_stmt = SqlQueries.user_table_create,
    insert_stmt = SqlQueries.user_table_insert,
    is_delete_load = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'songs',
    create_stmt = SqlQueries.song_table_create,
    insert_stmt = SqlQueries.song_table_insert,
    is_delete_load = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'artists',
    create_stmt = SqlQueries.artist_table_create,
    insert_stmt = SqlQueries.artist_table_insert,
    is_delete_load = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'time',
    create_stmt = SqlQueries.time_table_create,
    insert_stmt = SqlQueries.time_table_insert,
    is_delete_load = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    dq_checks = {
        "SELECT COUNT(*) FROM users WHERE userid IS NULL" : 0,
        "SELECT COUNT(*) FROM songs WHERE songid IS NULL" : 0,
        "SELECT COUNT(*) FROM artists WHERE artistid IS NULL" : 0,
        "SELECT COUNT(*) FROM time WHERE start_time IS NULL" : 0,
        "SELECT COUNT(*) FROM songplays WHERE playid IS NULL" : 0
    }
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG ORDER
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
