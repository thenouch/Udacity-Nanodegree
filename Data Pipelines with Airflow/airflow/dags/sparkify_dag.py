from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'nicolas',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry': False,
    'catchup'=False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
          
        )

# start operator that runs the initial step in the dag
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# connect to the log path
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json = 's3://udacity-dend/log_json_path.json'
)

#connect to the log path
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    table='staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key='song_data',
    json='auto'
)

#songplay_table_insert
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    sql = SqlQueries.songplay_table_insert,
    
    
)

#user_table_insert
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id ='redshift',
    table ='users',
    sql=SqlQueries.user_table_insert,
    append = False
)

#song_table_insert
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table='songs',
    sql=SqlQueries.song_table_insert,
    append = False
    
)

#artist_table_insert
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id ='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert,
    append=False
)

#time_table_insert
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id ='redshift',
    table='time',
    sql=SqlQueries.time_table_insert,
    append = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables = ['songplays','users','songs','artists','time']
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


"""
Start sequence of dependencies
"""

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

