#! /usr/bin/python

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False,   
    'depends_on_past': False,
}

# dag definition
dag = DAG('dag_no_catchup_no_past_runs',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          schedule_interval='0 * * * *',    # Means, run every hour at 0th minute - Similar to @hourly
          
        )

# Dummy operator to mark start
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_bucket="udacity-dend",
    # Since we are using no-catchup, no past runs setting, we cannot load based on execution date as this will duplicate data in Dimensions during multiple parallel runs.
    s3_key="log_data/2018/11/",
    s3_json_key="log_json_path.json"
)

# Custom Operator to Stage data from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/"
)

# Custom Operator to Load songplays dimension
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)

# Custom Operator to Load user dimension
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    sql=SqlQueries.user_table_insert
)

# Custom Operator to Load song dimension
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    sql=SqlQueries.song_table_insert
)

# Custom Operator to Load artist dimension
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    sql=SqlQueries.artist_table_insert
)

# Custom Operator to Load time dimension
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    sql=SqlQueries.time_table_insert
)

# Data Quality Operator - Used to run Data quality checks
# The follwing Data Quality rules are checked here:
# Tables should not be empty (Row count should not be zero)
# There should be duplicates in users table based on user_id
# There should be any NULL values on user_id column in users table
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table=["songs","artists","users","time"],
    test_cases=[{'name':'user_table_duplicate_check', 'sql':SqlQueries.user_table_duplicate_check, 'exp_result':0}, 
                {'name':'user_table_userid_null_check', 'sql':SqlQueries.user_table_userid_null_check, 'exp_result':0}]
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

# Task Flow
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
