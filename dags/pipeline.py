import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 1),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'catchup': False
}

# Define the DAG with a schedule to run hourly
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline with staging, fact/dim loads, and quality checks',
    schedule_interval='@hourly'
)

# Helpers - Placeholder for tasks
def load_to_redshift(task_name):
    def _load(**kwargs):
        logging.info(f"{task_name} placeholder - SQL execution not implemented.")
    return _load

# Tasks
begin_execution = PythonOperator(
    task_id='begin_execution',
    python_callable=load_to_redshift("Begin_execution"),
    dag=dag
)

stage_events = PythonOperator(
    task_id='stage_events',
    python_callable=load_to_redshift("Stage_events"),
    dag=dag
)

stage_songs = PythonOperator(
    task_id='stage_songs',
    python_callable=load_to_redshift("Stage_songs"),
    dag=dag
)

load_songplays_fact_table = PythonOperator(
    task_id='load_songplays_fact_table',
    python_callable=load_to_redshift("Load_songplays_fact_table"),
    dag=dag
)

load_user_dim_table = PythonOperator(
    task_id='load_user_dim_table',
    python_callable=load_to_redshift("Load_user_dim_table"),
    dag=dag
)

load_song_dim_table = PythonOperator(
    task_id='load_song_dim_table',
    python_callable=load_to_redshift("Load_song_dim_table"),
    dag=dag
)

load_artist_dim_table = PythonOperator(
    task_id='load_artist_dim_table',
    python_callable=load_to_redshift("Load_artist_dim_table"),
    dag=dag
)

load_time_dim_table = PythonOperator(
    task_id='load_time_dim_table',
    python_callable=load_to_redshift("Load_time_dim_table"),
    dag=dag
)

run_data_quality_checks = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=load_to_redshift("Run_data_quality_checks"),
    dag=dag
)

end_execution = PythonOperator(
    task_id='end_execution',
    python_callable=load_to_redshift("End_execution"),
    dag=dag
)

# DAG Dependencies
begin_execution >> [stage_events, stage_songs]
[stage_events, stage_songs] >> load_songplays_fact_table
load_songplays_fact_table >> [load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table]
[load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table] >> run_data_quality_checks
run_data_quality_checks >> end_execution