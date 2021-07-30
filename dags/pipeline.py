from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator

from loader_subdag import loader_subdag

from helpers.sql_queries import SqlQueries

s3_bucket = 'mot-data-ude'
item_s3_key = "df_item_json"
result_s3_key = "df_result"
retries = 3 # Number of retries to be attempted if the DAG fails
retry_delay_minutes = 5 # Delay in minutes before retry is attempted

default_args = {
    'owner': 'sama_26@hotmail.com',
    'start_date': datetime(2021, 7, 30),
    'depends_on_past':False,
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':retries,
    'retry_delay':timedelta(minutes=retry_delay_minutes),
    'catchup':False
    }

dag = DAG('mot_data_pipeline',
    default_args=default_args,
    description='ETL MOT data from S3 into redshift',
    schedule_interval='@monthly',
    max_active_runs = 1
    )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

stage_results_to_redshift = StageToRedshiftOperator(
    task_id='Stage_results',
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    table="staging_results",
    s3_bucket = s3_bucket,
    s3_key = result_s3_key,
    file_format = "csv",
    dag=dag,
    )

stage_items_to_redshift = StageToRedshiftOperator(
    task_id='Stage_items',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    table="staging_items",
    s3_bucket = s3_bucket,
    s3_key = item_s3_key,
    file_format = "json",
    )

load_test_result_table = LoadFactOperator(
    task_id='Load_test_result_fact_table',
    redshift_conn_id = 'redshift',
    create_sql_stmt = SqlQueries.test_result_table_insert,
    dag=dag
	)

load_test_item_dimension_table = SubDagOperator(
    subdag=loader_subdag(
        parent_dag_name='mot_data_pipeline',
        task_id="load_test_item_dim_table",
        redshift_conn_id="redshift",
        table="test_item",
        create_sql_stmt=SqlQueries.test_item_table_insert,
        start_date=default_args['start_date'],
        replace = True
    ),
    task_id='Load_test_item_dim_table',
    dag=dag,
)

load_vehicle_dimension_table = SubDagOperator(
    task_id='Load_vehicle_dim_table',
    dag=dag,
    subdag=loader_subdag(
        parent_dag_name='mot_data_pipeline',
        task_id="Load_vehicle_dim_table",
        redshift_conn_id="redshift",
        table="vehicle",
        create_sql_stmt=SqlQueries.vehicle_table_insert,
        start_date=default_args['start_date'],
        replace = True
    	)
	)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task

create_tables_task >> Stage_results_to_redshift
create_tables_task >> stage_items_to_redshift

stage_results_to_redshift >> load_test_result_table
stage_items_to_redshift >> load_test_result_table

load_test_result_table >> load_test_item_dimension_table
load_test_result_table >> load_vehicle_dimension_table

load_vehicle_dimension_table >> run_quality_checks
load_test_item_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
