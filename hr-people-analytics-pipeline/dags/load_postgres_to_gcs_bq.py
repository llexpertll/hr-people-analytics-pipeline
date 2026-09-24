import pandas as pd
from functools import reduce
from datetime import timedelta
import datetime as date


from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from google.cloud import storage
from airflow.operators.python import PythonOperator


GBQ_DS = 'hr'
GCP_PROJECT = 'kmitl-is'
GCS_BUCKET = 'data-lake-hr'
GCS_OBJECT_PATH = 'employee'

SOURCE_TABLE_NAME1 = 'employee_all'
SOURCE_TABLE_NAME1_Final = 'employee'
SOURCE_TABLE_NAME2 = 'employee_address'
SOURCE_TABLE_NAME3 = 'employee_attrition'
SOURCE_TABLE_NAME4 = 'employee_education'
SOURCE_TABLE_NAME5 = 'employee_leave'
SOURCE_TABLE_NAME6 = 'employee_performance_rating'

POSTGRESS_CONN_ID = 'postgres_default'
GCP_CONN_ID = 'google_cloud_default'
FILE_FORMAT = 'csv'


def transform_emp_data():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET, GCP_PROJECT)
    path1 = f'gs://{GCS_BUCKET}/{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME1}.{FILE_FORMAT}'
    path2 = f'gs://{GCS_BUCKET}/{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME2}.{FILE_FORMAT}'
    path4 = f'gs://{GCS_BUCKET}/{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME4}.{FILE_FORMAT}'

    # Read CSV from GCS to Dataframe
    df_emp_tmp = pd.read_csv(path1)
    df_address_tmp = pd.read_csv(path2)
    df_education_tmp = pd.read_csv(path4)

    df_emp = df_emp_tmp[['x_emp_id','name','active','department_id','dept_name','job_id','job_title','company_id','company_name']]
    df_address = df_address_tmp[['x_emp_id','private_street','private_street2','private_city','private_state_id','state_name','private_zip','private_country_id','country_name']]
    df_education = df_education_tmp[['x_emp_id','certificate','study_field','study_school']]

    # Merge DF
    data_frames = [df_emp, df_education, df_address]
    df_merged = reduce(lambda  left, right: pd.merge(left, right, on=['x_emp_id'], how='left'), data_frames)

    # Save CSV to airflow/data
    path_output = f'gs://{GCS_BUCKET}/{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME1_Final}.{FILE_FORMAT}'
    df_merged.to_csv(path_output, index=False)


## DAG Initialization

with DAG(
    dag_id = 'load_postgres_to_gcs_bq',
    start_date = days_ago(1),
    end_date = date.datetime(2024, 4, 30),
    default_args = {
        'owner': 'surasak',
        'retries': 5,
        'retry_delay': timedelta(minutes=5),
    },
    # schedule_interval = "*/5 * * * *", # every 5 min
    # schedule_interval = "@once",
    schedule_interval='@hourly',
    # max_active_runs = 1,
    tags = ["hr, is2, kmitl"]

) as dag:

    start_pipeline_task = DummyOperator(
        task_id = 'start_pipeline',
    )

    ## ============= 10:Extract =============
    ## Task to transfer data from PostgreSQL to GCS

    postgres_to_gcs_emp_all = PostgresToGCSOperator(
        task_id = f'postgres_to_gcs_emp_all',
        postgres_conn_id = POSTGRESS_CONN_ID,
        sql = f'SELECT * FROM {SOURCE_TABLE_NAME1};',
        bucket=GCS_BUCKET,
        filename = f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME1}.{FILE_FORMAT}',
        export_format = 'csv',
        gzip = False,
        use_server_side_cursor = False,
    )

    postgres_to_gcs_emp_address = PostgresToGCSOperator(
        task_id = f'postgres_to_gcs_emp_address',
        postgres_conn_id = POSTGRESS_CONN_ID,
        sql = f'SELECT * FROM {SOURCE_TABLE_NAME2};',
        bucket=GCS_BUCKET,
        filename = f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME2}.{FILE_FORMAT}',
        export_format = 'csv',
        gzip = False,
        use_server_side_cursor = False,
    )

    postgres_to_gcs_emp_attrition = PostgresToGCSOperator(
        task_id = f'postgres_to_gcs_emp_attrition',
        postgres_conn_id = POSTGRESS_CONN_ID,
        sql = f'SELECT * FROM {SOURCE_TABLE_NAME3};',
        bucket=GCS_BUCKET,
        filename = f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME3}.{FILE_FORMAT}',
        export_format = 'csv',
        gzip = False,
        use_server_side_cursor = False,
    )

    postgres_to_gcs_emp_education = PostgresToGCSOperator(
        task_id = f'postgres_to_gcs_emp_education',
        postgres_conn_id = POSTGRESS_CONN_ID,
        sql = f'SELECT * FROM {SOURCE_TABLE_NAME4};',
        bucket=GCS_BUCKET,
        filename = f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME4}.{FILE_FORMAT}',
        export_format = 'csv',
        gzip = False,
        use_server_side_cursor = False,
    )

    postgres_to_gcs_emp_leave = PostgresToGCSOperator(
        task_id = f'postgres_to_gcs_emp_leave',
        postgres_conn_id = POSTGRESS_CONN_ID,
        sql = f'SELECT * FROM {SOURCE_TABLE_NAME5}',
        bucket=GCS_BUCKET,
        filename = f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME5}.{FILE_FORMAT}',
        export_format = 'csv',
        gzip = False,
        use_server_side_cursor = False,
    )

    postgres_to_gcs_emp_performance = PostgresToGCSOperator(
        task_id = f'postgres_to_gcs_emp_performance',
        postgres_conn_id = POSTGRESS_CONN_ID,
        sql = f'SELECT * FROM {SOURCE_TABLE_NAME6};',
        bucket=GCS_BUCKET,
        filename = f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME6}.{FILE_FORMAT}',
        export_format = 'csv',
        gzip = False,
        use_server_side_cursor = False,
    )
 
    ## ============= 20:Tranform =============
    transform_emp_data = PythonOperator(
        task_id = 'transform_emp_data',
        python_callable = transform_emp_data,
    )

    transform_emp_atrrtion = DummyOperator(
        task_id = 'transform_emp_atrrtion',
    )

    ## ============= 30:Load =============
    ## Task to transfer data to BigQuery
    
    gcs_to_bq_employee = GCSToBigQueryOperator(
        task_id = f'gcs_to_bq_employee',
        bucket = GCS_BUCKET,
        source_objects = [f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME1_Final}.{FILE_FORMAT}'],
        destination_project_dataset_table = '.'.join([GCP_PROJECT, GBQ_DS, SOURCE_TABLE_NAME1_Final]),
        # schema_fields = schema,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
        skip_leading_rows = 1,
        allow_quoted_newlines = True,
        autodetect=True,
    )

    gcs_to_bq_emp_attrition = GCSToBigQueryOperator(
        task_id = f'gcs_to_bq_emp_attrition',
        bucket = GCS_BUCKET,
        source_objects = [f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME3}.{FILE_FORMAT}'],
        destination_project_dataset_table = '.'.join([GCP_PROJECT, GBQ_DS, SOURCE_TABLE_NAME3]),
        # schema_fields = schema,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
        skip_leading_rows = 1,
        allow_quoted_newlines = True,
        autodetect=True,
    )

    gcs_to_bq_emp_leave = GCSToBigQueryOperator(
        task_id = f'gcs_to_bq_emp_leave',
        bucket = GCS_BUCKET,
        source_objects = [f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME5}.{FILE_FORMAT}'],
        destination_project_dataset_table = '.'.join([GCP_PROJECT, GBQ_DS, SOURCE_TABLE_NAME5]),
        # schema_fields = schema,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
        skip_leading_rows = 1,
        allow_quoted_newlines = True,
        autodetect=True,
    )

    gcs_to_bq_emp_performance = GCSToBigQueryOperator(
        task_id = f'gcs_to_bq_emp_performance',
        bucket = GCS_BUCKET,
        source_objects = [f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME6}.{FILE_FORMAT}'],
        destination_project_dataset_table = '.'.join([GCP_PROJECT, GBQ_DS, SOURCE_TABLE_NAME6]),
        # schema_fields = schema,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
        skip_leading_rows = 1,
        allow_quoted_newlines = True,
        autodetect=True,
    )

    finish_pipeline_task = DummyOperator(
        task_id = 'finish_pipeline',
    )

    ## ============= 40: Ran DAGs =============
    start_pipeline_task >> [postgres_to_gcs_emp_all, postgres_to_gcs_emp_address, postgres_to_gcs_emp_education] >> transform_emp_data >> gcs_to_bq_employee >> finish_pipeline_task
    start_pipeline_task >> postgres_to_gcs_emp_attrition >> transform_emp_atrrtion >> gcs_to_bq_emp_attrition >> finish_pipeline_task
    start_pipeline_task >> postgres_to_gcs_emp_leave >> gcs_to_bq_emp_leave >> finish_pipeline_task
    start_pipeline_task >> postgres_to_gcs_emp_performance >> gcs_to_bq_emp_performance >> finish_pipeline_task

  