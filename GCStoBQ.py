import os

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


from airflow.utils.dates import days_ago

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'etltest')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'custom_table')

dag = models.DAG(
    dag_id='example_ETL',
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
)

create_test_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
)

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery_example',
    bucket='bucketname',
    source_objects=['/student_sample_data.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'marks', 'type': 'INTEGER', 'mode': 'NULLABLE'},
         {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

transform_bq = BigQueryInsertJobOperator(
    task_id="load_data_into_new_table",
    configuration={
        "query": {
            "query": """
            CREATE OR REPLACE TABLE `project-name.etltest.custom_table_results`
            AS
            SELECT 
            name,marks,gender 
            FROM `project-name.etltest.custom_table` 
            WHERE marks>80
            ORDER BY gender
            """,
            "useLegacySql": False
        }
    },
    dag=dag
)

create_test_dataset >> load_csv >> transform_bq

