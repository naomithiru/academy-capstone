from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator

import boto3

#client = boto3.client('batch')

dag = DAG(
    dag_id="my_dag",
    description="trigger-batch-job",
    default_args={"owner": "Airflow"},
    schedule_interval="@once",
    start_date=dt.datetime(2021, 1, 1),
)

batch_params = ["",""]

run_aws_batch = AWSBatchOperator(
    task_id='run_aws_batch',
    aws_conn_id='AKIAU5YMQWRQXDRKL44S',
    job_name='aws_batch_job',
    job_definition='arn:aws:batch:eu-west-1:338791806049:job-definition/Naomi_batch_revised:2',
    job_queue='arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-winter-2022-job-queue',
    overrides={'command': batch_params},
    dag=dag
)