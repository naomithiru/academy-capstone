# from airflow import DAG
# from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator

# import boto3


# dag = DAG(
#     dag_id="my_dag",
#     description="trigger-batch-job",
#     default_args={"owner": "Airflow"},
#     schedule_interval="@once",
#     start_date=dt.datetime(2021, 1, 1),
# )

# batch_params = ["",""]

# run_aws_batch = AWSBatchOperator(
#     task_id='run_aws_batch',
#     aws_conn_id='AKIAU5YMQWRQXDRKL44S',
#     job_name='aws_batch_job',
#     job_definition='arn:aws:batch:eu-west-1:338791806049:job-definition/Naomi_batch_revised:2',
#     job_queue='arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-winter-2022-job-queue',
#     overrides={'command': batch_params},
#     dag=dag
# )

# s3 = boto3.resource('s3')

# response = s3.create_bucket(
#     Bucket='s3://dataminded-academy-capstone-resources',
#     CreateBucketConfiguration={
#         'LocationConstraint': 'eu-west-1'
#     }
# )

# print(response)

import boto3

client = boto3.client('batch', region_name='eu-west-1')

response = client.submit_job(
    jobDefinition='arn:aws:batch:eu-west-1:338791806049:job-definition/Naomi_batch_revised:2',
    jobName='batch_job_1',
    jobQueue='arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-winter-2022-job-queue',
    containerOverrides={},
)

print(response)

# DAGs folder: s3://dataminded-academy-capstone-resources/Naomi/dags


