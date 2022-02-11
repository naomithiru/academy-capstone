import boto3
import base64
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from datetime import timedelta, datetime

def get_secret():

    secret_name = "snowflake/capstone/login"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    return client.get_secret_value(
            SecretId=secret_name
        )

secret = get_secret()
print(secret)

secretdict = json.loads(secret)



