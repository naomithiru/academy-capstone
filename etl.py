from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Mapping
import boto3
import json

def get_spark() -> SparkSession:
    java_archives = (
        "org.apache.hadoop:hadoop-aws:3.1.2",
        "net.snowflake:spark-snowflake_2.12:2.9.3-spark_3.1",
        "net.snowflake:snowflake-jdbc:3.13.14"
        )
    return (
        SparkSession
        .builder
        .config('spark.jars.packages', ",".join(java_archives))
        .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
    )

def extract(location: str, spark: SparkSession) -> DataFrame:
    return spark.read.json(data_location)


def transform(frame: DataFrame) -> DataFrame:
    return (
        frame.withColumn("latitude", frame["coordinates.latitude"])
        .withColumn("longitude", frame["coordinates.longitude"])
        .drop("coordinates")
        .withColumn("local_date", frame["date.local"])
        .withColumn("utc_date", frame["date.utc"])
        .drop("date")
    )


def get_secret(name: str) -> Dict[str, str]:
    client = boto3.client('secretsmanager', region_name='eu-west-1')
    response = client.get_secret_value(SecretId=name)

    return json.loads(response['SecretString'])


def create_snowflake_connector_options(creds: Mapping[str, str]) -> Dict[str, str]:
    return {
        "sfURL": f"{creds['URL']}.snowflakecomputing.com",
        "sfPassword": creds["PASSWORD"],
        "sfUser": creds["USER_NAME"],
        "sfDatabase": creds["DATABASE"],
        "sfSchema": "NAOMI",  # replace this with a pre-existing schema
        "sfWarehouse": creds["WAREHOUSE"],
        "sfRole": creds["ROLE"],
        "dbtable": "naomi_air_quality"
    }


if __name__ == "__main__":
    data_location = "s3://dataminded-academy-capstone-resources/raw/open_aq/"
    spark = get_spark()
    frame = extract(data_location, spark)
    flat_frame = transform(frame)
    flat_frame.printSchema()
    flat_frame.show()
    
    secrets = get_secret("snowflake/capstone/login")
    options = create_snowflake_connector_options(secrets)

    (
        flat_frame
        .write
        .format("net.snowflake.spark.snowflake")
        .options(**options)
        .mode("overwrite")
        .save()
    )

    