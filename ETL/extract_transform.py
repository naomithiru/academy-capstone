from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.sql.functions import to_timestamp, col

import boto3
import base64
import json

# #build a spark session

# spark = (SparkSession.builder.config("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.1.2')
#                             .config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#                             .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain").getOrCreate()
#         )

# df = spark.read.json("s3://dataminded-academy-capstone-resources/raw/open_aq/")

# #write locally to a file
# df.write.format('json').save("capstone.json")

spark = SparkSession.builder.getOrCreate()
df = spark.read.json("capstone.json")

#df.show()
#df.printSchema()
df_transformed = (df.select("city", 
                        "coordinates.latitude","coordinates.latitude", 
                        "country", 
                        "date.local", "date.utc", 
                        "entity", "isAnalysis", "isMobile", "location", "locationId", "parameter", "sensorType", "unit", "value")
                        .withColumn("local_time", to_timestamp("local", "yyyy_MM_dd HH_mm_ss"))
                        .withColumn("utc_time", to_timestamp("utc", "yyyy_MM_dd HH_mm_ss"))
                        .drop("local","utc")
                        )
# df.show()

# df_transformed.show()
# df_transformed.printSchema()


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

secretdict = json.loads(secret['SecretString'])

print(secretdict)


sfOptions = {
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

spark = (
    SparkSession.builder
    .config("net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1", 'net.snowflake:snowflake-jdbc:3.13.3')
    .getOrCreate()
)


(df_transformed
    .write
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(**sfOptions)
    .option("dbtable", 'naomi_air_quality').mode("overwrite").save()
)

