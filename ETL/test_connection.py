from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from datetime import timedelta, datetime


sfOptions = {
  "sfURL" : "yw41113.eu-west-1.snowflakecomputing.com",
  "sfRole" : "ACADEMY_CLASS_ROLE",
  "sfUser" : "ACADEMY_CLASS_USER",
  "sfPassword" : ")07)a90c*g",
  "sfDatabase" : "ACADEMY_CLASS_CAPSTONE_DATABASE",
  "sfSchema" : "NAOMI",
  "sfWarehouse" : "ACADEMY_CLASS_WAREHOUSE",
  "dbtable": "AirQuality"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

spark = (
    SparkSession.builder
    .config("net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1", 'net.snowflake:snowflake-jdbc:3.13.3')
    .getOrCreate()
)


df = (a.write.format(SNOWFLAKE_SOURCE_NAME)
    .options(**sfOptions)
    .option("dbtable", 'air_quality').mode("overwrite").save()
)



