import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf



#build a spark session
spark = SparkSession.builder.getOrCreate()


df = spark.read.csv("s3://dataminded-academy-capstone-resources/raw/open_aq/")
df.show()
