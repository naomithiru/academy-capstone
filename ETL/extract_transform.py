
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

#build a spark session
spark = SparkSession.builder.getOrCreate()


df = spark.read.csv("s3://dataminded-academy-capstone-resources/raw/open_aq/")
df.show()
