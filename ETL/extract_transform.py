from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.sql.functions import to_timestamp, col


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

df_transformed.show()
df_transformed.printSchema()



