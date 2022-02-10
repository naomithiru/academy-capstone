from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import flatten

#build a spark session
#wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.2/hadoop-aws-3.1.2.jar
spark = (SparkSession.builder.config("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.1.2')
                            .config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                            .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain").getOrCreate()
        )
#sc = spark.sparkContext
df = spark.read.json("s3://dataminded-academy-capstone-resources/raw/open_aq/")

#write locally to a file
df.write.format('json').save("capstone.json")

#df.show()
df.printSchema()
df_exploded = df.select("city", 
                        "coordinates.latitude","coordinates.latitude", 
                        "country", 
                        "date.local", "date.utc", 
                        "entity", "isAnalysis", "isMobile", "location", "locationId", "parameter", "sensorType", "unit", "value")
df_exploded.show()

