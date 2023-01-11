import os

import pyspark
from pyspark.sql import SparkSession

mongodb_user = os.environ['MONGODB_USER']
mongodb_password = os.environ['MONGODB_PASSWORD']
mongodb_address = os.environ['MONGODB_ADDRESS']

jars_path = 'jars/*'

spark = (
    SparkSession
    .builder
    .master('local[*]')
    .config('spark.mongodb.input.uri', 'mongodb://{mongodb_user}:{mongodb_password}@{mongodb_address}/ohlcv_db')
    .config('spark.mongodb.input.readPreference.name', 'secondaryPreferred')
    .config('spark.driver.extraClassPath', jars_path)
    .getOrCreate()
)

df = spark.read.format('mongodb').load()
df.printSchema()
