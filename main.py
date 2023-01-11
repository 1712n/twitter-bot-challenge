import os

import pyspark
from pyspark.sql import SparkSession

mongodb_user = os.environ['MONGODB_USER']
mongodb_password = os.environ['MONGODB_PASSWORD']
mongodb_address = os.environ['MONGODB_ADDRESS']
mongodb_uri = 'mongodb://{mongodb_user}:{mongodb_password}@{mongodb_address}'

jars_path = 'jars/*'

spark = (
    SparkSession.builder
    .master('local[*]')
    .config('spark.mongodb.input.uri', mongodb_uri)
    .config('spark.mongodb.input.database', 'ohlcv_db')
    .config('spark.mongodb.input.readPreference.name', 'secondaryPreferred')
    .config('spark.driver.extraClassPath', jars_path)
    .getOrCreate()
)

df = (
    spark.read
    .format('mongodb')
    .option('uri', f'{mongodb_uri}/ohlcv_db')
    .option('database', 'ohlcv_db')
    .load()
)
df.printSchema()
