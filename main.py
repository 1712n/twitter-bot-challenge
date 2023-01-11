import os

import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

mongodb_user = os.environ['MONGODB_USER']
mongodb_password = os.environ['MONGODB_PASSWORD']
mongodb_address = os.environ['MONGODB_ADDRESS']
mongodb_uri = f'mongodb+srv://{mongodb_user}:{mongodb_password}@{mongodb_address}'

jars_path = 'jars/*'

spark = (
    SparkSession.builder
    .master('local[*]')
    .config('spark.driver.extraClassPath', jars_path)
    .config('spark.mongodb.read.connection.uri', f'{mongodb_uri}')
    .getOrCreate()
)

df = (
    spark.read
    .format('mongodb')
    .option('database', 'metrics')
    .option('collection', 'ohlcv_db')
    .load()
    .orderBy(F.desc('volume'))
    .limit(100)
)
df.printSchema()
df.show()
