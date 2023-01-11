import os

import pyspark
from pyspark.sql import SparkSession

mongodb_user = os.environ['MONGODB_USER']
mongodb_password = os.environ['MONGODB_PASSWORD']
mongodb_address = os.environ['MONGODB_ADDRESS']
mongodb_uri = f'mongodb://{mongodb_user}:{mongodb_password}@{mongodb_address}'

from pymongo import MongoClient

mongodb_uri = f'mongodb+srv://{mongodb_user}:{mongodb_password}@{mongodb_address}'
client = MongoClient(uri)
print(client)

'''
jars_path = 'jars/*'

spark = (
    SparkSession.builder
    .master('local[*]')
    .config('spark.driver.extraClassPath', jars_path)
    .config('spark.mongodb.read.connection.uri', f'{mongodb_uri}/ohlcv_db.pair')
    #.config('spark.mongodb.write.connection.uri', 'mongodb://test/test.coll')
    .getOrCreate()
)

df = (
    spark.read
    .format('mongodb')
    #.option('database', 'ohlcv_db')
    #.option('collection', 'pair')
    .load()
)
df.printSchema()
'''
