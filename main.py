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

ohlcv_df = (
    spark.read
    .format('mongodb')
    .option('database', 'metrics')
    .option('collection', 'ohlcv_db')
    .option('partitioner', 'com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner')
    .load()
)

# check if compound volume should be in pieces or in usd
# check if last n days should be filtered explicitly
# check if need to handle the case usd != usdc,usdt...
# check if top coins should be taken but not pairs
top_100_pairs = (
    ohlcv_df
    .filter('granularity = "1h"') # filter 'today - n days >= timestamp'?
    .withColumn('compound_volume_usd', F.col('volume')*F.col('close')) # close * stablecoin_rate?
    .groupBy('pair_base', 'pair_symbol') # pair_symbol only?
    .agg(F.sum('volume'), F.sum('compound_volume_usd'))
    .orderBy(F.desc('sum(compound_volume_usd)')) # sum(volume)?
    .limit(100)
)

top_100_pairs.show(truncate=False)
