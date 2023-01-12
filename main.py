import os

import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

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

# check if last n days should be filtered explicitly
ohlcv_pair = (
    ohlcv_df
    .filter('granularity = "1h"') # filter 'today - n days >= timestamp'?
    .withColumn('pair_symbol', F.upper('pair_symbol'))
    .withColumn('pair_base', F.upper('pair_base'))
    .withColumn('pair', F.concat('pair_symbol', F.lit('-'), 'pair_base').alias('pair'))
    .withColumnRenamed('marketVenue', 'market_venue')
    .persist()
)

# check if compound volume should be in pieces or in usd
# check if need to handle the case usd != usdc,usdt...
# check if top coins should be taken but not pairs
top_100_pair = (
    ohlcv_pair
    .withColumn('compound_volume_usd', F.col('volume')*F.col('close')) # close * stablecoin_rate
    .groupBy('pair') # pair_symbol only?
    .agg(F.sum('volume'), F.sum('compound_volume_usd'))
    .orderBy(F.desc('sum(compound_volume_usd)')) # sum(volume)?
    .limit(100)
    .persist()
)
top_100_pair.show(truncate=False)

posts_df = (
    spark.read
    .format('mongodb')
    .option('database', 'metrics')
    .option('collection', 'posts_db')
    .option('partitioner', 'com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner')
    .load()
)

w = Window.partitionBy('pair').orderBy(F.desc('time'))

last_post = (
    top_100_pair
    .join(posts_df, on='pair', how='left')
    .withColumn('row_number', F.row_number().over(w))
    .withColumn('days_from_post', F.datediff(F.current_date(), F.col('time')))
    .filter('row_number = 1')
    .select(
        'pair',
        'tweet_id',
        'time',
        'days_from_post')
)
last_post.show(100)

# clarify how old should be the last post to do the new one
pair_to_post = last_post.filter('days_from_post >= 3 or days_from_post is null')

w = Window.partitionBy('pair')
pair_market_share = (
    ohlcv_pair
    .join(pair_to_post, on='pair')
    .withColumn('total_volume', F.sum('volume').over(w))
    .groupBy('market_venue', 'pair', 'total_volume')
    .agg(F.sum('volume').alias('market_volume'))
    .withColumn('market_share', F.round(F.col('market_volume')/F.col('total_volume')*100, 2))
    .orderBy('pair', F.desc('market_share'))
)
pair_market_share.show()

top_5_market = (
    pair_market_share
    .withColumn('row_number', F.row_number().over(w.orderBy(F.desc('market_share'))))
    .filter('row_number <= 5')
    .filter('market_share >= 0.01')
    .select('market_venue', 'pair', 'market_share') 
)

other_market = (
    top_5_market
    .groupBy('pair')
    .agg(F.round(100 - F.sum('market_share'), 2).alias('market_share'))
    .select(
        F.lit('other').alias('market_venue'),
        'pair', 
        'market_share'
    )
    .filter('market_share >= 0.01')
)

top_5_with_other_market = (
    top_5_market
    .unionByName(other_market)
    .orderBy('pair', F.desc('market_share'))
)
top_5_with_other_market.show()

message_to_post = (
    top_5_with_other_market
    .withColumn('text', F.concat(F.initcap('market_venue'), F.lit(' '), F.col('market_share'), F.lit('%')))
    .groupBy('pair')
    .agg(F.concat_ws('\n', F.collect_list('text')).alias('footer'))
    .withColumn('header', F.concat(F.lit('Top Market Venues for '), 'pair', F.lit(':\n')))
    .select(
        'pair',
        F.concat('header', 'footer').alias('tweet_text'))
)
message_to_post.show(truncate=False)
