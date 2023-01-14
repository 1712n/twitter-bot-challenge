import os
import logging

import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import tweepy

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

def log_df(legend, df, n=20, truncate=20, vertical=False):
    int_truncate = int(truncate)
    df_string = df._jdf.showString(n, int_truncate, vertical)
    logger.info(f'{legend}:\n{df_string}')

mongodb_user = os.environ['MONGODB_USER']
mongodb_password = os.environ['MONGODB_PASSWORD']
mongodb_address = os.environ['MONGODB_ADDRESS']
mongodb_uri = f'mongodb+srv://{mongodb_user}:{mongodb_password}@{mongodb_address}'

tw_consumer_key = os.environ["TW_CONSUMER_KEY"]
tw_consumer_secret = os.environ["TW_CONSUMER_KEY_SECRET"]
tw_access_token = os.environ["TW_ACCESS_TOKEN"]
tw_access_token_secret = os.environ["TW_ACCESS_TOKEN_SECRET"]

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
log_df('Top 100 pairs by compound volume', top_100_pair, truncate=False)

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
log_df('Last posts in twitter for top 100 pairs', last_post, 100)

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
log_df('Share of volume per market', pair_market_share)

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
        F.lit('others').alias('market_venue'),
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
log_df('Top 5 markets share per pair', top_5_with_other_market)

message_to_post = (
    top_5_with_other_market
    .withColumn(
        'market_venue_share',
        F.concat(
            F.initcap('market_venue'),
            F.lit(' '),
            F.col('market_share'),
            F.lit('%')))
    .groupBy('pair')
    .agg(F.concat_ws('\n', F.collect_list('market_venue_share')).alias('footer'))
    .withColumn(
        'header',
        F.concat(
            F.lit('Top Market Venues for '),
            F.col('pair'),
            F.lit(':\n')))
    .select(
        F.col('pair'),
        F.concat('header', 'footer').alias('tweet_text'))
    .orderBy('pair')
    .filter('pair = "BTC-USD"') # for test
)
log_df('Message texts', message_to_post, truncate=False)

tweet_df = (
    message_to_post
    .join(
        last_post
        .select(
            F.current_timestamp().alias('time'),
            F.col('pair'),
            F.col('tweet_id').alias('parent_tweet_id')),
        on='pair',
        how='left')
    .withColumn('tweet_id', F.lit(None))
)

twitter_client = tweepy.Client(
    consumer_key=tw_consumer_key,
    consumer_secret=tw_consumer_secret,
    access_token=tw_access_token,
    access_token_secret=tw_access_token_secret
)

# twitter posts limitation
rows = tweet_df.limit(5).collect()
for i, row in enumerate(rows):
    parent_tweet_id = row['parent_tweet_id']
    try:
        if parent_tweet_id:
            tweet = twitter_client.create_tweet(text=tweet_text, in_reply_to_tweet_id=parent_tweet_id)
            pass
        else:
            tweet = twitter_client.create_tweet(text=tweet_text)
            pass
        tweet_id = tweet.data['id']
        tweet_df = tweet_df.withColumn(
            'tweet_id',
            F.when(
                F.col('pair') == row['pair'],
                tweet_id
            ).otherwise(
                F.col('tweet_id')))
    except:
        logging.exception(msg=f'Tweet for {row["pair"]} failed')

(
    tweet_df
    .write
    .format('mongodb')
    .mode('append')
    .option('database', 'metrics')
    .option('collection', 'posts_db')
    .option('partitioner', 'com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner')
    .save()
)
log_df('Records inserted in posts_db:', tweet_df)
