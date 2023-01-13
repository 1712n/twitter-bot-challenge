import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pymongo
import tweepy
from dotenv import load_dotenv

load_dotenv()

# load environment variables
TW_ACCESS_TOKEN = os.getenv('TW_ACCESS_TOKEN')
TW_ACCESS_TOKEN_SECRET = os.getenv('TW_ACCESS_TOKEN_SECRET')
TW_CONSUMER_KEY = os.getenv('TW_CONSUMER_KEY')
TW_CONSUMER_KEY_SECRET = os.getenv('TW_CONSUMER_KEY_SECRET')

MONGODB_USER = os.getenv('MONGODB_USER')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD')
MONGO_DB_ADDRESS = os.getenv('MONGO_DB_ADDRESS')

# set logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


# method for connecting to the MongoDB cluster
def get_mongodb_client():
    uri = f'mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGO_DB_ADDRESS}'
    logger.info('Connecting to MongoDB')
    client = pymongo.MongoClient(uri)
    logger.info('Connected to MongoDB.')
    return client


# methods for working with db
def get_top_pairs(ohlcv_db, time_period=1):
    """
    Searching for top traiding pairs to post.
    """
    logger.info('Starting get_top_pairs function.')
    logger.info('Querying ohlcv_db for the top 100 pairs by compound volume..')
    top_pairs = list(
        ohlcv_db.aggregate(
            [
                {
                    '$match': {
                        'timestamp': {
                            '$gte': datetime.now().astimezone()
                            - timedelta(hours=time_period)
                        },
                    }
                },
                {
                    '$group': {
                        '_id': {'$concat': [
                            '$pair_symbol', '-', '$pair_base'
                            ]},
                        'volume_sum': {'$sum': {'$toDouble': '$volume'}},
                    }
                },
                {
                    '$sort': {'volume_sum': -1},
                },
                {
                    '$limit': 5
                },
            ]
        )
    )
    if len(top_pairs) == 0:
        raise ValueError('No pairs for the given time period were found.')
    return top_pairs


def get_latest_posts(top_pairs, posts_db):
    """
    Searching for the corresponding latest posts.
    """
    logger.info('Starting get_latest_posts function.')
    logger.info('Querying posts_db for the latest documents corresponding to'
                'choosen 100 pairs...')
    pairs = list(x['_id'].upper() for x in top_pairs)
    posts = list(
        posts_db.aggregate(
            [
                {
                    '$match': {
                        'pair': {'$in': pairs}
                    },
                },
                {
                    '$group': {
                        '_id': '$pair',
                        'time': {
                            '$max': {'time': '$time', 'post_id': '$_id'}
                        }
                    }
                },
            ]
        )
    )
    if len(posts) == 0:
        logger.info('No posts matching the pairs were found.')
    else:
        logger.info(f'Found {len(posts)} matching posts')
        return posts


def get_pair_to_post(top_pairs, posts):
    """
    Choosing pair to post.
    """
    logger.info('Starting get_pair_to_post function.')
    logger.info('Choosing pair to post...')
    pairs = list(x['_id'].upper() for x in top_pairs)
    top_pairs = pd.DataFrame(top_pairs, index=pairs)
    posts_indexes = []
    for val in posts:
        if type(val['_id']) == list:
            posts_indexes.append(val['_id'][0])
        else:
            posts_indexes.append(val['_id'])
    posts = pd.DataFrame(
        [x['time'] for x in posts], index=posts_indexes
    )
    result = top_pairs.join(posts, how='left')
    result.sort_values(
        by=['time', 'volume_sum'], ascending=[True, False], inplace=True
    )
    pair_to_post = result.iloc[0]
    logger.info(f'Selected pair is {result.index.values[0]}')
    return [result.index.values[0], pair_to_post['post_id']]


def compose_message(pair, pair_symbol, pair_base, ohlcv_db, time_period=1):
    """
    Compose message to post.
    """
    logger.info('Starting compose_message function.')
    logger.info(f'Querying ohlcv_db for {pair} pair '
                'with corresponding latest volumes by market values')
    result = list(
        ohlcv_db.aggregate(
            [
                {
                    '$match': {
                        'timestamp': {
                            '$gte': datetime.now().astimezone()
                            - timedelta(hours=time_period)
                        },
                        'pair_symbol': pair_symbol,
                        'pair_base': pair_base,
                    }
                },
                {
                    '$group': {
                        '_id': '$marketVenue',
                        'volume': {
                            '$max': {
                                'time': '$timestamp',
                                'value': {'$toDouble': '$volume'}
                            }
                        },
                    }
                },

            ]
        )
    )
    if len(result) == 0:
        raise ValueError('No documents for the given pair were found.')

    result = pd.DataFrame(
        [{'_id': x['_id'], 'market_volume': x['volume']['value']}
         for x in result]
    ).sort_values(by='market_volume', ascending=False)

    message_to_post = f'Top Market Venues for {pair}:\n'
    total_volume = result['market_volume'].sum()
    if len(result) <= 6:
        for i, val in result.iterrows():
            message_to_post += (
                f"{val['_id'].capitalize()} {(val['market_volume']/total_volume)*100:.2f}%\n"
            )
    else:
        for i, val in result.iloc[:5].iterrows():
            message_to_post += (
                f"{val['_id'].capitalize()} {(val['market_volume']/total_volume)*100:.2f}%\n"
            )
        message_to_post += (
            f"Others {(1 - result['market_volume'].iloc[:5].sum()/total_volume)*100:.2f}%\n"
        )
    return message_to_post


def add_post_to_db(pair, posts_db, message, tweet_id):
    """
    Function for adding post to db.
    """
    logger.info('Adding post to db...')
    try:
        posts_db.insert_one(
            {
                'pair': pair,
                'time': datetime.now().astimezone(),
                'tweet_text': message,
                'tweet_id': tweet_id,
            }
        )
        logger.info('Post was successfully added to db.')
    except Exception as error:
        logger.error(f'Problems with adding new post to db: {error}')


# methods for twitter
def post_tweet(message_to_post, pair, posts_db):
    """
    Post tweet function.
    """
    logger.info('Getting tweepy client.')
    try:
        tweepy_client = tweepy.Client(
            consumer_key=TW_CONSUMER_KEY,
            consumer_secret=TW_CONSUMER_KEY_SECRET,
            access_token=TW_ACCESS_TOKEN,
            access_token_secret=TW_ACCESS_TOKEN_SECRET,
        )
        logger.info('Tweepy client succsessfuly created.')
        user_id = tweepy_client.get_me().data.id
    except Exception as error:
        logger.error(f'Problems with getting tweepy client: {error}')

    logger.info('Searching for the corresponding Twitter thread.')
    post = list(posts_db.find({
        'pair': pair,
        'tweet_id': {
            '$exists': True
        }
    }).sort('time', pymongo.DESCENDING).limit(1)
    )
    if len(post) == 0:
        logger.info('Corresponding Twitter thread was not found. '
                    'Posting new tweet.')
        tweepy_client.create_tweet(text=message_to_post)
        recent_tweets = tweepy_client.get_users_tweets(
            id=user_id, user_auth=True)
        tweet_id = recent_tweets.meta['newest_id']
    else:
        logger.info('Posting tweet to the corresponding thread.')
        tweet_id = post[0]['tweet_id']
        tweepy_client.create_tweet(
            text=message_to_post,
            in_reply_to_tweet_id=tweet_id
        )
    return tweet_id


# other methods
def check_tokens():
    """
    Checking environment variables.
    """
    tokens = (
        TW_ACCESS_TOKEN, TW_ACCESS_TOKEN_SECRET, TW_CONSUMER_KEY,
        TW_CONSUMER_KEY_SECRET, MONGO_DB_ADDRESS, MONGODB_PASSWORD,
        MONGODB_USER
    )
    logger.info('Checking environment variables.')
    for name in tokens:
        if name is None:
            logger.critical(
                f'Missing required environment variable: {name}'
            )
            return False
    return True


def main():
    """
    The main logic of the bot.
    """
    logger.info('Starting main function.')

    if not check_tokens():
        raise Exception('Missing required environment variables.')
    
    try:
        client = get_mongodb_client()
    except Exception as error:
        logger.error(f'Connection to MongoDB failed: {error}')
        sys.exit(1)
    
    ohlcv_db = client['metrics']['ohlcv_db']
    posts_db = client['metrics']['posts_db']

    try:
        top_pairs = get_top_pairs(ohlcv_db)
    except Exception as error:
        logger.error(f'Getting top pairs failed: {error}')
        sys.exit(1)
    
    try:
        posts = get_latest_posts(top_pairs, posts_db)
    except Exception as error:
        logger.error(f'Getting latest posts failed: {error}')
        sys.exit(1)
    
    try:
        pair, post_id = get_pair_to_post(top_pairs, posts)
    except Exception as error:
        logger.error(f'Getting pair to post failed: {error}')
        sys.exit(1)
    
    pair_symbol = pair.split('-')[0].lower()
    pair_base = pair.split('-')[1].lower()
    
    try:
        message_to_post = compose_message(
            pair, pair_symbol, pair_base, ohlcv_db
        )
    except Exception as error:
        logger.error(f'Composing message to post failed: {error}')
        sys.exit(1)
    
    try:
        tweet_id = post_tweet(message_to_post, pair, posts_db)
    except Exception as error:
        logger.error(f'Posting tweet failed: {error}')
        sys.exit(1)
    
    try:
        add_post_to_db(
            pair, posts_db, message_to_post, tweet_id)
    except Exception as error:
        logger.error(f'Adding post to db failed: {error}')
        sys.exit(1)
