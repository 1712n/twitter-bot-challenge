import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pymongo
from dotenv import load_dotenv

load_dotenv()

# get environment variables
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

# connecting to the MongoDB cluster and getting collections
uri = f'mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGO_DB_ADDRESS}'
client = pymongo.MongoClient(uri)

db = client['metrics']
ohlcv_db = db['ohlcv_db']
posts_db = db['posts_db']


# methods for working with db
def get_top_pairs(ohlcv_db, time_period=1):
    """
    Searching for top traiding pairs to post.
    """
    logger.info('Starting get_pair_to_post method.')
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


def get_latest_posts(top_pairs):
    """
    Searching for the corresponding latest posts.
    """
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
