import os
import logging
from collections import OrderedDict
from pprint import pformat
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from pymongo import MongoClient, collection
from pymongo.errors import PyMongoError
import tweepy

import src.db_querries as querry


logger = logging.getLogger(__name__)
# TODO: add config variable for levels of debuging
logging.basicConfig(
    format='%(name)s: %(levelname)s: %(message)s', level=logging.DEBUG)


class TopPairsByVolume(OrderedDict):
    def diff(self, external_keys_set: set) -> set:
        return set(self.keys()).difference(external_keys_set)

    def find_largest_among(self, pairs_set: set):
        for pair in self.keys():
            if pair in pairs_set:
                return pair, self[pair]
        raise Exception(
            "Pair supposed to be in the dictionary!")


class OldestLastPostsForPairs(OrderedDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__oldest_post_iterator = iter(self)

    # returns None if there's no pairs left to interate to
    def next_oldest_post_pair(self):
        return next(self.__oldest_post_iterator, None)


class PairMarketStats(OrderedDict):
    def compose_message(self, pair: str, total_vol: float):
        message = f"Top Market Venues for {pair}:\n"

        percent_sum = 0
        for market in self.keys():
            market_vol_percens = self[market] / total_vol * 100
            percent_sum += market_vol_percens

            if market_vol_percens >= 0.01:
                # forming a message
                message += f"{market.title()} {market_vol_percens:.2f}%\n"

        remains = 10000 - int(percent_sum * 100)
        remains /= 100
        if remains >= 0.01:
            message += f"Others {remains:.2f}%"

        return message


def find_twitter_post_id(last_post: dict, tw_client: tweepy.Client):
    if 'time' not in last_post:
        logger.debug(
            "Incorrect last post format (can't find 'time' field), giving up on search for the message id!")
        return None
    post_time = last_post['time']

    if 'tweet_text' not in last_post:
        logger.debug(
            "Incorrect last post format (can't find 'tweet_text' field), giving up on search for the message id!")
        return None
    post_text = last_post['tweet_text']

    delta = timedelta(minutes=1)
    post_time_beg = post_time - delta
    post_time_end = post_time + delta

    try:
        bot_id = tw_client.get_me().data.id
        tweets = tw_client.get_users_tweets(
            id=bot_id, start_time=post_time_beg, end_time=post_time_end, max_results=100, user_auth=True)
    except tweepy.TweepyException:
        logger.error("Error while requesting for old tweets!")
        raise

    for tweet in tweets.data:
        if tweet.text == post_text:
            logger.debug(
                f"Found previous tweeter post for pair with id:{tweet.id}!")
            return tweet.id
    logger.debug("Couldn't find previous massege, giving up.")
    return None


def post_unposted_pair(pair: str, total_vol: float,
                       ohlcv_col: collection.Collection,
                       posts_col: collection.Collection,
                       tw_client: tweepy.Client):
    try:
        symbol, base = pair.lower().split('-')
    except PyMongoError:
        logger.error(
            "Pair expected to be string in 'PAIR_SYMBOL-PAIR_BASE' format")
        raise

    try:
        pair_market_stats = PairMarketStats(
            querry.gather_pair_data(ohlcv_col, symbol, base))
    except:
        logger.error(
            "Database query for Pair's market statistics failure:")
        raise

    message = pair_market_stats.compose_message(pair, total_vol)
    logger.debug(f"Message for pair {pair}:\n{message}")

    try:
        response = tw_client.create_tweet(text=message)
    except tweepy.TweepyException:
        logger.error("Error while posting new tweet!")
        raise

    doc_to_ins = dict()
    doc_to_ins['pair'] = pair
    doc_to_ins['tweet_id'] = response.data['id']
    doc_to_ins['tweet_text'] = message
    doc_to_ins['time'] = datetime.now(timezone.utc)
    logger.debug(
        f"Generated document for the new post:\n{pformat(doc_to_ins)}")

    try:
        posts_col.insert_one(doc_to_ins)

    except PyMongoError:
        logger.error("Error while writing new post to mongoDB!")
        raise
    logger.info(f"New document inserted into posts_db!")


def post_already_posted_pair(pair: str, total_vol: float,
                             last_post: dict, ohlcv_col: collection.Collection,
                             posts_col: collection.Collection,
                             tw_client: tweepy.Client):
    try:
        symbol, base = pair.lower().split('-')
    except:
        logger.error(
            "Pair expected to be string in 'PAIR_SYMBOL-PAIR_BASE' format")
        raise

    try:
        pair_market_stats = PairMarketStats(
            querry.gather_pair_data(ohlcv_col, symbol, base))
    except PyMongoError:
        logger.error(
            "Database query for Pair's market statistics failure:")
        raise

    message = pair_market_stats.compose_message(pair, total_vol)
    logger.debug(f"Message for pair {pair}:\n{message}")

    prev_post_id = find_twitter_post_id(last_post, tw_client)
    try:
        if prev_post_id is None:
            response = tw_client.create_tweet(text=message)
            logger.info("Posted standalone tweet")
        else:
            response = tw_client.create_tweet(
                text=message, in_reply_to_tweet_id=prev_post_id)
            logger.info("Posted tweet in reply")
    except tweepy.TweepyException:
        logger.error("Error while posting new tweet!")
        raise

    doc_to_ins = dict()
    doc_to_ins['pair'] = pair
    doc_to_ins['tweet_id'] = response.data['id']
    doc_to_ins['tweet_text'] = message
    doc_to_ins['time'] = datetime.now(timezone.utc)
    logger.debug(
        f"Generated document for the new post:\n{pformat(doc_to_ins)}")

    try:
        posts_col.insert_one(doc_to_ins)
    except PyMongoError:
        logger.error("Error while writing new post to mongoDB!")
        raise
    logger.info(f"New document inserted into posts_db!")


def main():
    load_dotenv()

    user = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]
    address = os.environ["MONGO_DB_ADDRESS"]

    uri = f"mongodb+srv://{user}:{password}@{address}"

    mongo_client = MongoClient(uri)

    # not working from russian ip
    tw_client = tweepy.Client(
        consumer_key=os.environ["TW_CONSUMER_KEY"],
        consumer_secret=os.environ["TW_CONSUMER_KEY_SECRET"],
        access_token=os.environ["TW_ACCESS_TOKEN"],
        access_token_secret=os.environ["TW_ACCESS_TOKEN_SECRET"]
    )

    try:
        # The ping command is cheap and does not require auth.
        mongo_client["metrics"].command('ping')
    except PyMongoError:
        logger.error("Database connection failure:")
        raise

    try:
        ohlcv_col = mongo_client["metrics"]["ohlcv_db"]
        posts_col = mongo_client["metrics"]["posts_db"]

        pairs_vollume_dict = TopPairsByVolume(querry.get_top_pairs(ohlcv_col))
        pairs_last_posts_dict = OldestLastPostsForPairs(querry.get_posts_for_pairs(
            posts_col, list(pairs_vollume_dict.keys())))
    except PyMongoError:
        logger.error("Database queries failure:")
        raise

    # pairs wich lack posts at all
    unposted_pairs = pairs_vollume_dict.diff(set(pairs_last_posts_dict.keys()))
    logger.info(f"There's {len(unposted_pairs)} unposted pairs")

    if len(unposted_pairs) > 0:
        pair, vol = pairs_vollume_dict.find_largest_among(unposted_pairs)
        logger.info(
            f"Working with unposted pair {pair}")

        post_unposted_pair(pair, vol, ohlcv_col, posts_col, tw_client)
    else:
        pair = pairs_last_posts_dict.next_oldest_post_pair()
        if pair == None:
            logger.error(f"There's no posts")
            raise Exception("Incorrect data gathered!")
        vol = pairs_vollume_dict[pair]
        last_post = pairs_last_posts_dict[pair]

        logger.info(f"Working with already posted pair {pair}")
        logger.debug(f"Last post for pair {pair}:\n{pformat(last_post)}")
        post_already_posted_pair(pair, vol, last_post,
                                 ohlcv_col, posts_col, tw_client)


if __name__ == "__main__":
    main()
