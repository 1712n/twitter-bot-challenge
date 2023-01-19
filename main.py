import os
import logging
from pprint import pformat
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import tweepy

import src.db_querries as querry


logger = logging.getLogger(__name__)


class MarketCapBot():
    # initializes class, gets top 100 pairs and latest posts corresponding to them
    def __init__(self, db_client: MongoClient, tw_client: tweepy.Client):
        try:
            self.tw_client = tw_client

            self.ohlcv_col = db_client["metrics"]["ohlcv_db"]
            self.posts_col = db_client["metrics"]["posts_db"]

            # getting top 100 pairs by volume
            self.pairs_vollume_dict = querry.get_top_pairs(self.ohlcv_col)

            pairs_list = list(self.pairs_vollume_dict.keys())
            # getting last posts for all previously gathered pairs
            self.pairs_last_posts_dict = querry.get_posts_for_pairs(
                self.posts_col, pairs_list)
        except PyMongoError:
            logger.error("Database queries failure:")
            raise

    # returns pair's name, its total volume and last post if any
    def get_pair_to_post(self) -> tuple[str, float, dict]:
        posted_pairs = set(self.pairs_last_posts_dict.keys())

        # pairs wich lack posts at all
        unposted_pairs = self.pairs_vollume_dict.diff(posted_pairs)
        logger.info(f"There's {len(unposted_pairs)} unposted pairs")

        # if tere's uposted pair than we should post it first
        if len(unposted_pairs) > 0:
            pair, vol = self.pairs_vollume_dict.find_largest_among(
                unposted_pairs)
            logger.info(
                f"Working with unposted pair {pair}")
            return pair, vol, None

        pair = self.pairs_last_posts_dict.first()
        if pair == None:
            logger.error(f"There's no posts")
            raise Exception("Incorrect data gathered!")
        vol = self.pairs_vollume_dict[pair]
        last_post = self.pairs_last_posts_dict[pair]

        logger.info(f"Working with already posted pair {pair}")
        logger.debug(f"Last post for pair {pair}:\n{pformat(last_post)}")

        return pair, vol, last_post

    def find_twitter_post_id(self, last_post: dict) -> int:
        if 'time' not in last_post or 'tweet_text' not in last_post:
            logger.info(
                "Incorrect last post format (can't find required fields), giving up on search for the message id!")
            return None

        if 'tweet_id' in last_post:
            logger.debug(
                f"Found previous post tweet_id in last_post document with id:{last_post['tweet_id']}, going on with it")
            return last_post['tweet_id']

        post_time = last_post['time']
        post_text = last_post['tweet_text']

        delta = timedelta(minutes=1)
        post_time_beg = post_time - delta
        post_time_end = post_time + delta

        try:
            bot_id = self.tw_client.get_me().data.id
            tweets = self.tw_client.get_users_tweets(
                id=bot_id, start_time=post_time_beg, end_time=post_time_end, max_results=100, user_auth=True)
        except tweepy.TweepyException:
            logger.error("Error while requesting for old tweets!")
            raise

        for tweet in tweets.data:
            if tweet.text == post_text:
                logger.debug(
                    f"Found previous tweeter post for pair with id:{tweet.id}!")
                return tweet.id
        logger.info("Couldn't find previous message, giving up.")
        return None

    def post_tweet(self):
        pair, pair_total_vol, last_post_from_db = self.get_pair_to_post()

        try:
            symbol, base = pair.lower().split('-')
        except ValueError:
            logger.error(
                "Pair expected to be string in 'PAIR_SYMBOL-PAIR_BASE' format")
            raise

        try:
            pair_market_stats = querry.gather_pair_data(
                self.ohlcv_col, symbol, base)
        except PyMongoError:
            logger.error(
                "Database query for Pair's market statistics failure:")
            raise

        self.new_post_pair = pair
        self.new_post_message = pair_market_stats.compose_message(
            pair, pair_total_vol)
        logger.debug(f"Message for pair {pair}:\n{self.new_post_message}")

        prev_post_id = None
        if last_post_from_db != None:
            prev_post_id = self.find_twitter_post_id(last_post_from_db)
        '''separate not None checks needed in this case since not always 
        existence of last post in mongo database guarantees that we can 
        find corresponding tweet'''
        try:
            if prev_post_id != None:
                response = self.tw_client.create_tweet(
                    text=self.new_post_message, in_reply_to_tweet_id=prev_post_id)
                logger.info("Posted tweet in reply")

                self.new_post_response = response
                return response
        except tweepy.TweepyException:
            logger.error(
                "Error while posting new tweet as response, trying to post it as standalone tweet!")

        try:
            response = self.tw_client.create_tweet(
                text=self.new_post_message)
            logger.info("Posted standalone tweet")

            self.new_post_response = response
        except tweepy.TweepyException:
            logger.error("Error while posting new tweet!")
            raise
        return response

    def write_to_posts_db(self):
        doc_to_ins = dict({
            "pair": self.new_post_pair,
            "tweet_id": self.new_post_response.data['id'],
            "tweet_text": self.new_post_message,
            "time": datetime.now(timezone.utc),
        })
        logger.debug(
            f"Generated document for the new post:\n{pformat(doc_to_ins)}")

        try:
            res = self.posts_col.insert_one(doc_to_ins)
        except PyMongoError:
            logger.error("Error while writing new post to mongoDB!")
            raise
        logger.info("New document inserted into posts_db!")
        logger.debug(f"Database response:\n{res.inserted_id}")
        return res


if __name__ == "__main__":
    load_dotenv()

    level = 'INFO'
    if 'DEBUG_LEVEL' in os.environ:
        level = os.environ["DEBUG_LEVEL"]

    logging.basicConfig(
        format='%(name)s: %(lineno)s line %(levelname)s %(message)s', level=logging.getLevelName(level))

    user = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]
    address = os.environ["MONGO_DB_ADDRESS"]

    uri = f"mongodb+srv://{user}:{password}@{address}"

    mongo_client = MongoClient(uri)

    # not working from russian ip
    twitter_client = tweepy.Client(
        consumer_key=os.environ["TW_CONSUMER_KEY"],
        consumer_secret=os.environ["TW_CONSUMER_KEY_SECRET"],
        access_token=os.environ["TW_ACCESS_TOKEN"],
        access_token_secret=os.environ["TW_ACCESS_TOKEN_SECRET"]
    )

    bot = MarketCapBot(db_client=mongo_client, tw_client=twitter_client)
    bot.post_tweet()
    bot.write_to_posts_db()
