import logging
from typing import List

from bot.daos.ohlcv_dao import OhlcvDao
from bot.daos.posts_dao import PostsDao
from bot.message_formatter import message_format
from bot.twitter_client import TwitterClient


def _select_pair_to_post(pairs: List[str], latest_posted_pairs: List[str]) -> str:
    for p in pairs:
        if p not in latest_posted_pairs:
            return p
    return latest_posted_pairs[0]


class Bot:
    def __init__(self,
                 ohlcv_dao: OhlcvDao,
                 posts_dao: PostsDao,
                 twitter_client: TwitterClient,
                 is_dev=False):
        self._ohlcv_dao = ohlcv_dao
        self._posts_dao = posts_dao
        self._twitter_client = twitter_client
        self._is_dev = is_dev

    def run(self):
        top_pairs = self._ohlcv_dao.top_by_compound_volume(100)
        logging.info(f"Found {len(top_pairs)} top pairs by compound volume: {top_pairs[:5]}...")

        latest_posted_pairs = self._posts_dao.latest_posted_pairs(top_pairs)
        logging.info(f"Found latest post for {len(latest_posted_pairs)} pair: {latest_posted_pairs[:5]}")

        pair_to_post = _select_pair_to_post(top_pairs, latest_posted_pairs)
        logging.info(f"Pair to post: {pair_to_post}")

        volume_by_market = self._ohlcv_dao.volume_by_markets(pair_to_post)
        logging.info(f"Volume by market: {volume_by_market}")

        message = message_format(pair_to_post, volume_by_market)
        logging.info(f"Created Tweet message:\n{message}")

        # TODO rely on database
        origin_tweet_id = self._twitter_client.origin_tweet_id(pair_to_post)
        logging.info(f"Found origin Tweet ID: {origin_tweet_id}")
        if not self._is_dev:
            tweet_id = self._twitter_client.post(message, origin_tweet_id)
            logging.info(f"Tweet {tweet_id} posted")
            tweet_object_id = self._posts_dao.save_tweet(pair_to_post, message)
            logging.info(f"Tweet {tweet_id} saved with ObjectId: {tweet_object_id}")
