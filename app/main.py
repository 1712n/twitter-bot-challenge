import logging

import config
import twitter
from bot import MarketCapBot
from db import get_mongo_collection


def main():
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    level = logging.INFO
    if config.DEBUG:
        level = logging.DEBUG
    logging.basicConfig(level=level, format=log_format, datefmt=date_format)

    logging.info("Starting bot...")
    ohlcv_db = get_mongo_collection('ohlcv_db')
    posts_db = get_mongo_collection('posts_db')
    twitter_client = twitter.get_client()
    bot = MarketCapBot(ohlcv_db, posts_db, twitter_client)
    bot.post_message()


if __name__ == "__main__":
    main()
