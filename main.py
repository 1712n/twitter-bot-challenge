import logging
import os

from bot.bot import Bot
from bot.daos.ohlcv_dao import OhlcvDao
from bot.daos.posts_dao import PostsDao
from bot.mongo_db_client import MongoDbClient
from bot.twitter_client import TwitterClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s: %(lineno)d - %(message)s"
)

if __name__ == '__main__':
    logging.info("Prepare configuration...")
    is_development = os.environ["IS_DEV_ENV"]

    twClient = TwitterClient(os.environ["TW_ACCESS_TOKEN"],
                  os.environ["TW_ACCESS_TOKEN_SECRET"],
                  os.environ["TW_CONSUMER_KEY"],
                  os.environ["TW_CONSUMER_KEY_SECRET"])

    dbClient = MongoDbClient(os.environ["MONGODB_USER"], os.environ["MONGODB_PASSWORD"], os.environ["MONGODB_ADDRESS"])

    ohlcv_dao = OhlcvDao(dbClient, "1m")
    posts_dao = PostsDao(dbClient)

    bot = Bot(ohlcv_dao, posts_dao, twClient, is_dev=is_development)
    logging.info("Bot starting")
    bot.run()
    logging.info("Bot stopped")
