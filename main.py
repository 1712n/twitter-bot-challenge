import os

from bot.bot import Bot
from bot.daos.ohlcv_dao import OhlcvDao
from bot.daos.posts_dao import PostsDao
from bot.mongo_db_client import MongoDbClient
from bot.twitter_client import TwitterClient

if __name__ == '__main__':
    is_development = os.environ["IS_DEV_ENV"]

    twClient = TwitterClient(os.environ["TW_ACCESS_TOKEN"],
                  os.environ["TW_ACCESS_TOKEN_SECRET"],
                  os.environ["TW_CONSUMER_KEY"],
                  os.environ["TW_CONSUMER_KEY_SECRET"])

    dbClient = MongoDbClient(os.environ["MONGODB_USER"], os.environ["MONGODB_PASSWORD"], os.environ["MONGODB_ADDRESS"])

    ohlcv_dao = OhlcvDao(dbClient, "1h")
    posts_dao = PostsDao(dbClient)

    bot = Bot(ohlcv_dao, posts_dao, twClient)
    bot.run()
