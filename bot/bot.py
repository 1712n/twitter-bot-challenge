from bot.daos.ohlcv_dao import OhlcvDao
from bot.daos.posts_dao import PostsDao
from bot.message_formatter import MessageFormatter
from bot.twitter_client import TwitterClient


class Bot:
    def __init__(self,
                 ohlcv_dao: OhlcvDao,
                 posts_dao: PostsDao,
                 message_formatter: MessageFormatter,
                 twitter_client: TwitterClient):
        self._ohlcv_dao = ohlcv_dao
        self._posts_dao = posts_dao
        self._message_formatter = message_formatter
        self._twitter_client = twitter_client

    def run(self):
        pass
