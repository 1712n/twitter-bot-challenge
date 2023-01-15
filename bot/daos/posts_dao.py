from typing import List

from bot.mongo_db_client import MongoDbClient


class PostsDao:
    def __init__(self, mongodb_client: MongoDbClient):
        self.db = mongodb_client

    def latest_posted_pairs(self, pairs: List[str]) -> List[str]:
        pass
