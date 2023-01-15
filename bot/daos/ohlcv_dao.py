from typing import List, Dict

from bot.mongo_db_client import MongoDbClient


class OhlcvDao:
    def __init__(self, mongodb_client: MongoDbClient, granuality: str):
        self._granuality = granuality
        self._db = mongodb_client

    def top_by_compound_volume(self, limit: int) -> List[str]:
        pass

    def latest_volume_by_markets(self, pair: str) -> Dict[str, int]:
        pass
