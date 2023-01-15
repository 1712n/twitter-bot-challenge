from pymongo import MongoClient


class MongoDbClient:
    def __init__(self, user: str, password: str, cluster_address: str):
        uri = f"mongodb+srv://{user}:{password}@{cluster_address}"
        self._client = MongoClient(uri)

    def get_collection(self, collection_name: str):
        return self._client["metrics"][collection_name]
