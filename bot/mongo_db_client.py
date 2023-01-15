class MongoDbClient:
    def __init__(self, user: str, password: str, cluster_address: str):
        self._user = user
        self._password = password
        self._address = cluster_address
