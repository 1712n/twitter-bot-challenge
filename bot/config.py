import os
from pymongo import MongoClient

#bearer_token = os.environ["TW_BEARER_TOKEN"]

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

uri = f"mongodb+srv://{user}:{password}@{address}"
client = MongoClient(uri)
