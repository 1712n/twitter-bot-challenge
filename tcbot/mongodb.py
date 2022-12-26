import os
from dotenv import load_dotenv
from pymongo import MongoClient


def get_mongodb_client():
    load_dotenv()

    address = os.environ["MONGO_DB_ADDRESS"]
    username = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]

    uri = f"mongodb+srv://{username}:{password}@{address}"

    return MongoClient(uri)
