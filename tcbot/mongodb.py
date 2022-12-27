from pymongo import MongoClient

from tcbot import env


def get_mongodb_client():
    address = env.MONGODB_ADDRESS
    username = env.MONGODB_USERNAME
    password = env.MONGODB_PASSWORD

    uri = f"mongodb+srv://{username}:{password}@{address}"

    return MongoClient(uri)
