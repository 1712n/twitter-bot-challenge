from pymongo import MongoClient

from tcbot import env


class InvalidMongoClientError(Exception):
    def __init__(self):
        default_message = "Could not get a MongoDB client. " \
        "Please check your credentials, your network connection, and try again."
        super().__init__(default_message)


def get_mongodb_client():
    try:
        address = env.MONGODB_ADDRESS
        username = env.MONGODB_USERNAME
        password = env.MONGODB_PASSWORD

        uri = f"mongodb+srv://{username}:{password}@{address}"

        return MongoClient(uri)
    except:
        return None
