import os
import pytest
import pymongo

from dotenv import load_dotenv


load_dotenv()

user = os.environ["TEST_MONGODB_USER"]
password = os.environ["TEST_MONGODB_PASSWORD"]
address = os.environ["TEST_MONGO_DB_ADDRESS"]
uri = f"mongodb+srv://{user}:{password}@{address}"


@pytest.fixture(scope="session")
def demo_client():
    client = pymongo.MongoClient(uri)
    yield client


@pytest.fixture(scope="function")
def demo_db(demo_client):
    demo_db = demo_client["metrics"]
    demo_db["ohlcv_db"]
    demo_db["posts_db"]
    yield demo_db
    demo_db.drop_collection("ohlcv_db")
    demo_db.drop_collection("posts_db")
