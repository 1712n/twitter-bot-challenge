import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

uri = f"mongodb+srv://{user}:{password}@{address}"
client = MongoClient(uri)