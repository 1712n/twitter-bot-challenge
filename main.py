# from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()
import os
import PairToPost
import MarketCapBot

if __name__ == "__main__":

    pair_to_post = PairToPost(os.environ["MONGODB_USER"], os.environ["MONGODB_PASSWORD"], os.environ["MONGO_DB_ADDRESS"])
    bot = MarketCapBot(os.environ["TW_CONSUMER_KEY"], os.environ["TW_CONSUMER_KEY_SECRET"], os.environ["TW_ACCESS_TOKEN"], os.environ["TW_ACCESS_TOKEN_SECRET"])
    # to be continued
