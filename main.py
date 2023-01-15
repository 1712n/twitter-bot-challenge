from pymongo import MongoClient
import os
# from time import time
# from datetime import datetime
import SomeClass

if __name__ == "__main__":

    sc = SomeClass(os.environ["MONGODB_USER"], os.environ["MONGODB_PASSWORD"], os.environ["MONGO_DB_ADDRESS"])

    # some code will be here a bit later
