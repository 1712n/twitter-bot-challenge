from bot import MarketCapBot
from database import MongoDatabase
from twitter import Twitter
import logging


logging.basicConfig(level=logging.INFO)


def main():
    db = MongoDatabase()
    twitter =  None #Twitter()
    bot = MarketCapBot(db=db, twitter=twitter)
    bot.run()


if __name__ == '__main__':
    main()