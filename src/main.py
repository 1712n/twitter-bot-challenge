import logging

from bot import TwitterMarketCapBot
from database import MongoDatabase
from twitter import Twitter


logging.basicConfig(level=logging.INFO)


def main():
    db = MongoDatabase()
    twitter = Twitter()
    bot = TwitterMarketCapBot(db=db, twitter=twitter)
    bot.run()


if __name__ == '__main__':
    main()
