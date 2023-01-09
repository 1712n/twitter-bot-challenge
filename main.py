import logging

from bot import TwitterMarketCapBot
from database import MongoDatabase


def main():
    db = MongoDatabase()
    bot = TwitterMarketCapBot(db=db)
    bot.run()


if __name__ == '__main__':
    main()
