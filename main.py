import os
import tweepy
import logging
from database import connect_db, choose_pair, get_markets


def main():
    if os.environ.get("DEBUG"):
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]: %(message)s", level=level)

    db_client = connect_db()
    pair, volume = choose_pair(db_client)
    markets = get_markets(db_client, pair)


if __name__ == '__main__':
    main()

