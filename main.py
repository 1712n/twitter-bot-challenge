import tweepy
import logging
from database import connect_db, choose_pair


def main():
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]: %(message)s", level=logging.INFO)
    db_client = connect_db()
    choose_pair(db_client)


if __name__ == '__main__':
    main()

