import tweepy
from database import connect_db


def main():
    db_client = connect_db()


if __name__ == '__main__':
    main()

