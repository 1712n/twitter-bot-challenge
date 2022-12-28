from tcbot.core import TCBot
from tcbot.mongodb import get_mongodb_client
from tcbot.twitter import get_twitter_client


def main():
    mongodb_client = get_mongodb_client()
    twitter_client = get_twitter_client()

    bot = TCBot(mongodb_client, twitter_client)
    bot.start()


if __name__ == "__main__":
    main()
