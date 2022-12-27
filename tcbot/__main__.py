from tcbot.core import TCBot
from tcbot.mongodb import get_mongodb_client
from tcbot.twitter import get_twitter_api


def main():
    mongodb_client = get_mongodb_client()
    twitter_api = get_twitter_api()

    bot = TCBot(mongodb_client, twitter_api)
    bot.start()


if __name__ == "__main__":
    main()
