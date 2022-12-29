from tcbot.core import TCBot
from tcbot.logging import logger
from tcbot.mongodb import get_mongodb_client, InvalidMongoClientError
from tcbot.twitter import get_twitter_client, InvalidTwitterClientError


def main():
    try:
        mongodb_client = get_mongodb_client()
        twitter_client = get_twitter_client()

        if not mongodb_client:
            raise InvalidMongoClientError

        if not twitter_client:
            raise InvalidTwitterClientError

        bot = TCBot(mongodb_client, twitter_client)
        bot.start()

    except (InvalidMongoClientError, InvalidTwitterClientError) as err:
        logger.error(str(err))
    except Exception:
        raise


if __name__ == "__main__":
    main()
