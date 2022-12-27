from tcbot.logging import logger
from tcbot.mongodb import get_mongodb_client, get_pair_to_post


def main():
    logger.info("TCBot has started!")

    client = get_mongodb_client()
    database = client.get_database("metrics")

    logger.info("Getting pair to post...")
    results = get_pair_to_post(database)
    results = list(results)

    if len(results) > 0:
        pair_to_post = results[0]
        logger.info("The pair '{}' needs to be posted!", pair_to_post['_id'])
    else:
        logger.info("No pair to post found :(")

    client.close()

    logger.info("TCBot is exiting. Good-bye!")


if __name__ == "__main__":
    main()
