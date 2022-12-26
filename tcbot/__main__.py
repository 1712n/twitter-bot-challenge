from tcbot.logging import logger
from tcbot.mongodb import get_mongodb_client


def main():
    logger.info("tcbot has started!")

    client = get_mongodb_client()
    database = client.get_database("metrics")

    ohlcv_results = database.ohlcv_db.aggregate([
        {
            "$limit": 10
        }
    ])

    assert len(list(ohlcv_results)) == 10

    posts_results = database.posts_db.aggregate([
        {
            "$limit": 10
        }
    ])

    assert len(list(posts_results)) == 10

    client.close()

    logger.info("tcbot is exiting. Good-bye!")


if __name__ == "__main__":
    main()
