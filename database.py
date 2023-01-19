import os
import time
import logging
import datetime
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConfigurationError, ConnectionFailure


def handle_mongodb_errors(func):
    """A decorator to handle MongoDB exceptions.

    Tries to reconnect 5 times with increasing wait times, then fails. Number of
    reconnects can be changed via MONGODB_RECONNECT_ATTEMPTS environment variable.
    Logs other errors.
    """

    def _handle_mongodb_errors(*args, **kwargs):
        max_attempts = os.environ.get("MONGODB_RECONNECT_ATTEMPTS", 5)
        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)

            except AutoReconnect:
                logging.warning(
                    "Connecting to the database failed. Trying to reconnect...")
                time.sleep(pow(2, attempt))

            except ConnectionFailure as error:
                logging.error("Connecting to the database failed. Cause: %s", str(error))
                raise

            except ConfigurationError as error:
                logging.error("Database is configured incorrectly: %s", str(error))
                raise

            except Exception as error:
                logging.error("A database operation failed. Cause: %s", str(error))
                raise

        return func(*args, **kwargs)

    return _handle_mongodb_errors


@handle_mongodb_errors
def connect_db():
    """Connects to MongoDB using credentials from environment vars.

    Returns:
        pymongo.MongoClient: database client.
    """

    user = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]
    address = os.environ["MONGODB_ADDRESS"]
    uri = f"mongodb+srv://{user}:{password}@{address}"

    client = MongoClient(uri)
    logging.info('Successfully connected to MongoDB')

    return client


@handle_mongodb_errors
def choose_pair(client, granularity="1h"):
    """Chooses a trading pair with big market volume that hasn't been posted for a while.

    Arguments:
        client (pymongo.MongoClient): A database client.
        granularity (str): Granularity of aggregated data. Equals '1h' by default.

    Returns:
        (string, float): Name of the chosen pair and its market volume.
    """

    logging.info('Choosing a pair to post...')
    top_pairs_cursor = client['metrics']['ohlcv_db'].aggregate([
        {
            '$match': {
                'granularity': granularity
            }
        }, {
            '$sort': {
                'timestamp': -1
            }
        }, {
            '$group': {
                '_id': {
                    'marketVenue': '$marketVenue',
                    'pair_base': '$pair_base',
                    'pair_symbol': '$pair_symbol'
                },
                'volume': {
                    '$first': '$volume'
                },
                'pair_base': {
                    '$first': '$pair_base'
                },
                'pair_symbol': {
                    '$first': '$pair_symbol'
                },
                'marketVenue': {
                    '$first': '$marketVenue'
                }
            }
        }, {
            '$project': {
                'pair': {
                    '$toUpper': {
                        '$concat': [
                            '$pair_symbol', '-', '$pair_base'
                        ]
                    }
                },
                'volume': {
                    '$convert': {
                        'input': '$volume',
                        'to': 'double'
                    }
                }
            }
        }, {
            '$group': {
                '_id': '$pair',
                'volume': {
                    '$sum': '$volume'
                }
            }
        }, {
            '$sort': {
                'volume': -1
            }
        }, {
            '$limit': 100
        }
    ])

    top_pairs = {}
    for doc in top_pairs_cursor:
        top_pairs[doc['_id']] = doc['volume']
    logging.info('Aggregated top 100 pairs by market volume')

    # pipeline unwinds first to handle documents with pair: [pair_name, pair_volume]
    last_posts_cursor = client['metrics']['posts_db'].aggregate([
        {
            '$unwind': {
                'path': '$pair'
            }
        }, {
            '$match': {
                'pair': {
                    '$in': list(top_pairs.keys())
                }
            }
        }, {
            '$sort': {
                'time': -1
            }
        }, {
            '$group': {
                '_id': '$pair',
                'time': {
                    '$first': '$time'
                }
            }
        }, {
            '$sort': {
                'time': 1
            }
        }, {
            '$limit': 5
        }
    ])

    last_posts = {}
    for doc in last_posts_cursor:
        last_posts[doc['_id']] = doc['time']
    logging.info(
        'Aggregated 5 oldest posts corresponding to top 100 pairs')

    posted_pairs_cursor = client['metrics']['posts_db'].aggregate([
        {
            '$unwind': {
                'path': '$pair'
            }
        }, {
            '$group': {
                '_id': '$pair'
            }
        }, {
            '$match': {
                '_id': {
                    '$type': 'string'
                }
            }
        }
    ])
    logging.info('Aggregated all posted pairs')

    posted_pairs = [pair['_id'] for pair in posted_pairs_cursor]

    candidate_pairs = []
    for pair in top_pairs:  # finding pairs that haven't been posted yet
        if pair not in posted_pairs:
            candidate_pairs.append(pair)

    candidate_pairs += list(last_posts.keys())

    candidate_pairs = sorted(
        candidate_pairs, key=lambda pair: top_pairs[pair], reverse=True)
    chosen_pair = candidate_pairs[0]

    logging.debug("Result: " + ", ".join(
        [f"{pair}: {top_pairs[pair]} ({last_posts.get(pair, 'not posted')})" for pair in candidate_pairs]))
    logging.info(
        f"Chose pair {chosen_pair} with market volume {top_pairs[chosen_pair]}")

    return chosen_pair, top_pairs[chosen_pair]


@handle_mongodb_errors
def get_markets(client, pair, granularity="1h"):
    """Returns all markets with volumes for a given pair.

    Arguments:
        client (pymongo.MongoClient): A database client.
        pair (str): A string representing a trading pair.
        granularity (str): Granularity of aggregated data. Equals '1h' by default.
    Returns:
        dict: Keys are market's names, values are their volumes.
    """

    logging.info('Getting markets for the chosen pair...')
    pair_symbol, pair_base = pair.lower().split('-')
    markets_cursor = client['metrics']['ohlcv_db'].aggregate([
        {
            '$match': {
                'pair_base': pair_base, 
                'pair_symbol': pair_symbol,
                'granularity': granularity
            }
        }, {
            '$sort': {
                'timestamp': -1
            }
        }, {
            '$group': {
                '_id': '$marketVenue', 
                'volume': {
                    '$first': '$volume'
                }
            }
        }, {
            '$project': {
                'volume': {
                    '$convert': {
                        'input': '$volume', 
                        'to': 'double'
                    }
                }
            }
        }
    ])

    markets = {}
    for doc in markets_cursor:
        markets[doc['_id']] = doc['volume']

    logging.info('Got markets successfully')
    logging.debug('Markets: %s', str(markets))

    return markets


@handle_mongodb_errors
def get_pair_thread(client, pair):
    """Finds the first tweet about given pair.

    Arguments:
        client (pymongo.MongoClient): a database client.
        pair (str): a string representation of a trading pair.
    Returns:
        int: first tweet's id if it exists, None otherwise.
    """

    result = client['metrics']['posts_db'].aggregate([
        {
            '$match': {
                'pair': pair, 
                'tweet_id': {
                    '$exists': True
                }
            }
        }, {
            '$sort': {
                'time': -1
            }
        }, {
            '$group': {
                '_id': '$pair', 
                'tweet_id': {
                    '$first': '$tweet_id'
                }
            }
        }
    ])
    thread = list(result)

    if len(thread) == 0:
        logging.info("Pair's thread not found, a new one will be created")
        return None
    logging.info("Found pair's thread")
    return thread[0]["tweet_id"]


@handle_mongodb_errors
def save_tweet(client, text, tweet_id, pair):
    """Saves a given tweet to posts_db.

    Arguments:
        client (pymongo.MongoClient): a database client.
        text (str): tweet's text.
        tweet_id (int): tweet's id.
        pair (str): a string representation of a trading pair.
    """

    client["metrics"]["posts_db"].insert_one({
        "time": datetime.utcnow(),
        "pair": pair,
        "tweet_id": str(tweet_id),
        "tweet_text": text
    })
    logging.info("Successfully saved tweet to posts_db")
