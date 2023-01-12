from pymongo import MongoClient
import os
from time import time

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]
uri = f"mongodb+srv://{user}:{password}@{address}"

client = MongoClient(uri)
db = client['metrics']
col_ohlcv = db['ohlcv_db']
col_posts = db['posts_db']

def basic_agregation() -> "CommandCursor":
    """
    performs a basic aggregation to get a dataset to work with. Order:
    1) Gets top 100 pair by compound volume
    2) Joins every pair with the latest correlated post
    """
    result = col_ohlcv.aggregate([
    {
        '$group': {
            '_id': {
                'pair_base': '$pair_base', 
                'pair_symbol': '$pair_symbol'
            }, 
            'compound_volume': {
                '$sum': {
                    '$toDouble': '$volume'
                }
            }
        }
    }, {
        '$sort': {
            'compound_volume': -1
        }
    }, {
        '$limit': 100
    }, {
        '$lookup': {
            'from': 'posts_db', 
            'let': {
                'ohlcv_pair_base': '$_id.pair_base', 
                'ohlcv_pair_symbol': '$_id.pair_symbol'
            }, 
            'pipeline': [
                {
                    '$group': {
                        '_id': '$pair', 
                        'latest_post_time': {
                            '$max': '$time'
                        }
                    }
                }, {
                    '$match': {
                        '$expr': {
                                    '$eq': [
                                        {
                                            '$concat': [
                                                {
                                                    '$toUpper': '$$ohlcv_pair_symbol'
                                                }, '-', {
                                                    '$toUpper': '$$ohlcv_pair_base'
                                                }
                                            ]
                                        }, '$_id'
                                    ]
                                }
                    }
                }
            ], 
            'as': 'latest_post'
        }
    }
    ])
    return result

if __name__ == "__main__":
    start_time = time()
    aggregation_result = basic_agregation()
    elapsed_time = (time() - start_time)*1000
    print("elapsed_time: " + str(elapsed_time))

    with open(".\output_files\ggregation_result.txt", mode="w") as file:
        for doc in aggregation_result:
            file.write(doc.__str__())
    
    # with open("aggregation_result.txt") as file:
    #     f_content = file.readlines()
    
    # print(len(f_content))