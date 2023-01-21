# Mongodb

db name: `metrics`

collection: ohlcv_db

# Mongodb Time Series

Fields:

- timeField
- metaField
- granularity

# Granularity

- seconds
- minutes
- hours

Granularity controls the bucket time span in which measurements 
with the same metaField can be stored and grouped together as one document.

# Insert

db.weather.insertMany([{}, {}, ...])

# Query

db.weather.find() - not efficient

# Bucketing Catalog

It's a core concept of Time Series.
Documents stored with the same `metaField` stored together.

# How to get data

## Our interest

Get all granularities:

```json
db.getCollection("ohlcv_db").aggregate(
  [
    { "$group":
        { "_id": "$granularity" } },
  ]
);
```

Get granularity for largest by volume pair:

```json
db.getCollection("ohlcv_db").aggregate(
[{'$project': {'granularity': '$granularity',
               'pair': {'$concat': ['$pair_symbol', '-', '$pair_base']},
               'volume': '$volume'}},
 {'$group': {'_id': {'granularity': '$granularity', 'pair': '$pair'},
             'volume': {'$sum': {'$toDouble': '$volume'}}}},
 {'$sort': {'volume': -1}},
 {'$limit': 1}]
);
```

# Contents

```json
{
  'timestamp': datetime.datetime(2023, 1, 15, 1, 0), 
  'market_id': 'okx-ltc-usdt-f', 
  'marketVenue': 'okx', 
  'high': '87.14', 
  'pair_symbol': 'ltc', 
  '_id': ObjectId('63c35fc6387a9b4f31f4c10f'), 
  'pair_base': 'usdt', 
  'granularity': '1h', 
  'open': '86.68', 
  'close': '86.86', 
  'low': '86.25', 
  'volume': '8968'
}

{
  'timestamp': datetime.datetime(2023, 1, 15, 1, 14), 
  'market_id': 'okx-ltc-usdt-f', 
  'marketVenue': 'okx', 
  'high': '86.63', 
  'pair_symbol': 'ltc', 
  '_id': ObjectId('63c354c9387a9b4f31f4826f'), 
  'pair_base': 'usdt', 
  'granularity': '1m', 
  'open': '86.51', 
  'close': '86.63', 
  'low': '86.51', 
  'volume': '90'}
```

collection: posts_db

```json
{
  '_id': ObjectId('639532ca0d693dfae1b8b0a7'), 
  'time': datetime.datetime(2022, 12, 13, 12, 21), 
  'tweet_text': 'Top Market Venues for LINK-USDT:\nBinance: 66.64%\nHitbtc: 11.25%\nOkx: 9.6%\nHuobi: 2.82%\nKucoin: 2.8%\nOthers: 6.89%', 
  'pair': 'LINK-USDT'
}

{
  '_id': ObjectId('639532ca0d693dfae1b8b0a9'), 
  'time': datetime.datetime(2022, 12, 13, 0, 58), 
  'tweet_text': 'Top Market Venues for BTC-USD:\nCoinbase: 68.98%\nBitstamp: 9.33%\nCrypto-com: 7.66%\nBinance-us: 5.28%\nKraken: 3.07%\nOthers: 5.68%', 
  'pair': 'BTC-USD'
}
```
