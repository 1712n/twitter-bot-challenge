It is an attempt to solve the problem: 
MarketCapBot - Automate posting on Twitter

# Task

https://github.com/1712n/challenge/issues/86

# Project structure
```
/core
    config.py       - Settings 
    log.py          - Logging
/db                 
    session.py      - mongodb connection
/doc                - Here will be dragons
/tests              - Here will be dragons

.env                - Environment variables
config.yaml         - Configuration not in environment
loggin_config.yaml  - Configuration file for logging
requirements.txt 
```

# Mongodb

db name: `metrics`

collection: ohlcv_db
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
  'volume': '8968'}
```

collection: posts_db
```json
{
  '_id': ObjectId('639532ca0d693dfae1b8b0a7'), 
  'time': datetime.datetime(2022, 12, 13, 12, 21), 
  'tweet_text': 'Top Market Venues for LINK-USDT:\nBinance: 66.64%\nHitbtc: 11.25%\nOkx: 9.6%\nHuobi: 2.82%\nKucoin: 2.8%\nOthers: 6.89%', 
  'pair': 'LINK-USDT'}
```

# Completed

- It has logging.
- It can query mongodb.
- It's able to get something.

# Last problem

# TODO


