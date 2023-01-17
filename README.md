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

{
  'timestamp': datetime.datetime(2023, 1, 15, 1, 15), 
  'market_id': 'okx-ltc-usdt-f', 
  'marketVenue': 'okx', 
  'high': '86.75', 
  'pair_symbol': 'ltc', 
  '_id': ObjectId('63c35506387a9b4f31f483f7'), 
  'pair_base': 'usdt', 
  'granularity': '1m', 
  'open': '86.6', 
  'close': '86.67', 
  'low': '86.6', 
  'volume': '560'}

{
  'timestamp': datetime.datetime(2023, 1, 15, 1, 16), 
  'market_id': 'okx-ltc-usdt-f', 
  'marketVenue': 'okx', 
  'high': '86.61', 
  'pair_symbol': 'ltc', 
  '_id': ObjectId('63c35544387a9b4f31f48556'), 
  'pair_base': 'usdt', 
  'granularity': '1m', 
  'open': '86.61', 
  'close': '86.5', 
  'low': '86.49', 
  'volume': '113'
}

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

{
  '_id': ObjectId('639532ca0d693dfae1b8b0aa'), 
  'time': datetime.datetime(2022, 12, 13, 19, 4), 
  'tweet_text': 'Top Market Venues for ETH-USDC:\nHitbtc: 35.19%\nHuobi: 20.92%\nOkx: 20.57%\nBybit: 11.08%\nKucoin: 8.22%\nOthers: 4.01%', 
  'pair': 'ETH-USDC'
}

{
  '_id': ObjectId('639532ca0d693dfae1b8b0ab'), 
  'time': datetime.datetime(2022, 12, 12, 23, 17), 
  'tweet_text': 'Top Market Venues for ETH-USD:\nCoinbase: 68.93%\nCrypto-com: 10.86%\nKraken: 5.81%\nBitfinex: 5.15%\nBitstamp: 4.69%\nOthers: 4.55%', 
  'pair': 'ETH-USD'
}
```

# Completed

- It has logging.
- It can query mongodb.
- It's able to get something.
- Github actions tested.
- Testing environment
  - [v] Multiple .env files and symlink for .env

# Last problem

# TODO

# Problems

- Tests?
  - pytest...
- Should I have a separate module/class for mongodb?
  - OK
- Should I check connection and wait in cycle?
  - Ok
- Correct way to check mongodb connection?
- Correct way to check mongodb db/collection read availability?
- Correct way to check mongodb db/collection write availability
