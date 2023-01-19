Mongodb Time Series

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



