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

Get all instruments/pairs:

```json
db.getCollection("ohlcv_db").aggregate(
  [
    { "$group":
    	{ "_id": "$market_id" } },
  ]
);

```

Get all granularity for instrument:

```json
db.getCollection("ohlcv_db").aggregate(
  [
    { "$match": { "market_id": "okx-ltc-usdt-f" } },
    { "$group":
    	{ "_id": "$granularity" } },
  ]
);

```

Get summary volumes for each instrument:

```json
db.getCollection("ohlcv_db").aggregate(
  [
    { $group:
    	{ 
    	  "_id": "$market_id" ,
    	  "total": { $sum: { $toDouble: "$volume"} }
    	},
    }
  ]
);
```

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

Get top 100 instruments by accumulated volume group by: marketVenue, market_id,
granularity:

```json
db.getCollection("ohlcv_db").aggregate(
  [
    { "$group":
    	{ 
    	  "_id": {"marketVenue": "$marketVenue", "market_id": "$market_id", "granularity": "$granularity" },
    	  "volume": { $sum: { $toDouble: "$volume"} },
    	},
    },
    { $sort: { "volume": -1 } },
    { $limit: 100 },
  ]
);
```
