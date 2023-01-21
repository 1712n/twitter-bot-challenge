It's just a pile of mongodb queries. 


```json lines
db.getCollection("posts_db").aggregate(
[
    { "$match": { "pair": { "$in": [ "LINK-USDT", "shib-usdt" ] } } },
    { "$sort": {"pair": 1, "time": -1}},
    { "$group": {
        "_id": { 
            "pair": "$pair", 
            "tweet_text": "$tweet_text",
            "lastTime": {"$last": "$time"},
        },
    },
    }, 
]
)
```

```
db.getCollection("posts_db").aggregate(
[
    { "$match": { "pair": { "$in": [ "LINK-USDT", "shib-usdt" ] } } },
    { "$sort": {"time": -1}},
    { "$group": {
        "_id": { 
            "pair": "$pair", 
            "time": "$time", 
            "tweet_text": "$tweet_text",
        },
    },
    }, 
]
)
```

```
db.getCollection("posts_db").aggregate(
[
    { "$match": { "pair": { "$in": [ "LINK-USDT", ] } } },
    { "$sort": {"time": -1}},
]
)
```

```python
db.getCollection("posts_db").aggregate(
[
    { "$match": { "pair": { "$in": [ "LINK-USDT", "LTC-USDT"] } } },
    { "$sort": {"pair": 1, "time": -1}},
    { "$group": {
        "_id": { 
            "pair": "$pair", 
        },
        "latestTime": { "$first": "$time"}, 
      },
    }, 
]
)
```


```python
db.getCollection("posts_db").aggregate(
[
    { "$match": { "pair": { "$in": [ "LINK-USDT", "LTC-USDT"] } } },
    { "$sort": {"pair": 1, "time": -1}},
    { "$group": {
        "_id": { 
            "pair": "$pair", 
        },
        "latestTime": { "$first": "$time"}, 
      },
    }, 
    { "$sort": {"time": 1}},
    {"$limit": 1},
]
)
```


```python
db.getCollection("pairs_db").aggregate(
[{'$match': {'pair': 'SHIB-USDT'}},
 ({'$group': {'_id': {'marketVenue': '$marketVenue'}, 'latestTime': {'$first': '$time'}}},),
 {'$sort': {'time': 1}},
 {'$limit': 1}]    
)
```

# Get percentage

Get venues shares in %:
Link to HOWTO: https://www.mongodb.com/community/forums/t/statistics-on-a-collection-by-the-latest-value-of-a-field/158125/2

```python
db.getCollection("ohlcv_db").aggregate(
[
    { "$project": {
      'pair': {
        '$toUpper':  
            {'$concat': ['$pair_symbol', '-', '$pair_base']},
        },
      'marketVenue': '$marketVenue',
      'volume': '$volume',
      }
    },
  {'$match': {'pair': 'SHIB-USDT'}},
  {
    '$group': {
      '_id': '$marketVenue', 
      'venueVolume': {"$sum": {"$toDouble": "$volume"}},
    }
  }, {
    '$group': {
      '_id': null, 
      'total': {
        '$sum': '$venueVolume'
      }, 
      'data': {
        '$push': '$$ROOT'
      }
    }
  }, {
    '$unwind': {
      'path': '$data'
    }
  }, {
    '$project': {
      '_id': 0, 
      'marketVenue': '$data._id', 
      'share': '$data.venueVolume', 
      'percentage': {
        '$multiply': [
          100, {
            '$divide': [
              '$data.venueVolume', '$total'
            ]
          }
        ]
      }
    }
  },
    {'$sort': {'percentage': -1}},
  { '$limit': 5 },
]
)
```

Get venues volumes:

```
db.getCollection("ohlcv_db").aggregate(
[
    { "$project": {
      'pair': {
        '$toUpper':  
            {'$concat': ['$pair_symbol', '-', '$pair_base']},
        },
      'marketVenue': '$marketVenue',
      'volume': '$volume',
      }
    },
  {'$match': {'pair': 'SHIB-USDT'}},
  {
    '$group': {
      '_id': '$marketVenue', 
      'venueVolume': {"$sum": {"$toDouble": "$volume"}},
    }
  }, 
]
)
```

```python
"$concat": [ 
    { "$substr": [ 
        { 
            "$multiply": [ 
                { 
                    "$divide": [ 
                        "$count", 
                        {"$literal": nums }
                    ] 
                }, 
                100 
            ] 
        }, 
        0,2 
    ] 
    }, 
    "", 
    "%" 
]
```

----

Get venues shares in % with round:

```python
db.getCollection("ohlcv_db").aggregate(
[
    { "$project": {
      'pair': {
        '$toUpper':  
            {'$concat': ['$pair_symbol', '-', '$pair_base']},
        },
      'marketVenue': '$marketVenue',
      'volume': '$volume',
      }
    },
  {'$match': {'pair': 'SHIB-USDT'}},
  {
    '$group': {
      '_id': '$marketVenue', 
      'venueVolume': {"$sum": {"$toDouble": "$volume"}},
    }
  }, {
    '$group': {
      '_id': null, 
      'total': {
        '$sum': '$venueVolume'
      }, 
      'data': {
        '$push': '$$ROOT'
      }
    }
  }, {
    '$unwind': {
      'path': '$data'
    }
  }, {
    '$project': {
      '_id': 0, 
      'marketVenue': '$data._id', 
      'share': '$data.venueVolume', 
      'percentage': {
        '$multiply': [
          100, {
            '$divide': [
              '$data.venueVolume', '$total'
            ]
          }
        ]
      }
    }
  },
    {'$sort': {'percentage': -1}},
  { '$limit': 5 },
    {"$project": {
        "marketVenue": "$marketVenue",
        "percentage": {"$round": ["$percentage", 2]}
    }}
]
)
```

Check if pair in posts:

```python
db.getCollection("posts_db").aggregate(
[
    { "$match": { "pair": "LINK-USDT" } },
    { '$limit': 1 },
]
)
```

```python
db.getCollection("posts_db").aggregate(
[
    { "$match": { "pair": "LINK-USDT" } },
    { '$limit': 1 },
    { "$count": "count" },
]
)
```

```python
db.getCollection("posts_db").find({ "pair": "LINK-USDT" }).limit(1)
```
----

Get tweet_id:

```python
db.getCollection("posts_db").aggregate(
[
    {
        "$match": {
            "$and": [
                {"pair": "LINK-USDT"},
                {"tweet_id": {"$exists": "true"}},
                {"tweet_id": {"$nin": [null, ""]}},
            ]
        }
    },
    { "$sort": {"time": -1}},
    { "$limit": 1},
]
)
```

```python
db.getCollection("posts_db").insert_one(
    {
        "pair": "SHIB-USDT", "tweet_id": 1616496140731535362, 
        "text": "Top Market Venues for SHIB-USDT:"
     }
)
```