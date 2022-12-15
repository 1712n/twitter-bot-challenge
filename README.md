# Twitter Bot Challenge

### Github Secrets for Actions

#### Description

For this challenge you have a bearer token for Twitter Developer Account and credentials to access MongoDB Cluster. 

The bearer token is stored in an environment variable `TW_BEARER_TOKEN`.

The MongoDB credentials are stored in `MONGODB_USER` and `MONGODB_PASSWORD`, also the cluster's address is stored in `MONGO_DB_ADDRESS`.

Make sure you are using the latest [PyMongo](https://github.com/mongodb/mongo-python-driver) with srv support, you can install it with:

```bash
pip install -U 'pymongo[srv]'
```

#### Usage

You can access environment variables in Python like that:

```python
import os

bearer_token = os.environ["TW_BEARER_TOKEN"]
```


URI for connecting to the MongoDB cluster can be constructed in the following way:

```python
from pymongo import MongoClient

user = os.environ["MONGODB_USER"]
password = os.environ["MONGODB_PASSWORD"]
address = os.environ["MONGO_DB_ADDRESS"]

uri = f"mongodb+srv://{user}:{password}@{address}"
client = MongoClient(uri)

```
