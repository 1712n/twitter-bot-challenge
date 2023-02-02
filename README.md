# Twitter Bot Challenge - archieve



## What's next?

If you are ready with your branch - create a pull request and assign it to @sofiasedlova and @darknessest for review. As soon as we get a good enough solution from a candidate, we start the interviewing process.


### Github Secrets for Actions

#### Description

For this challenge you have a Authentication Tokens (Access Token and Secret) and Consumer Keys (API Key and Secret), 
so you can only use [OAuth 1.0a](https://developer.twitter.com/en/docs/authentication/oauth-1-0a) for authentication.

Authentication Tokens are stored in `TW_ACCESS_TOKEN` and `TW_ACCESS_TOKEN_SECRET` respectivelly.

Consumer Keys are stored in `TW_CONSUMER_KEY` and `TW_CONSUMER_KEY_SECRET` respectively.

The MongoDB credentials are stored in `MONGODB_USER` and `MONGODB_PASSWORD`, also the cluster's address is stored in `MONGO_DB_ADDRESS`.

Make sure you are using the latest [PyMongo](https://github.com/mongodb/mongo-python-driver) with srv support, you can install it with:

```bash
pip install -U 'pymongo[srv]'
```

You can find a Github Action template [here](.github/workflows/gh-action-template.yml), please make sure you copy it to your branch and change the name of the branch in the yaml file. This will help the action's execution. 


#### Usage

To pass Github Secrets to your action, you need to specify the secrets and their corresponding names like following:

```yaml
jobs:
  build:
    runs-on: ubuntu-latest

    steps:

      - name: Run Python Code
        env:
          MONGODB_USER: ${{ secrets.MONGODB_USER }}
          TW_BEARER_TOKEN: ${{ secrets.TW_BEARER_TOKEN }}
        run: |
            python3 main.py
```



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
