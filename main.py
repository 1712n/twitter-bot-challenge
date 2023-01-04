import re
import datetime
import pymongo
import tweepy
import pandas as pd
import numpy as np

# Set environment variables
CONSUMER_KEY = "NnPL65juj8nstnd6x5t4tECun"
CONSUMER_SECRET = "i0yD4lmam6mBDqTaLlCOSQ5DdP38yj8yeqZ2ezNy3GRHYw4Zku"
ACCESS_TOKEN = "1600087262909317120-WaeIU8aBbV0QkyML1U3xtzZP1NHeAO"
ACCESS_TOKEN_SECRET = "uNdcHPWMzR8uruFHoXEFKylue5VmOExki4PZn3omw9x3U"

user = "twitter-bot-challenge-user"
password = "1Dci5pk0UHGBUzpN"
cluster_address = "loadtests.mjmdg.mongodb.net"

# Get the URI to MongoDB database
mongodb_uri = (
    "mongodb+srv://"
    + user
    + ":"
    + password
    + "@"
    + cluster_address
    + "/test?retryWrites=true&w=majority"
)

# Connect to MongoDB
client = pymongo.MongoClient(mongodb_uri)
db = client["metrics"]
ohlcv_collection = db["ohlcv_db"]
posts_collection = db["posts_db"]

# Get DataFrame with top pairs
def get_top_pairs_df(collection):
    top_pairs = list(collection.find().sort("volume", -1).limit(100))
    df_top_pairs = pd.DataFrame(top_pairs)
    df_top_pairs['pair'] = df_top_pairs['pair_symbol'] + '-' + df_top_pairs['pair_base'] 
    return df_top_pairs
  
def get_value(clm_1, clm_2):
    if clm_1 != clm_1:
        return clm_2
    else:
        return clm_1
      
def get_all_the_pairs(collection):
    posts = list(collection.find())
    df_posts = pd.DataFrame(posts)
    df_posts['pair'] = re.findall('(\w+-\w+)', df_posts['pair'].to_string())
    list_of_pairs = [s.lower() for s in list(df_posts['pair'].unique())]
    return list_of_pairs
  
# Get results by the oldest timestamp with corresponding volumes
def get_the_oldest_posts_df(collection):
    posts = list(collection.find())
    df_posts = pd.DataFrame(posts)
    df_posts['time_combined'] = df_posts.apply(lambda x: get_value(x['timestamp'], x['time']), axis=1)
    df_posts['text_combined'] = df_posts.apply(lambda x: get_value(x['message'], x['tweet_text']), axis=1)
    df_posts['pair'] = re.findall('(\w+-\w+)', df_posts['pair'].to_string().lower())
    df_posts = df_posts.sort_values(['pair', 'time_combined'], ascending=[False, False]).groupby(df_posts['pair']).first().reset_index(drop=True)
    return df_posts  
  
# Select the pair with the biggest volume
def selecting_pair_to_post(top_pairs, latest_posts):
    merged = top_pairs.merge(latest_posts, how='left', on='pair')
    merged = merged.sort_values(['time_combined'], ascending=[False])[0:5]
    pair_to_post = merged[merged['volume'] == merged['volume'].max()]['pair'].item()
    return pair_to_post
  
# Get market top for selected pair
def composing_message(df_top_pairs, pair_to_post):
    df_top_pairs = df_top_pairs[df_top_pairs['pair'] == pair_to_post]
    df_top_pairs['volume'] = df_top_pairs['volume'].astype('float')
    total = df_top_pairs['volume'].sum()
    df_top_pairs_agg = df_top_pairs.groupby(['marketVenue']).agg({'volume' : ['sum']}).reset_index()
    df_top_pairs_agg.columns = ['_'.join(i).rstrip('_') for i in df_top_pairs_agg.columns.values]
    df_top_pairs_agg['percentage'] = round(df_top_pairs_agg['volume_sum']*100/total, 2)
    df_top_pairs_agg = df_top_pairs_agg.sort_values(['percentage'], ascending=[False]).drop(['volume_sum'], axis=1)
    df_top_pairs_agg['percentage'] = df_top_pairs_agg['percentage'].astype('str')

    messages = []
    for index, row in df_top_pairs_agg.iterrows():
        message = row['marketVenue'].title() + ' ' + row['percentage'] + '%'
        messages += [message]
        message_to_post = "Top Market Venues for " + pair_to_post.upper() + ":/n" + '/n'.join(messages)

# Connect to Twitter
client = tweepy.Client(consumer_key=CONSUMER_KEY,
                       consumer_secret=CONSUMER_SECRET,
                       access_token=ACCESS_TOKEN,
                       access_token_secret=ACCESS_TOKEN_SECRET)

def post_new_tweet(client, response):
    client.create_tweet(text=response)

def post_reply(client, response, tweet_id):
    client.create_tweet(text=response, in_reply_to_tweet_id=tweet_id)
    
def post_message_to_db(db, pair, message):
    post = {
        "pair": pair,
        "tweet_text": message,
        "time": datetime.datetime.fromisoformat(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M"))
    }
    db.posts().insert_one(post)
    
if __name__ == '__main__':

    try:
        top_pairs = get_top_pairs_df(ohlcv_collection)
    except Exception as e:
        top_pairs = None

    try:
        latest_pairs = get_the_oldest_posts_df(posts_collection)
    except Exception as e:
        latest_pairs = None

    pair_to_post = selecting_pair_to_post(top_pairs, latest_pairs)

    try:
        message_to_post = composing_message(top_pairs, pair_to_post)
    except Exception as e:
        message_to_post = None


    if not pair_to_post in get_all_the_pairs(posts_collection):
        try:
            post_new_tweet(client, message_to_post)
        except Exception as e:
            pass
    else:
        df = pd.DataFrame(list(posts_collection.find()))
        tweet_id = df[df['pair'] ==  pair_to_post]['tweet_id']
        try:
            post_reply(client, message_to_post, tweet_id)
        except Exception as e:
            pass
        
    try:
        post_message_to_db(db, pair_to_post, message)
    except Exception as e:
        pass            
