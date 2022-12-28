import datetime
import logging
from typing import Dict

import db
import pymongo
import tweepy
import twitter
from db import mongodb_read_error_handler, mongodb_write_error_handler


class MarketCapBot:
    """ 
        Class to represent the MarketCapBot
    """

    def __init__(
        self,
        ohlcv_db:pymongo.collection.Collection,
        posts_db:pymongo.collection.Collection,
        twitter_client:tweepy.client.Client
        ):

        """
            Constructor for MarketCapBot
        """
        logging.info("Initializing MarketCapBot...")
        self.ohlcv_db = ohlcv_db
        self.posts_db = posts_db
        self.twitter_client = twitter_client
    
    def get_pair_to_post(self)-> str:
        """
            Method to return the pair to post
        """
        top_pairs = db.get_top_pairs(max_num=100)
        latest_posted_pairs = db.get_latest_posted_pairs(top_pairs,max_num=5)
        pair_to_post = db.get_pair_to_post(top_pairs,latest_posted_pairs)
        return pair_to_post
        
    @mongodb_read_error_handler
    def _get_message_dict(self,pair:str=None,pair_symbol:str=None,pair_base:str=None) -> Dict:
        """
            Method to return a dictionary of marketVenue:volume percentage for a given pair
        """
        if pair is None and (pair_symbol is None or pair_base is None):
            raise ValueError("Either pair or pair_symbol and pair_base must be provided")
        elif pair is None:
            pair = self.get_pair_to_post()
        elif pair is not None:
            try:
                pair_symbol = pair.split('-')[0].lower()
                pair_base = pair.split('-')[1].lower()
            except:
                raise ValueError("Invalid pair provided, must be in the format 'SYMBOL-BASE")
            
        ohlcv_documents_for_pair = self.ohlcv_db.find(
            {'pair_symbol':pair_symbol,'pair_base':pair_base}
            ).sort('timestamp', pymongo.DESCENDING)

        latest_date = None
        message_dict = {
            'others':0
        }
        for item in ohlcv_documents_for_pair:
            
            if latest_date is None:
                latest_date = item['timestamp']
            if item['timestamp'] == latest_date:
                if item['marketVenue'] not in message_dict:
                    if len(message_dict) <= 5:
                        # key for top 5 marketVenues
                        message_dict[item['marketVenue']] = float(item['volume'])
                    else:
                        # store the rest in 'others'
                        message_dict['others'] += float(item['volume'])
                    continue    
                message_dict[item['marketVenue']] += float(item['volume'])
            else:
                break
        total_volume = sum(message_dict.values())
        message_dict = {key:round((value/total_volume)*100,2) for key,value in message_dict.items()}
        return message_dict

    def compose_message(self,pair:str=None,message_dict:dict=None) -> str:
        """
            method to compose a message for a given pair and message_dict
        """

        if pair is None:
            pair = self.get_pair_to_post()
        if message_dict is None:
            message_dict = self._get_message_dict(pair=pair)
        
        logging.info("Composing message...")  
        message = f"Top Market Venues for {pair}:\n"
        for marketVenue,percentage in sorted(message_dict.items()):
            message += f"{marketVenue.capitalize()}: {percentage}%\n"
        logging.info("Message composed!")
        return message

    @mongodb_write_error_handler
    def _save_message_to_db(self,pair:str,tweet_id:str=None,message:str=None) -> None:
        """
            Method to save message to the database
        """
        if message is None:
            message = self.compose_message(pair=pair)
        logging.info("Saving message to database...")
        current_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        post_document = {
            'pair':pair,
            'tweet_text':message,
            'time':datetime.datetime.fromisoformat(current_time)
        }
        if tweet_id is not None:
            post_document['tweet_id'] = tweet_id
        self.posts_db.insert_one(post_document)

    @mongodb_read_error_handler
    def post_message(self,pair:str=None,message:str=None) -> None:
        """
            Method to post a message to twitter
        """
        if pair is None:
            pair = self.get_pair_to_post()
        if message is None:
            message = self.compose_message(pair=pair)

        pair_main_post = self.posts_db.find_one({'pair':pair,'tweet_id':{'$exists':True}})
        tweet_id = None
        try:
            logging.info("Posting message...")
            if pair_main_post is None:
                response = self.twitter_client.create_tweet(text=message)
                tweet_id = response.data.get('id')
            else:
                self.twitter_client.create_tweet(
                    text=message,
                    in_reply_to_tweet_id=pair_main_post['tweet_id']
                    )
        except tweepy.TweepyException as e:
            logging.error("%s"%e)

        except Exception as e:
            logging.error("Error posting message %s"%e)

        else:
            logging.info("Message posted!")
            if tweet_id:
                self._save_message_to_db(pair=pair,tweet_id=tweet_id,message=message)
            else:
                self._save_message_to_db(pair=pair,message=message)
            return

        logging.info("Message not posted!")


    def ping(self) -> None:
        """
            Method to ping the bot
        """
        try:
            logging.info("Pinging bot...")
            self.twitter_client.create_tweet(text="Pong!")
            logging.info("Bot pinged!")
        except tweepy.TweepyException as e:
            logging.error("%s"%e)
            return None
        except Exception as e:
            logging.error("Something went wrong while pinging the bot:%s"%e)
            return None
        

if __name__ == "__main__":
    
    bot = MarketCapBot(db.ohlcv_db,db.posts_db,twitter.client)
    # bot.ping()
    print(bot.compose_message())