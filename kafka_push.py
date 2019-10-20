import pykafka
import json
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import config_twitter

access_token = config_twitter.access_token
access_token_secret = config_twitter.access_token_secret
consumer_key = config_twitter.consumer_key
consumer_secret = config_twitter.consumer_secret

# autitencicando na api do twitter
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)


class TweetPush(StreamListener):
    def __init__(self):
        self.client = pykafka.KafkaClient("localhost:9092")
        self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()

    def on_data(self, raw_data):
        self.producer.produce(bytes(raw_data, "ascii"))
        return True

    def on_error(self, status_code):
        print(status_code)
        return True


twitter_stream = Stream(auth, TweetPush())

twitter_stream.filter(locations=[-73.9872354804, -33.7683777809, -34.7299934555, 5.24448639569], languages=['pt'])
