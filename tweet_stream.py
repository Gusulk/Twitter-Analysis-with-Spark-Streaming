import socket
import sys
import json
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import config_twitter
import send_spark
import unicodedata


access_token = config_twitter.access_token
access_token_secret = config_twitter.access_token_secret
consumer_key = config_twitter.consumer_key
consumer_secret = config_twitter.consumer_secret

# StreamListener
class TweetListerner(StreamListener):

    def __init__(self, c):
        self.socket = c

    def on_data(self, raw_data):
        self.send_data(raw_data)
        return True

    def send_data(self, raw_data):
        try:

            data = json.loads(raw_data)
            dc = unicodedata.normalize('NFKD', data['text']).encode('ascii', 'ignore')
            self.socket.send(dc)
            #print(dc)
            #print(data['text'].encode('utf-8'))
            return True
        except BaseException as e:
              print("Error on_data: %s" % str(e))

    def on_error(self, status_code):
        if status_code == 420:
            # Retorna False se a stream for desconectada
            return False



class TweetStream():

    def __init__(self, auth, listener):
        self.stream = Stream(auth=auth, listener=listener)

    def start(self, keyword_list):
        self.stream.filter(track=keyword_list, languages = ['en'], locations = [-171.791110603, 18.91619, -66.96466, 71.3577635769])

if __name__ == "__main__":

    c = send_spark.createSocket('localhost', 9009)
    listener = TweetListerner(c)

    # autenticando na api do twitter
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = TweetStream(auth, listener)
    stream.start(['covid'])

    #languages = ['pt'], locations = [-73.9872354804, -33.7683777809, -34.7299934555, 5.24448639569]
    #languages = ['en'], locations = [-171.791110603, 18.91619, -66.96466, 71.3577635769]