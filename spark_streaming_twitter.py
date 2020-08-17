from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import desc
from pyspark.streaming.kafka import KafkaUtils
import json
from textblob import TextBlob
import re
import requests
import sys

def cleanText(tweet):
    tweet = re.sub(r'https?://\S+', '', tweet)
    return tweet

def tweet_sentiment(text):
  polarity = round(float(TextBlob(text).sentiment.polarity),2)
  if polarity > 0.10:
        return ('positive')
  elif polarity < -0.10:
        return ('negative')
  else: return ('neutral')


if __name__ == "__main__":

    conf = SparkConf().setAppName("SparkStreaming").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    #sc = SparkContext(appName="SparkStreaming")
    sc.setCheckpointDir("checkpoint_SparkStreaming")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 2)

    #DStream
    #tweetStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter': 1})

    tweetStream = ssc.socketTextStream('localhost', 9009)
    tweetStream.persist(StorageLevel.MEMORY_AND_DISK)

    words = tweetStream.flatMap(lambda line: line.split(" "))

    #Trending Hashtags
    hashtags = words.filter(lambda w: w.startswith('#') and len(w) > 1).map(lambda x: (x.lower(), 1))
    trending_hashtags = hashtags.reduceByKeyAndWindow(lambda a, b: a+b, None, 60*60*24, 2).transform(lambda rdd: rdd.sortBy(lambda a: -a[-1]))
    trending_hashtags.pprint(10)


    #Trending Users Mentioned
    users = words.filter(lambda w: w.startswith('@') and len(w) > 1).map(lambda x: (x.lower(), 1))
    trending_users = users.reduceByKeyAndWindow(lambda a, b: a + b, None, 60 * 60 * 24, 2).transform(lambda rdd: rdd.sortBy(lambda a: -a[-1]))
    trending_users.pprint(10)

    #Sentiment Tweets
    sentiment = tweetStream.filter(lambda t: len(t) > 10).map(lambda text: tweet_sentiment(text)).filter(lambda w: len(w) > 1).map(lambda x: (x, 1))
    count_sentiment = sentiment.reduceByKeyAndWindow(lambda a, b: a + b, None, 60 * 60 * 24, 2).transform(lambda rdd: rdd.sortBy(lambda a: -a[-1]))
    count_sentiment.pprint(3)

    ssc.start()
    ssc.awaitTermination()


