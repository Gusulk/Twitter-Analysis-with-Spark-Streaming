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

    #Trending Hashtags
    words = tweetStream.flatMap(lambda line: line.split(" "))
    hashtags = words.filter(lambda w: w.startswith('#') and len(w) > 1).map(lambda x: (x.lower(), 1))
    trending_hashtags = hashtags.reduceByKeyAndWindow(lambda a, b: a+b, None, 60*60*24, 2).transform(lambda rdd: rdd.sortBy(lambda a: -a[-1]))
    trending_hashtags.pprint(10)


    #Trending Users Mentioned
    words = tweetStream.flatMap(lambda line: line.split(" "))
    users = words.filter(lambda w: w.startswith('@') and len(w) > 1).map(lambda x: (x.lower(), 1))
    trending_users = users.reduceByKeyAndWindow(lambda a, b: a + b, None, 60 * 60 * 24, 2).transform(lambda rdd: rdd.sortBy(lambda a: -a[-1]))
    trending_users.pprint(10)


    # def hashtag_to_flask(rdd):
    #     if not rdd.isEmpty():
    #         top = rdd.take(10)
    #
    #         labels = []
    #         counts = []
    #
    #         for label, count in top:
    #             labels.append(label)
    #             counts.append(count)
    #             print(labels)
    #             print(counts)
    #             request = {'label': str(labels), 'count': str(counts)}
    #             response = requests.post('http://0.0.0.0:5000/update', data=request)



    # trending.foreachRDD(hashtag_to_flask)

    # sentiment = tweetStream.map(lambda text: (cleanText(text), round(float(TextBlob(cleanText(text)).sentiment.polarity),2) ))
    # sentiment.pprint(10)





    # sentiment.pprint(10)  # print the current top ten hashtags to the console
    # sentiment.foreachRDD(sentiment_to_flask)  # send the current top 10 hashtags rdd to flask for visualisation

    # somar cada hasthtag no intervalo
    #tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
    # processa para cada rdd criado
    #tags_totals.foreachRDD(process_rdd)

    ssc.start()
    ssc.awaitTermination()

    #send_to_dashboard(sort)

