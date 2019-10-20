from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


def get_tweet_with_hashtag(tweet):
    data = json.loads(tweet)
    result = []
    try:
        if len(data['entities']['hashtags']) == 0:
            return ()

        for hashtag in data['entities']['hashtags']:
            #(hashtag.autor)
            result = ["#" + hashtag["text"], "@" + data['user']['screen_name']]
        return result

    except KeyError:
        print("Error key -> get_tweet_with_hashtag")

if __name__ == "__main__":

    sc = SparkContext(appName="SparkStreaming")
    ssc = StreamingContext(sc, 30)

    #DStream
    tweetStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter': 1})

    # return (hasgtag,user)
    lines = tweetStream.map(lambda x: get_tweet_with_hashtag(x[1])).filter(lambda x: len(x) > 0)

    #return (hastag,1)
    group = lines.map(lambda x: (x[0], 1))

    count_hashtags = group.reduceByKeyAndWindow(lambda x, y: int(x) + int(y), None, 30, 30)

    sort = count_hashtags.map(lambda x: (x[1], x[0])).transform(lambda rdd: rdd.sortByKey(False)).map(lambda x: (x[1], x[0]))

    sort.pprint(30)

    ssc.start()
    ssc.awaitTermination()
