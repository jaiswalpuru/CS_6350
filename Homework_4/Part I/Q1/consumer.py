import os
import sys
import json
from elasticsearch import Elasticsearch
from pyspark import SparkContext, SparkConf
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

es = Elasticsearch("http://localhost:9200")

def get_sentiment_val(text):
	s_a = SentimentIntensityAnalyzer()
	polarity = s_a.polarity_scores(text)
	if polarity["compound"] < 0:
		return "negative"
	elif polarity["compound"] > 0:
		return "positive"
	else:
		return "neutral"


def get_hash_tag(text):
	if "blacklivesmatter" in text.lower():
		return "#blacklivesmatter"
	elif "covid19" in text.lower():
		return "#covid19"
	elif "corona" in text.lower():
		return "corona"
	else:
		return "others"

def analyze(time, rdd):
	t = rdd.collect()
	for i in t:
		es.index(index="hash_tags_sentiment_analysis", document=i)

if __name__ == '__main__':
	spark = SparkSession.builder.appName("Sentiment_Analysis").config("spark.eventLog.enabled", "false").getOrCreate()
	sc = SparkContext.getOrCreate()
	topics = 'tweets_sentiment'
	ssc = StreamingContext(sc, 20)

	consumer = KafkaUtils.createDirectStream(
		ssc, [topics], {"metadata.broker.list":'localhost:9092'})
	rdds = consumer.map(lambda x : str(x[1].encode('ascii','ignore'))).map(
		lambda x: (x, get_sentiment_val(x), get_hash_tag(x))).map(
		lambda x: {"tweet": x[0], "sentiment": x[1], "hash_tag": x[2]})

	rdds.foreachRDD(analyze)
	ssc.start()
	ssc.awaitTermination()