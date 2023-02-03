import tweepy
import configparser
import json

# import twitter keys and tokens
from transformers import pipeline
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

api_key = "SuyYRyiYZPZLBNdL6drST7FPS"
api_key_secret = "umOROTkPHqqQI937eB8yWTKViFElVFETUeFRxCeF2kXx6iF5Xw"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAOF%2BjQEAAAAAKQkc%2Bs%2BDmV3k7v3DnQPIPbO3NQQ%3DvsEjNzaz8ffJI7lymUi4SffXudOgT43bpNt52ji3mooMB8vROd"
access_token = "836210067867381760-Iy36zBmN314wpDjJKddULhXCzPBe1qb"
access_token_secret = "tfeTePgweTlEjMj8uzxLbiphnpqXmSWMHCIRcmKZNK1bF"

config = configparser.ConfigParser()
config.read('config.ini')
kafka_host = config["arguments"]["kafka_host"]+":"+config["arguments"]["kafka_port"]
topic = config["arguments"]["topic"]
producer = KafkaProducer(bootstrap_servers=kafka_host)

def perform_analysis(tweet):
	t_sentiment = classifier(tweet.text)
	sentiment = t_sentiment[0]["label"]
	return sentiment

class KafkaListener(tweepy.StreamingClient):
	
	def on_tweet(self, tweet):
		sentiment = perform_analysis(tweet)
		dictionary = {"id":tweet.id,"sentiment":sentiment}
		producer.send(topic, bytes(json.dumps(dictionary), 'utf-8'))
		print("Sentiment: " + sentiment);
		print("-"*50)
		producer.flush()
		

if __name__ == "__main__":
	

	classifier = pipeline('text-classification')
	printer = KafkaListener(bearer_token)

	rule = tweepy.StreamRule(value=config["arguments"]["search"])
	printer.add_rules(rule)
	 
	printer.filter()
