
"""API ACCESS KEYS"""

# access_token = ""
# access_token_secret = ""
# consumer_key = ""
# consumer_secret = ""

import config as cfg
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=cfg.es_brokers, 
    security_protocol='SASL_SSL', 
    sasl_mechanism='PLAIN',
    sasl_plain_username='token',
    sasl_plain_password=cfg.es_apikey,
    ) #Same port as your Kafka server


topic_name = "nowPlaying"


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(cfg.consumer_key, cfg.consumer_secret)
        auth.set_access_token(cfg.access_token, cfg.access_token_secret)

        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS() 
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["#NowPlaying"], stall_warnings=True, languages= ["en"])


class ListenerTS(StreamListener):
    def on_data(self, raw_data):
        print("enviando")
        producer.send(topic_name, str.encode(raw_data))
        print("enviado")
        return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()