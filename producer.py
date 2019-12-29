from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API

import argparse
import json
import os


TRACK = "trump"

KAFKA_URL = "localhost:9092"

TWITTER_ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN', '')
TWITTER_ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET', '')
TWITTER_CONSUMER_KEY = os.environ.get('TWITTER_CONSUMER_KEY', '')
TWITTER_CONSUMER_SECRET = os.environ.get('TWITTER_CONSUMER_SECRET', '')


class ProducerTwitter(StreamListener):
    def __init__(self, topic_name=None, n_partition=3, n_replica=1, 
                 callback=None):
        self.topic_name = topic_name
        self.n_partition = n_partition
        self.n_replica = n_replica
        self.broker_properties = {
            'bootstrap.servers': f"PLAINTEXT://{KAFKA_URL}"
        }
        self.callback = callback

        self.client = AdminClient(self.broker_properties)
        self.create_topic()

        self.producer = Producer(
            self.broker_properties,
        )

    def create_topic(self):
        if self.__topic_is_exist(self.client, self.topic_name) is False:
            topics = [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.n_partition,
                    replication_factor=self.n_replica,
                ),
            ]

            futures = self.client.create_topics(topics)

            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"topic created: {topic}")
                except Exception as e:
                    print(f"failed to create topic {topic_name}: {e}")
                    raise

        else:
            print(f"{self.topic_name} is exist!")

    def __topic_is_exist(self, client, topic_name):
        cluster_metadata = client.list_topics(timeout=5)

        return cluster_metadata.topics.get(topic_name) is not None

    def on_data(self, data):
        # print(data)
        deserialized_data = json.loads(data)

        if "id_str" in deserialized_data:
            self.producer.produce(
                topic=self.topic_name,
                key=json.dumps(
                    {"id": deserialized_data["id_str"]},
                    ensure_ascii=False).encode('utf8'),
                value=json.dumps(
                    deserialized_data,
                    ensure_ascii=False).encode('utf8'),
                callback=self.callback,
            )
        else:
            print(deserialized_data)

        return True

    def on_error(self, status):
        print(status)

    def close(self):
        if self.producer is not None:
            self.producer.flush()


class Twitter():
    def __init__(self, token=TWITTER_ACCESS_TOKEN,
                 token_secret=TWITTER_ACCESS_TOKEN_SECRET,
                 consumer_key=TWITTER_CONSUMER_KEY,
                 consumer_secret=TWITTER_CONSUMER_SECRET):
        self.token = token
        self.token_secret = token_secret
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret

    @property
    def auth(self):
        auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.token, self.token_secret)

        return auth


class TwitterAPI(Twitter):
    @property
    def api(self):
        return API(self.auth)

    def get_timeline(self, limit=100):
        timeline = self.api.home_timeline(count=limit)

        return timeline


class TwitterStream(Twitter):
    def __init__(self, token=TWITTER_ACCESS_TOKEN,
                 token_secret=TWITTER_ACCESS_TOKEN_SECRET,
                 consumer_key=TWITTER_CONSUMER_KEY,
                 consumer_secret=TWITTER_CONSUMER_SECRET, listener=None):
        super().__init__(token, token_secret, consumer_key, consumer_secret)
        self.listener = listener

    @property
    def stream(self):
        return Stream(self.auth, self.listener)

    def track(self, track=None):
        self.stream.filter(track=track, languages=["en"])


def callback_stream_twitter(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


if __name__ == "__main__":
    topic_name = f"com.twitter.track.{TRACK.replace(' ', '_')}"

    try:
        producer = ProducerTwitter(
            topic_name=topic_name,
            callback=callback_stream_twitter,
        )
        TwitterStream(
            listener=producer,
        ).track(track=track)
    except KeyboardInterrupt as e:
        producer.close()

    except Exception as e:
        print(e)
