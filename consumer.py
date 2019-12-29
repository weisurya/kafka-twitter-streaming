from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING
from elasticsearch import Elasticsearch

import json
import logging
import time

TRACK = "trump"

KAFKA_URL = "localhost:9092"
KAFKA_POOL_TIMEOUT = 1.0


class TwitterConsumer():
    def __init__(self, group_id=None, topic_name=None, log=None, es=None):
        self.topic_name = topic_name
        self.group_id = group_id
        self.log = log
        self.es = es

        self.broker_properties = {
            'bootstrap.servers': KAFKA_URL,
            'group.id': self.group_id,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'
        }

        self.consumer = Consumer(self.broker_properties)

    def subscribe(self):
        self.consumer.subscribe(
            topics=[self.topic_name],
            on_assign=self._on_assign,
        )

        while True:
            try:
                message = self.consumer.poll(KAFKA_POOL_TIMEOUT)

                if message is None:
                    continue
                elif message.error():
                    self.log.error(f"Error: {message.error()}")
                    raise KafkaError(message.error())
                self.log.info(f"Message received: {message.key()}")
                # self.log.info(f"Message received: {message.key()} | {message.value()}")

                data = json.loads(message.value())

                self.es.index(
                    index=self.topic_name,
                    body= {
                        'id': data['id_str'],
                        'text': data['text'],
                        'source': data['source'],
                        'in_reply_to_status_id_str': data['in_reply_to_status_id_str'],
                        'lang': data['lang'],
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(data['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
                    },
                )

            except KafkaError as e:
                print("Error: {e}")

    def _on_assign(self, consumer, partitions):
        for partition in partitions:
            self.log.info(f"Set partition offset for {self.topic_name} to the beginning")

            partition.offset = OFFSET_BEGINNING
                

        logger.info("partitions assigned for %s", self.topic_name)
        consumer.assign(partitions)

    def close(self):
        self.consumer.close()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    es = Elasticsearch()

    group_id = "twitter"
    topic_name = "com.twitter.track.{TRACK}"

    try:
        consumer = TwitterConsumer(
            group_id=group_id,
            topic_name=topic_name,
            log=logger,
            es=es,
        )

        consumer.subscribe()

    except KeyboardInterrupt as e:
        consumer.close()
