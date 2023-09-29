import time
import requests
import logging

from google.protobuf.json_format import MessageToJson
from confluent_kafka import Producer

import gtfs_realtime_pb2
from config import config

class MTARealTimeProducer(object):
    def __init__(self):
        self.mta_api_key = config["mta_api_key"]
        self.mta_api_url = config["mta_api_url"]
        self.kafka_topic = config["kafka_topic"]
        self.kafka_producer = Producer(config["kafka_producer"])

    def produce_trip_updates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(
            url=self.mta_api_url,
            headers={
                'x-api-key': self.mta_api_key
            }
        )
        feed.ParseFromString(response.content)

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                update_json = MessageToJson(entity.trip_update)
                self.kafka_producer.produce(
                    self.kafka_topic, update_json.encode('utf-8'))

        self.kafka_producer.flush()

    def run(self):
        while True:
            self.produce_trip_updates()
            time.sleep(1)

if __name__ == "__main__":
    instance = MTARealTimeProducer()
    instance.run()