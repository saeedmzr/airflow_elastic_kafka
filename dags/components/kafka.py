from confluent_kafka import Producer
import json

class Kafka:
    def __init__(self, BOOTSTRAP, TOPIC,USERNAME,PASSWORD):
        self.producer = Producer({
            'bootstrap.servers': BOOTSTRAP
        })
        self.kafka_topic = TOPIC

    def insert_data(self, datas):
        for data in datas:
            data['created_at'] = data['published_at']
            self.producer.produce(self.kafka_topic, value=json.dumps(data).encode('utf-8'))
        self.producer.flush()
