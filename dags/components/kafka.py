import json

from confluent_kafka import Producer

from setting import config


class Kafka:
    __slots__ = (
        "_connection_name",
        "_topic",
        "producer"
    )

    def __init__(self):
        bootstrap = config.get(self._connection_name, 'bootstrap')
        username = config.get(self._connection_name, 'username')
        password = config.get(self._connection_name, 'password')
        print(bootstrap)

        self.producer = Producer({
            'bootstrap.servers': bootstrap,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password,
        })

    def insert_data(self, data):
        print('insert data process', data)
        print('insert data process', type(data))
        self.producer.produce(self._topic, value=json.dumps(data).encode('utf-8'))
        print('inserted data')
