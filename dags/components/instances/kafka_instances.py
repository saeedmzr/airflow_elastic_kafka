from components.kafka import Kafka


class Test(Kafka):
    _connection_name = 'kafka_connection1'
    _topic = 'test_instagram_tag'