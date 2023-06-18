from datetime import timedelta

import pendulum

import setting
from components.elastic import ElasticSearch
from components.kafka import Kafka
from tasks.emotions import Emotions

default_args = {
    "owner": "saeed mouzarmi",
    "depends_on_past": False,
    'start_date': pendulum.today('UTC').add(days=-3),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def transfer():
    elastic_client = ElasticSearch(es_host=setting.ES_HOST, es_port=setting.ES_PORT,
                                   es_username=setting.ES_USERNAME,
                                   es_password=setting.ES_PASSWORD, es_index=setting.ES_INDEX)
    latest_id = None
    while True:
        if latest_id is not None:
            elastic_data = elastic_client.receive_data(query={
                "query": {
                    "match": {
                        "id": latest_id
                    }
                },
                "_source": ["id", "@timestamp"]
            })
            gte = elastic_data[0]["_source"]["@timestamp"]
        else:
            gte = "2022-05-01T00:00:00"

        elastic_data = elastic_client.receive_data(query={
            "size": setting.ELASTIC_READ_SIZE,
            "sort": {
                "published_at": 'desc'
            },
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "exists": {
                                "field": "lf_emotion"
                            }
                        }
                    ],
                    "filter": {
                        "range": {
                            "published_at": {
                                "gte": gte,
                                "lt": "2022-06-22T00:00:00"
                            }
                        }
                    }
                }
            },
            "_source": ["id", "published_at", "full_text"]
        })
        latest_id = elastic_data[-1]["_source"]["id"]
        tran_data = [item["_source"] for item in elastic_data]
        batch_size = int(setting.BATCH_SIZE)
        while len(tran_data):
            init_data = tran_data[:batch_size]
            trasnfer_batch(init_data)
            del tran_data[:batch_size]


def trasnfer_batch(batch):
    kafka_client = Kafka(BOOTSTRAP=setting.KAFKA_BOOTSTRAP, TOPIC=setting.KAFKA_TOPIC,
                         USERNAME=setting.KAFKA_USERNAME, PASSWORD=setting.KAFKA_PASSWORD)
    emotion_client = Emotions(setting.EMOTION_URL, setting.EMOTION_TOKEN)
    emotions = emotion_client.get_emotions([x.pop("full_text") for x in batch])
    for idx, doc in enumerate(batch):
        batch[idx]["lf_emotion"] = emotions[idx]
    kafka_client.insert_data(batch)

    return batch


transfer()
