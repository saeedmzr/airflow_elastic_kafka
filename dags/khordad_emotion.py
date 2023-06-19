import json
import time
from datetime import timedelta
from multiprocessing import Process

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

print(setting.BATCH_SIZE)


def transfer():
    elastic_client = ElasticSearch(es_host=setting.ES_HOST, es_port=setting.ES_PORT,
                                   es_username=setting.ES_USERNAME,
                                   es_password=setting.ES_PASSWORD, es_index=setting.ES_INDEX)
    latest_id = None
    counter = 1
    ignore_ids = []
    processes = []
    while True:
        # if latest_id is not None:
        #     elastic_data = elastic_client.receive_data(query={
        #         "query": {
        #             "match": {
        #                 "id": latest_id
        #             }
        #         },
        #         "_source": ["id", "published_at"]
        #     })
        #     gte = elastic_data[0]["_source"]["published_at"]
        # else:
        #     gte = "2022-05-01T00:00:00"
        print('ignore_ids', len(ignore_ids))
        elastic_data = elastic_client.receive_data(query={
            "size": setting.ELASTIC_READ_SIZE,
            "sort": {
                "published_at": 'asc'
            },
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "exists": {
                                "field": "lf_emotion"
                            }
                        },
                        {
                            "terms": {
                                "id": ignore_ids
                            }
                        }
                    ],
                    "filter": {
                        "range": {
                            "published_at": {
                                "gt": "2022-05-21T00:00:00",
                                "lt": "2022-06-22T00:00:00"
                            }
                        }
                    }
                }
            },
            "_source": ["id", "created_at", "full_text"]
        })
        tran_data = []
        for item in elastic_data:
            ignore_ids.append(item["_source"]['id'])
            tran_data.append(item["_source"])
        if len(ignore_ids) > 210000:
            ignore_ids = ignore_ids[-210000:]
        p = Process(target=middle, args=(tran_data, counter,))
        p.start()
        processes.append(p)

        while len(processes) > 9:
            for idx, process in enumerate(processes):
                if not process.is_alive():
                    del (processes[idx])
            time.sleep(4)
        counter += 1


def middle(tran_data, counter):
    kafka_client = Kafka(BOOTSTRAP=setting.KAFKA_BOOTSTRAP, TOPIC=setting.KAFKA_TOPIC,
                         USERNAME=setting.KAFKA_USERNAME, PASSWORD=setting.KAFKA_PASSWORD)
    emotion_client = Emotions(setting.EMOTION_URL, setting.EMOTION_TOKEN)
    batch_size = int(setting.BATCH_SIZE)
    batch_conter = 1
    while len(tran_data):
        batch = tran_data[:batch_size]
        del tran_data[:batch_size]

        emotions = emotion_client.get_emotions([x.pop("full_text") for x in batch])
        print('counter', counter, 'batch_counter', batch_conter)
        batch_conter += 1
        for idx, doc in enumerate(batch):
            batch[idx]["lf_emotion"] = emotions[idx]
            kafka_client.producer.produce(kafka_client.kafka_topic,
                                          value=json.dumps(batch[idx], ensure_ascii=False).encode('utf-8'))
        kafka_client.producer.flush()
    print('done', counter)


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
