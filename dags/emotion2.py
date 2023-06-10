from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import List, Any

import pendulum
from airflow.decorators import task, dag
from components.elastic import ElasticSearch
from components.kafka import Kafka
from tasks.emotions import Emotions
import setting


default_args = {
    "owner": "saeed mouzarmi",
    "depends_on_past": False,
    'start_date': pendulum.today('UTC').add(days=-3),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
now = pendulum.now()


@dag(dag_id='add_emotion_tag_with_multi_treading', default_args=default_args, catchup=False)
def processor():

    def trasnfer_batch(batch):
        emotion_client = Emotions(setting.EMOTION_URL,setting.EMOTION_TOKEN)
        emotions = emotion_client.get_emotions([x.pop("full_text") for x in batch])
        for idx, doc in enumerate(batch):
            batch[idx]["lf_emotion"] = emotions[idx]
        return batch

    @task()
    def reterieve_data_from_elastic():
        elastic_client = ElasticSearch(es_host=setting.ES_HOST, es_port=setting.ES_PORT, es_username=setting.ES_USERNAME,
                                       es_password=setting.ES_PASSWORD, es_index=setting.ES_INDEX)
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
                    ]
                }
            },
            "_source": ["id", "published_at", "full_text"]
        })
        return elastic_data

    @task
    def transfer_data(raw_data):
        pool = ThreadPoolExecutor(max_workers=500)

        pool_array = []
        tran_data = [item["_source"] for item in raw_data]
        batch_size = int(setting.BATCH_SIZE)
        while len(tran_data):
            init_data = tran_data[:batch_size]
            pool_array.append(init_data)
            del tran_data[:batch_size]

        expanded_tasks = pool.map(trasnfer_batch, pool_array)
        pool.shutdown()
        response_list = []  # List to store response dictionaries

        for response in expanded_tasks:
            for item in response:
                response_list.append(item)

        return response_list

    @task()
    def publish_to_kafka(publish_data):
        kafka_client = Kafka(BOOTSTRAP=setting.KAFKA_BOOTSTRAP, TOPIC=setting.KAFKA_TOPIC,USERNAME=setting.KAFKA_USERNAME, PASSWORD=setting.KAFKA_PASSWORD)
        pool = ThreadPoolExecutor(max_workers=10)
        pool.map(kafka_client.insert_data, publish_data)
        pool.shutdown()

    data = reterieve_data_from_elastic()
    transfered_data = transfer_data(data)
    published_kafka = publish_to_kafka(transfered_data)

    data >> transfered_data >> published_kafka


dag = processor()
