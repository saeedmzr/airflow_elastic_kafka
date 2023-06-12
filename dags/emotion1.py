import datetime
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

import setting
from components.elastic import ElasticSearch
from components.kafka import Kafka
from tasks.emotions import Emotions

default_args = {
    "owner": "saeed mouzarmi",
    "depends_on_past": False,
    'start_date': pendulum.now(),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(dag_id="add_emotion_tag_with_expand", default_args=default_args, schedule_interval='*/5 * * * *') as dag:
    @task()
    def reterieve_data_from_elastic():
        elastic_client = ElasticSearch(es_host=setting.ES_HOST, es_port=setting.ES_PORT,
                                       es_username=setting.ES_USERNAME,
                                       es_password=setting.ES_PASSWORD, es_index=setting.ES_INDEX)
        try:
            latest_id = Variable.get("latest_id")
        except KeyError:
            latest_id = None

        if latest_id != None:
            elastic_data = elastic_client.receive_data(query={
                "query": {
                    "match": {
                        "id": latest_id
                    }
                },
                "_source": ["id", "@timestamp"]
            })
            timedate = elastic_data[0]["_source"]["@timestamp"]
        else:
            now = datetime.datetime.utcnow()

            timedate = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

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
                            "@timestamp": {
                                "lt": timedate
                            }
                        }
                    }
                }
            },
            "_source": ["id", "published_at", "full_text"]
        })

        Variable.set("latest_id", elastic_data[-1]["_source"]["id"])

        return elastic_data


    @task(max_active_tis_per_dag=20)
    def trasnfer_batch(batch):
        emotion_client = Emotions(setting.EMOTION_URL, setting.EMOTION_TOKEN)
        emotions = emotion_client.get_emotions([x.pop("full_text") for x in batch])
        for idx, doc in enumerate(batch):
            batch[idx]["lf_emotion"] = emotions[idx]
        return batch


    @task()
    def transfer_data(raw_data):
        pool_array = []
        tran_data = [item["_source"] for item in raw_data]
        batch_size = int(setting.BATCH_SIZE)
        while len(tran_data):
            init_data = tran_data[:batch_size]
            pool_array.append(init_data)
            del tran_data[:batch_size]

        return pool_array


    @task(max_active_tis_per_dag=20)
    def publish_to_kafka(publish_data):
        kafka_client = Kafka(BOOTSTRAP=setting.KAFKA_BOOTSTRAP, TOPIC=setting.KAFKA_TOPIC,
                             USERNAME=setting.KAFKA_USERNAME, PASSWORD=setting.KAFKA_PASSWORD)
        kafka_client.insert_data(publish_data)


    data = reterieve_data_from_elastic()
    transfered_data = transfer_data(data)
    expanded_tasks = trasnfer_batch.expand(batch=transfered_data)
    published_kafka = publish_to_kafka.expand(publish_data=expanded_tasks)
