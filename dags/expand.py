import datetime
import json
import random
import time
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from requests import Timeout
from requests.exceptions import ChunkedEncodingError

import setting
from components.elastic import ElasticSearch
from components.kafka import Kafka
from tasks.emotion_v1 import EmotionV1
from tasks.emotions import Emotions

default_args = {
    "owner": "Instagram",
    "depends_on_past": False,
    'start_date': pendulum.today('UTC').add(days=-3),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(dag_id="add_emotion_tag_with_expand", default_args=default_args, schedule_interval='*/1 * * * *',
         catchup=False) as dag:
    @task()
    def reterieve_data_from_elastic():
        elastic_client = ElasticSearch(es_host=setting.ES_HOST, es_port=setting.ES_PORT,
                                       es_username=setting.ES_USERNAME,
                                       es_password=setting.ES_PASSWORD, es_index=setting.ES_INDEX)
        try:
            latest_id = Variable.get("latest_id")
        except KeyError:
            latest_id = None

        if latest_id is not None:
            elastic_data = elastic_client.receive_data(query={
                "query": {
                    "match": {
                        "id": latest_id
                    }
                },
                "_source": ["id", setting.ORDERABLE_PARAMETERS]
            })
            timedate = elastic_data[0]["_source"][setting.ORDERABLE_PARAMETERS]
        else:
            now = datetime.datetime.utcnow()

            timedate = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        elastic_data = elastic_client.receive_data(query={
            "size": setting.ELASTIC_READ_SIZE,
            "sort": {
                setting.ORDERABLE_PARAMETERS: 'desc'
            },
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "exists": {
                                "field": setting.NOT_EXISTS_FIELD
                            }
                        }
                    ],
                    "filter": {
                        "range": {
                            setting.ORDERABLE_PARAMETERS: {
                                "lt": timedate
                            }
                        }
                    }
                }
            },
            "_source": ["pk", setting.ORDERABLE_PARAMETERS, setting.FULL_TEXT_FIELD]
        })

        Variable.set("latest_id", elastic_data[-1]["_source"]["pk"])

        return elastic_data


    @task(max_active_tis_per_dag=20)
    def perform_emotion_v1(batch):
        ai_module_client = EmotionV1('instagram')
        emotions = ai_module_client.get_emotion_v1([x.pop(setting.FULL_TEXT_FIELD) for x in batch])
        for idx, doc in enumerate(batch):
            batch[idx][setting.NOT_EXISTS_FIELD] = emotions[idx]
        print(batch)
        # kafka_client.insert_data(batch)
        return batch


    @task(max_active_tis_per_dag=20)
    def perform_ner(batch):
        urls = [
            'http://192.168.10.188:8080/predictions/ner',
            'http://192.168.10.196:8080/predictions/ner',
            'http://192.168.10.199:8080/predictions/ner'
        ]
        url = urls[random.randint(0, 2)]
        payload = json.dumps(batch)
        headers = {
            'Authorization': setting.EMOTION_TOKEN,
            'Content-Type': 'application/json'
        }
        while True:
            try:
                response = requests.request("POST", url, headers=headers, data=payload)
            except (Timeout, ConnectionError, ChunkedEncodingError) as e:
                time.sleep(10)
                continue
            if response.status_code != 200:
                time.sleep(10)
                continue
            return json.loads(response.text)


    @task()
    def print_output(batch):
        print(batch)
        return batch


    @task()
    def prepare_data(raw_data):
        pool_array = []
        tran_data = [item["_source"] for item in raw_data]
        batch_size = int(setting.BATCH_SIZE)
        while len(tran_data):
            init_data = tran_data[:batch_size]
            pool_array.append(init_data)
            del tran_data[:batch_size]

        return pool_array


    data = reterieve_data_from_elastic()
    prepared_data = prepare_data(data)
    ner_tagged_data = perform_ner.expand(batch=prepared_data)
    final_data = print_output.expand(batch=ner_tagged_data)

    data >> prepared_data >> ner_tagged_data
