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
from tasks.emotions import Emotions

default_args = {
    "owner": "Instagram",
    "depends_on_past": False,
    'start_date': pendulum.today('UTC').add(days=-3),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(dag_id="add_emotion_tag_with_expand", default_args=default_args, schedule_interval='*/1 * * * *',
         catchup=False, max_active_tasks=20, max_active_runs=1) as dag:
    @task()
    def reterieve_data_from_elastic():
        elastic_client = ElasticSearch(es_host=setting.ES_HOST, es_port=setting.ES_PORT,
                                       es_username=setting.ES_USERNAME,
                                       es_password=setting.ES_PASSWORD, es_index=setting.ES_INDEX)
        last_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        try:
            last_timestamp = Variable.get("last_timestamp")
        except KeyError:
            pass

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
                                "lt": last_timestamp
                            }
                        }
                    }
                }
            }
        })
        Variable.set("last_timestamp", elastic_data[0]["_source"][setting.ORDERABLE_PARAMETERS])

        return elastic_data


    @task(max_active_tis_per_dag=3)
    def perform_ner(batch):
        texts_list = []
        for batch_item in batch:
            if batch_item.get('caption'):
                texts_list.append(batch_item['caption'].get('text'))
            else:
                texts_list.append(None)
        url = 'http://192.168.10.62/predictions/ner'
        payload = json.dumps(texts_list)
        headers = {
            'Authorization': setting.EMOTION_TOKEN,
            'Content-Type': 'application/json'
        }
        while True:
            try:
                response = requests.request("POST", url, headers=headers, data=payload, timeout=60*3)
            except (Timeout, ConnectionError, ChunkedEncodingError) as e:
                print(e)
                time.sleep(10)
                continue
            if response.status_code != 200:
                print('error not 200', response.status_code)
                time.sleep(10)
                continue
            return json.loads(response.text)


    @task()
    def print_output(batch):
        print(len(batch))
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
