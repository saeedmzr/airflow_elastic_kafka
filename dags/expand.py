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
import pandas as pd
from emotion import Emotion
from topic.topic1 import InstagramTopicDetection as topic1
from topic.topic2.instagram import instagram_topic as topic2
from components.elastic import ElasticSearch
from components.instances.kafka_instances import Test

import setting

default_args = {
    "owner": "Amirreza Akbari",
    "depends_on_past": False,
    'start_date': pendulum.today('UTC').add(days=-3),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(dag_id="add_emotion_ner_sentiment_tag", default_args=default_args, schedule_interval='*/1 * * * *',
         catchup=False, max_active_tasks=20, max_active_runs=1) as dag:
    @task()
    def reterieve_data_from_elastic():
        elastic_client = ElasticSearch(es_host=setting.ES_HOST, es_port=setting.ES_PORT,
                                       es_username=setting.ES_USERNAME,
                                       es_password=setting.ES_PASSWORD, es_index=setting.ES_INDEX)
        exclude_ids = Variable.get("exclude_ids", default_var=[], deserialize_json=True)
        print(type(exclude_ids))
        elastic_data = elastic_client.receive_data(query={
            "size": setting.ELASTIC_READ_SIZE,
            "sort": {
                setting.ORDERABLE_PARAMETERS: 'desc'
            },
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "terms": {
                                "_id": exclude_ids
                            }
                        },
                        {
                            "exists": {
                                "field": "ner"
                            }
                        },
                        {
                            "exists": {
                                "field": "lf_emotion"
                            }
                        },
                        {
                            "exists": {
                                "field": "lf_sentiment"
                            }
                        }
                    ],
                }
            }
        })
        Variable.set("exclude_ids", json.dumps([item['_id'] for item in elastic_data]))

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
                response = requests.request("POST", url, headers=headers, data=payload, timeout=60 * 3)
            except (Timeout, ConnectionError, ChunkedEncodingError) as e:
                print(e)
                time.sleep(10)
                continue
            if response.status_code != 200:
                print('error not 200', response.status_code)
                time.sleep(10)
                continue
            return json.loads(response.text)


    @task(max_active_tis_per_dag=6)
    def perform_subject_v1(batch_tuple):
        texts = batch_tuple[0]
        ner_results = batch_tuple[1]
        topic = topic1()
        results = []
        for idx, text in enumerate(texts):
            try:
                results.append(topic.infer(text, ner_results[idx]))
            except Exception as e:
                print('topic1 Error ========================>> ', e)
                results.append(None)
        return results


    @task(max_active_tis_per_dag=6)
    def perform_subject_v2(batch):
        results = []
        for idx, text in enumerate(batch):
            try:
                results.append(topic2(text))
            except Exception as e:
                print('topic2 Error ========================>> ', e)
                results.append(None)
        return results


    @task(max_active_tis_per_dag=3)
    def perform_emotion_v1(texts):
        emotion = Emotion('instagram')
        df = pd.DataFrame({'id': list(range(1, len(texts) + 1)), 'text': texts})
        df = emotion.infer(df)
        df.columns = ['id', 'text', 'lf_wonder', 'lf_credence', 'lf_sadness', 'lf_happiness', 'lf_expectation',
                      'lf_anger', 'lf_hate',
                      'lf_hope', 'lf_fear']

        selected_columns = ['lf_wonder', 'lf_credence', 'lf_sadness', 'lf_happiness', 'lf_expectation',
                            'lf_anger', 'lf_hate', 'lf_hope', 'lf_fear']
        result = []
        for index, row in df.iterrows():
            selected_data = {col: row[col] for col in selected_columns}
            result.append(selected_data)
        print(result[0])
        return result


    @task(max_active_tis_per_dag=3)
    def perform_sentiment(batch):
        texts_list = []
        for batch_item in batch:
            if batch_item.get('caption'):
                texts_list.append(batch_item['caption'].get('text'))
            else:
                texts_list.append(None)
        url = 'http://192.168.10.62/predictions/sentiment'
        payload = json.dumps(texts_list)
        headers = {
            'Authorization': setting.EMOTION_TOKEN,
            'Content-Type': 'application/json'
        }
        while True:
            try:
                response = requests.request("POST", url, headers=headers, data=payload, timeout=60 * 3)
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
    def print_output(raw_data, ner_result, sentimant_result, emotion_v1_result, subject_v1_result, subject_v2_result):
        kafka_client = Test()
        flattened_ner_result = [item for sub_list in ner_result for item in sub_list]
        flattened_sentiment_result = [item for sub_list in sentimant_result for item in sub_list]
        flattened_emotion_v1_result = [item for sub_list in emotion_v1_result for item in sub_list]
        flattened_subject_v1_result = [item for sub_list in subject_v1_result for item in sub_list]
        flattened_subject_v2_result = [item for sub_list in subject_v2_result for item in sub_list]
        final_data = []
        for idx, item in enumerate(raw_data):
            tmp_data = {
                **item,
                **flattened_emotion_v1_result[idx],
                'subject': flattened_subject_v1_result[idx],
                'lf_subject': flattened_subject_v2_result[idx],
                'lf_sentiment': flattened_sentiment_result[idx],
                'ner': flattened_ner_result[idx],
                "lf_modules": {
                    "sentiment": {
                        "version": "1.0.0"
                    },
                    "emotion": {
                        "version": "1.0.0"
                    },
                    "ner": {
                        "version": "1.0.0"
                    }
                }
            }
            kafka_client.insert_data(tmp_data)
            final_data.append(tmp_data)
        kafka_client.producer.flush()
        return final_data


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


    raw_data = reterieve_data_from_elastic()
    prepared_data = prepare_data(raw_data)
    ner_tags = perform_ner.expand(batch=prepared_data)
    emotion_v1_tags = perform_emotion_v1.expand(texts=prepared_data)
    sentiment_tags = perform_sentiment.expand(batch=prepared_data)
    final_data = print_output(
        raw_data=raw_data,
        ner_result=ner_tags,
        sentimant_result=sentiment_tags,
        emotion_v1_result=emotion_v1_tags,
        subject_v1_result=subject_v1_tags,
        subject_v2_result=subject_v2_tags,
    )

with DAG(dag_id="add_subject_tag", default_args=default_args, schedule_interval='*/1 * * * *',
         catchup=False, max_active_tasks=20, max_active_runs=1) as dag:
    @task()
    def reterieve_data_from_elastic():
        elastic_client = ElasticSearch(es_host=setting.ES_HOST, es_port=setting.ES_PORT,
                                       es_username=setting.ES_USERNAME,
                                       es_password=setting.ES_PASSWORD, es_index=setting.ES_INDEX)
        exclude_ids = Variable.get("exclude_ids", default_var=[], deserialize_json=True)
        print(type(exclude_ids))
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
                                "field": "lf_subject"
                            }
                        },
                        {
                            "exists": {
                                "field": "subject"
                            }
                        }
                    ],
                    "must": [
                        {
                            "exists": {
                                "field": "ner"
                            }
                        },
                    ]
                }
            }
        })
        Variable.set("exclude_ids", json.dumps([item['_id'] for item in elastic_data]))

        return elastic_data


    @task(max_active_tis_per_dag=6)
    def perform_subject_v1(batch_tuple):
        texts = batch_tuple[0]
        ner_results = batch_tuple[1]
        topic = topic1()
        results = []
        for idx, text in enumerate(texts):
            try:
                results.append(topic.infer(text, ner_results[idx]))
            except Exception as e:
                print('topic1 Error ========================>> ', e)
                results.append(None)
        return results


    @task(max_active_tis_per_dag=6)
    def perform_subject_v2(batch):
        results = []
        for idx, text in enumerate(batch):
            try:
                results.append(topic2(text))
            except Exception as e:
                print('topic2 Error ========================>> ', e)
                results.append(None)
        return results


    @task()
    def print_output(raw_data, subject_v1_result, subject_v2_result):
        kafka_client = Test()
        flattened_subject_v1_result = [item for sub_list in subject_v1_result for item in sub_list]
        flattened_subject_v2_result = [item for sub_list in subject_v2_result for item in sub_list]
        final_data = []
        for idx, item in enumerate(raw_data):
            tmp_data = {
                **item,
                **flattened_emotion_v1_result[idx],
                'subject': flattened_subject_v1_result[idx],
                'lf_subject': flattened_subject_v2_result[idx],
                'ner': flattened_ner_result[idx],
                "lf_modules": {
                    "subject": {
                        "version": "1.0.0"
                    }
                }
            }
            kafka_client.insert_data(tmp_data)
            final_data.append(tmp_data)
        kafka_client.producer.flush()
        return final_data


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


    @task()
    def prepare_data_for_subject_v1(ner_results, raw_texts):
        result = list(zip(raw_texts, ner_results))
        return result


    raw_data = reterieve_data_from_elastic()
    prepared_data = prepare_data(raw_data)
    subject_v2_tags = perform_subject_v2.expand(batch=prepared_data)
    subject_v1_tags = perform_subject_v1.expand(batch_tuple=prepared_data_for_subject_v1)
    final_data = print_output(
        raw_data=raw_data,
        subject_v1_result=subject_v1_tags,
        subject_v2_result=subject_v2_tags,
    )
