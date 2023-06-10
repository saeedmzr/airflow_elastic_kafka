from elasticsearch import Elasticsearch


class ElasticSearch:
    def __init__(self, es_host, es_port, es_index, es_username, es_password):
        self.es = Elasticsearch([f'http://{es_host}:{es_port}'], http_auth=(es_username, es_password))
        self.es_index = es_index

    def receive_data(self, query):
        res = self.es.search(index=self.es_index, body=query)
        return res['hits']['hits']
