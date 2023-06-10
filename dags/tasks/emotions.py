import time
import requests
from requests import Timeout
from requests.exceptions import ChunkedEncodingError, ConnectionError
import json
import os
from dotenv import dotenv_values



class Emotions:
    def __init__(self,url , Token):
        self.url = url
        self.token = Token

    def get_emotions(self, texts):
        payload = json.dumps(texts)
        headers = {
            'Authorization': self.token,
            'Content-Type': 'application/json'
        }
        while True:
            try:
                response = requests.request("POST", self.url, headers=headers, data=payload)
            except (Timeout, ConnectionError, ChunkedEncodingError) as e:
                time.sleep(10)
                continue
            return json.loads(response.text)
