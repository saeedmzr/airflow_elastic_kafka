import os
from dotenv import load_dotenv
from config.config import Config

# Load environment variables from .env file
load_dotenv()

config = Config()

# Access the environment variables
ES_HOST = os.getenv('ES_HOST')
ES_PORT = os.getenv('ES_PORT')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')
ES_INDEX = os.getenv('ES_INDEX')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')

BATCH_SIZE = os.getenv('BATCH_SIZE')
ELASTIC_READ_SIZE = os.getenv('ELASTIC_READ_SIZE')

EMOTION_URL = os.getenv('EMOTION_URL')
EMOTION_TOKEN = os.getenv('EMOTION_TOKEN')
FULL_TEXT_FIELD = os.getenv('FULL_TEXT_FIELD')
NOT_EXISTS_FIELD = os.getenv('NOT_EXISTS_FIELD')
ORDERABLE_PARAMETERS = os.getenv('ORDERABLE_PARAMETERS')

