import json
import logging

from azure.eventhub import EventHubProducerClient
from flask import Flask
from flask_restful import Api
from redis import StrictRedis

with open('./config.json', 'r') as f:
    config = json.load(f)

try:
    redis_args = config["redis"]
    event_hub_args = config["kafka"]
except KeyError as error:
    logging.ERROR("Config error")
    raise

# Main flask app
app = Flask(__name__)

# flask-restful api
api = Api(app)

# redis client
my_redis = StrictRedis(**redis_args)

# Azure Event hub client
event_hub_client = EventHubProducerClient.from_connection_string(**event_hub_args)
