import os
import pandas as pd
from bs4 import BeautifulSoup

#import redis
#import pymongo as py
#from py2neo import Node, Relationship, Graph, NodeMatcher

BASE_DIR = '/opt/airflow/'
DATA_DIR = os.path.join(BASE_DIR, 'data')

## host is the name of the service runnning in the docker file, in this case just redis
#redis_client = redis.StrictRedis(host='redis', port=6379, decode_responses=True)

## connection with mongo
# myclient = py.MongoClient("mongodb://mongo:27017/")
# mongo_db = myclient['hiking_db']
# mongo_collection_hikes = mongo_db['hikes']

##connection with neo4j
# graph = Graph("bolt://neo:7687")
# neo4j_session = graph.begin()