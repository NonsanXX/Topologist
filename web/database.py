import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB_NAME = os.getenv("DB_NAME", "topologist")
SCHEDULER_URL = os.getenv("SCHEDULER_URL", "http://scheduler:5001")

db = MongoClient(MONGO_URI)[DB_NAME]
