import os
import pymongo

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB_NAME = os.getenv("DB_NAME", "topologist")

db = pymongo.MongoClient(MONGO_URI)[DB_NAME]
