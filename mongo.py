import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

client = MongoClient(os.environ["MONGO_URI"])
db = client[os.environ["MONGO_DATABASE"]]
collection = db[os.environ["MONGO_COLLECTION"]]

def insert_product_view(document):
    result = collection.insert_one(document)
    document["_id"] = result.inserted_id
    return result.inserted_id

def close_mongo():
    client.close()
