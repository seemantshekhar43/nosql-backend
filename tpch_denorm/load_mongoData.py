from pymongo import MongoClient
client = MongoClient(port=27017)

db = client['test']
collection = db['orders']

with open('denorm.json') as file:
    file_data = json.load(file)
if isinstance(file_data, list):
    collection.insert_many(file_data)
else:
    collection.insert_one(file_data)
