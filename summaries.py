from pymongo import MongoClient
from bson.son import SON
import datetime, json, copy, re

client = MongoClient('mongodb://database/argo')
db = client.argo

# data_keys enumerations
data_keys = list(db['bgcargoplus'].distinct('data_info.0'))
data_keys.sort()
try:
    db.summaries.replace_one({"_id": 'bgcargoplus_data_keys'}, {"_id":'bgcargoplus_data_keys', "data_keys":data_keys}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(data_keys)


# rate limiter metadata

ratelimiter = db.summaries.find_one({"_id": 'ratelimiter'})

earliest_doc = db.bgcargoplus.find_one({'timestamp_argoqc': 1}, sort=[("timestamp", 1)])
latest_doc = db.bgcargoplus.find_one({'timestamp_argoqc': 1}, sort=[("timestamp", -1)])
startDate = earliest_doc["timestamp"].isoformat() + "Z"
endDate = latest_doc["timestamp"].isoformat() + "Z"
baplimit = {'metagroups': ['id', 'metadata', 'platform'], 'startDate': startDate, 'endDate': endDate, 'qc': 'timestamp_argoqc'}
ratelimiter['metadata']['bgcargoplus'] = baplimit

try:
    db.summaries.replace_one({"_id": 'ratelimiter'}, ratelimiter, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(datasets)
