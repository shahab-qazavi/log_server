from pymongo import MongoClient
import requests

import socket
from publics import PrintException, db
from bson.json_util import dumps, loads


try:
    socket.create_connection(('localhost', 27017))
    requests.get("http://localhost:8585/v1/")
    try:
        # con= MongoClient()
        # db = con['ldb']
        col_logs = db()['logs']
        logs_list = []
        ids_list = []

        for item in col_logs.find({'sent':{'$exists':False}}).limit(20):
            ids_list.append(item['_id'])
            del item['_id']
            item['date'] = str(item['date'])
            logs_list.append(item)

        # ADMIN_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiNWRiZmQxMjAxNzQ5ZWYxYjNiMGEwMjFkIiwicm9sZSI6ImFkbWluIiwiZGF0ZSI6IjIwMTktMTEtMDQgMTE6MTA6MDguMjg4MDk1In0.AtVoRKDTv1jvcHGpRmuLJvD9gG8sWqdR6P-Ibc6WDsU'
        ADMIN_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiNWRjZDU4MDZiZWU4YTZhODFjMzE0MDhkIiwicm9sZSI6ImFkbWluIiwiZGF0ZSI6IjIwMTktMTEtMTQgMTc6MDg6NDMuNDAzNDA4In0.QUCCJHXYcSMA-pyHFCQn8zBiHS7C48MpoFAS1T9JUWk'
        logs = dumps(logs_list)
        params = {'params':logs,'token':ADMIN_TOKEN}
        par = loads(params['params'])
        print(par[0])
        requests.post("http://localhost:8585/v1/collect",json=params)

        col_logs.update_many({'_id':{'$in':ids_list}},{'$set':{'sent':True}})
    except:
        PrintException()
except:
    PrintException()

