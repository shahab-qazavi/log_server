from datetime import datetime

from pymongo import MongoClient
from publics import db

# con = MongoClient()
# db = con['ldb']
col_services = db()['services']
col_logs = db()['logs']
col_users = db()['users']
col_search_history = db()['search_history']
col_events = db()['events']
col_chat = db()['chat']
item = ""
the_time = ""
condition = {'avg_date': {'$exists': False}}, {'_id': 1, 'create_date': 1}
# modify = {'_id': item['_id']}, {'$set': {'avg_date': the_time}}


# change date in search_history collection
for item in col_search_history.find({'avg_date': {'$exists': False}}, {'_id': 1, 'create_date': 1}):
    the_time = str(item['create_date'])
    the_time = the_time[:10]
    the_time = datetime.strptime(the_time, "%Y-%m-%d")
    col_search_history.find_and_modify({'_id': item['_id']}, {'$set': {'avg_date': the_time}})


# change date in events collection
for item in col_events.find({'avg_date': {'$exists': False}}, {'_id': 1, 'create_date': 1}):
    the_time = str(item['create_date'])
    the_time = the_time[:10]
    the_time = datetime.strptime(the_time, "%Y-%m-%d")
    col_events.find_and_modify({'_id': item['_id']}, {'$set': {'avg_date': the_time}})


# change date in chat collection
for item in col_chat.find({'avg_date': {'$exists': False}}, {'_id': 1, 'create_date': 1}):
    the_time = str(item['create_date'])
    the_time = the_time[:10]
    the_time = datetime.strptime(the_time, "%Y-%m-%d")
    col_chat.find_and_modify({'_id': item['_id']}, {'$set': {'avg_date': the_time}})

